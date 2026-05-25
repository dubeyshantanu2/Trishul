import os
from dotenv import load_dotenv
load_dotenv()

import json
import time
import asyncio
import logging
import struct
import numpy as np
from collections import deque
from datetime import datetime, timezone

from parser import NiftyFuturePipeline
from engine import DOMEngine
from adapters import log_signal, dispatch_webhook

logger = logging.getLogger("TrishulOrchestrator")
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

class ChopProtectionFilter:
    """
    Multi-variable state Chop Protection filter.
    Tracks OBI polarity changes to prevent alert spam during ranging markets.
    """
    def __init__(self, window_seconds: int = 60, max_flips: int = 5):
        self.state = "REGIME_TREND"
        self.flip_times = deque()
        self.last_polarity = 0
        self.unlatch_start_time = None
        self.unlatch_start_cvd = 0.0
        self.window_seconds = window_seconds
        self.max_flips = max_flips

    def update(self, obi: float, cvd: float) -> str:
        current_time = time.time()
        
        while self.flip_times and current_time - self.flip_times[0] > self.window_seconds:
            self.flip_times.popleft()

        current_polarity = 1 if obi > 0 else (-1 if obi < 0 else 0)
        
        if current_polarity != 0 and self.last_polarity != 0 and current_polarity != self.last_polarity:
            self.flip_times.append(current_time)
            
        if current_polarity != 0:
            self.last_polarity = current_polarity

        if self.state == "REGIME_TREND" and len(self.flip_times) > self.max_flips:
            self.state = "REGIME_CHOP"
            logger.warning(f"Chop Protection ENGAGED: >{self.max_flips} OBI polarity flips in {self.window_seconds}s window.")
            self.unlatch_start_time = None

        if self.state == "REGIME_CHOP":
            if abs(obi) > 0.50:
                if self.unlatch_start_time is None:
                    self.unlatch_start_time = current_time
                    self.unlatch_start_cvd = cvd
                elif current_time - self.unlatch_start_time >= 15.0:
                    cvd_momentum = cvd - self.unlatch_start_cvd
                    is_positive_momentum = (obi > 0 and cvd_momentum > 0) or (obi < 0 and cvd_momentum < 0)
                    
                    if is_positive_momentum:
                        self.state = "REGIME_TREND"
                        logger.info("Chop Protection DISENGAGED: Sustained OBI > 0.50 for 15s with positive CVD momentum.")
                        self.flip_times.clear()
                        self.unlatch_start_time = None
                    else:
                        self.unlatch_start_time = current_time
                        self.unlatch_start_cvd = cvd
            else:
                self.unlatch_start_time = None

        return self.state

def calculate_atm_strike(future_price: float, interval: int = 50, direction: str = "LONG", symbol: str = "") -> str:
    """
    Rounds future price to the nearest strike interval.
    Returns the formatted option strike string (CE for long, PE for short).
    """
    if future_price == 0.0:
        return "N/A"
        
    rounded_strike = round(future_price / interval) * interval
    opt_type = "CE" if direction == "LONG" else "PE"
    
    if symbol:
        return f"{symbol} {int(rounded_strike)} {opt_type}"
    return f"{int(rounded_strike)} {opt_type}"

class OrchestratedPipeline(NiftyFuturePipeline):
    """
    Unified runtime execution orchestrator.
    Pipes streaming ticks into the DOMEngine and manages alerting logic.
    """
    def __init__(self, config: dict):
        # Support either NIFTY or generic FUTURES profile
        profile = config.get("FUTURES_PROFILE") or config.get("NIFTY_FUTURES_PROFILE", {})
        sys_settings = config.get("SYSTEM_SETTINGS", {})
        
        # Override with environment variables for dynamic deployment (Fly.io secrets)
        security_id = int(os.environ.get("TARGET_SECURITY_ID", profile.get("security_id", 13)))
        
        super().__init__(
            security_id=security_id,
            token=os.environ.get("TRISHUL_AUTH_TOKEN", "mock"),
            client_id=os.environ.get("TRISHUL_CLIENT_ID", "mock")
        )
        self.config = config
        self.profile = profile
        self.sys_settings = sys_settings
        
        # Merge ENV variables into profile overrides
        self.symbol_name = os.environ.get("TARGET_SYMBOL_NAME", self.profile.get("symbol_name", ""))
        self.profile["security_id"] = security_id
        self.profile["tick_size"] = float(os.environ.get("TARGET_TICK_SIZE", self.profile.get("tick_size", 0.05)))
        self.profile["strike_interval"] = int(os.environ.get("TARGET_STRIKE_INTERVAL", self.profile.get("strike_interval", 50)))
        
        # Calculate Sweep QTY dynamically based on lot size and desired trade lots
        lot_size = int(os.environ.get("TARGET_LOT_SIZE", self.profile.get("lot_size", 50)))
        trade_lots = int(os.environ.get("TARGET_TRADE_LOTS", self.profile.get("trade_lots", 50)))
        self.profile["target_sweep_qty"] = lot_size * trade_lots
        
        self.engine = DOMEngine(
            tick_size=self.profile["tick_size"], 
            value_area_pct=0.70
        )
        self.chop_filter = ChopProtectionFilter(
            window_seconds=self.sys_settings.get("chop_tracking_window_seconds", 60),
            max_flips=self.profile.get("chop_max_sign_flips", 5)
        )
        
        self.bids_cache = np.empty((0, 3))
        self.asks_cache = np.empty((0, 3))
        self.last_future_price = 0.0
        self.last_signal_time = 0.0

    def _parse_depth(self, payload: bytes, side: str, num_rows: int):
        if num_rows == 0:
            return

        book_data = []
        for i in range(num_rows):
            offset = i * self.DEPTH_ROW_SIZE
            if offset + self.DEPTH_ROW_SIZE > len(payload):
                break
            price, qty, orders = struct.unpack(self.DEPTH_ROW_FMT, payload[offset:offset + self.DEPTH_ROW_SIZE])
            book_data.append([price, qty, orders])

        book_array = np.array(book_data)

        if side == "BID":
            self.bids_cache = book_array
            self.best_bid = book_array[0, 0] if book_array.size > 0 else 0.0
        else:
            self.asks_cache = book_array
            self.best_ask = book_array[0, 0] if book_array.size > 0 else 0.0
            
        self._evaluate_state()

    def _parse_tick(self, payload: bytes):
        if len(payload) >= 12:
            price, qty = struct.unpack("<d I", payload[:12])
            self.last_future_price = price
            
            self.engine.update_avp(price, qty)
            self.engine.update_cvd(price, qty, self.best_bid, self.best_ask)
            
            self._evaluate_state()

    def _evaluate_state(self):
        if self.bids_cache.size == 0 or self.asks_cache.size == 0 or self.last_future_price == 0.0:
            return
            
        spoof_filtered_obi = self.engine.calculate_spoof_filtered_obi(self.bids_cache, self.asks_cache)
        micro_price = self.engine.calculate_micro_price(self.bids_cache, self.asks_cache)
        cvd_val = self.engine.cvd
        
        regime = self.chop_filter.update(spoof_filtered_obi, cvd_val)
        
        trigger_threshold = self.profile.get("obi_trigger_threshold", 0.50)
        is_breakout = abs(spoof_filtered_obi) > trigger_threshold
        
        current_time = time.time()
        if is_breakout and (current_time - self.last_signal_time > 2.0):
            self.last_signal_time = current_time
            direction = "LONG" if spoof_filtered_obi > 0 else "SHORT"
            
            book_side = self.asks_cache if direction == "LONG" else self.bids_cache
            target_qty = self.profile.get("target_sweep_qty", 2500)
            sweep_vwap = self.engine.calculate_sweep_vwap(book_side, target_qty=target_qty)
            
            strike_interval = self.profile.get("strike_interval", 50)
            atm_strike_str = calculate_atm_strike(self.last_future_price, strike_interval, direction, symbol=self.symbol_name)
            
            # Simple spoof anomaly flag based on difference between micro and sweep
            spoof_anomaly = abs(sweep_vwap - micro_price) > (self.profile.get("tick_size", 0.05) * 80) 

            snapshot_data = {
                "timestamp_epoch": int(current_time),
                "instrument_ticker": str(self.profile.get("security_id", "13")),
                "signal_direction": direction,
                "spoof_filtered_obi": float(spoof_filtered_obi),
                "current_cvd": int(cvd_val),
                "avp_poc": float(self.engine.poc),
                "avp_vah": float(self.engine.vah),
                "avp_val": float(self.engine.val),
                "calculated_micro_price": float(micro_price),
                "calculated_sweep_vwap": float(sweep_vwap),
                "suggested_atm_strike": atm_strike_str,
                "current_regime": "CHOP" if regime == "REGIME_CHOP" else "TRENDING"
            }
            
            alert_payload = {
                "webhook_url": self.sys_settings.get("discord_webhook_url"),
                "direction": direction,
                "micro_price": float(micro_price),
                "sweep_vwap": float(sweep_vwap),
                "atm_strike": atm_strike_str,
                "spoof_anomaly": spoof_anomaly,
                "avp_vah": float(self.engine.vah),
                "avp_poc": float(self.engine.poc),
                "avp_val": float(self.engine.val),
                "cvd_velocity": f"{cvd_val:.1f} Δ"
            }

            if self.sys_settings.get("supabase_logging_enabled", True):
                asyncio.create_task(log_signal(snapshot_data))
            
            if regime != "REGIME_CHOP":
                alert_target = self.symbol_name if self.symbol_name else str(self.profile.get("security_id"))
                logger.info(f"Firing {direction} Breakout Alert for {alert_target}!")
                asyncio.create_task(dispatch_webhook(alert_payload))
            else:
                logger.info("Signal suppressed by CHOP PROTECTION regime. Database audit logged.")

async def main():
    config_path = "config.json"
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
    except FileNotFoundError:
        logger.error(f"Configuration file {config_path} not found. Ensure it exists in the root directory.")
        return
    except json.JSONDecodeError:
        logger.error(f"Configuration file {config_path} is malformed.")
        return

    orchestrator = OrchestratedPipeline(config)
    
    target_name = orchestrator.symbol_name if orchestrator.symbol_name else str(orchestrator.security_id)
    logger.info(f"Initializing Trishul Unified Runtime Execution Engine for {target_name}...")
    
    await orchestrator.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Orchestrator graceful shutdown triggered by operator.")
