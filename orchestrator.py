import os
import json
import time
import asyncio
import logging
import struct
import numpy as np
from collections import deque

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
    def __init__(self):
        self.state = "REGIME_TREND"
        self.flip_times = deque()
        self.last_polarity = 0
        self.unlatch_start_time = None
        self.unlatch_start_cvd = 0.0

    def update(self, obi: float, cvd: float) -> str:
        current_time = time.time()
        
        # 1. Clean up old flips outside the 60s rolling window
        while self.flip_times and current_time - self.flip_times[0] > 60.0:
            self.flip_times.popleft()

        # 2. Determine current polarity (+1 for positive, -1 for negative, 0 for neutral)
        current_polarity = 1 if obi > 0 else (-1 if obi < 0 else 0)
        
        # Track polarity flips (+ to -, - to +)
        if current_polarity != 0 and self.last_polarity != 0 and current_polarity != self.last_polarity:
            self.flip_times.append(current_time)
            
        if current_polarity != 0:
            self.last_polarity = current_polarity

        # 3. Chop latch logic: >5 polarity flips in 60s
        if self.state == "REGIME_TREND" and len(self.flip_times) > 5:
            self.state = "REGIME_CHOP"
            logger.warning("Chop Protection ENGAGED: >5 OBI polarity flips in 60s window.")
            self.unlatch_start_time = None

        # 4. Unlatch logic for Chop state
        if self.state == "REGIME_CHOP":
            if abs(obi) > 0.50:
                if self.unlatch_start_time is None:
                    # Start timing the 15-second stabilization
                    self.unlatch_start_time = current_time
                    self.unlatch_start_cvd = cvd
                elif current_time - self.unlatch_start_time >= 15.0:
                    # 15 seconds passed, check CVD momentum
                    cvd_momentum = cvd - self.unlatch_start_cvd
                    
                    # Positive CVD momentum means delta is confirming the OBI direction
                    is_positive_momentum = (obi > 0 and cvd_momentum > 0) or (obi < 0 and cvd_momentum < 0)
                    
                    if is_positive_momentum:
                        self.state = "REGIME_TREND"
                        logger.info("Chop Protection DISENGAGED: Sustained OBI > 0.50 for 15s with positive CVD momentum.")
                        self.flip_times.clear()
                        self.unlatch_start_time = None
                    else:
                        # Reset stabilization timer if momentum fails to confirm
                        self.unlatch_start_time = current_time
                        self.unlatch_start_cvd = cvd
            else:
                # OBI dropped below 0.50, reset timer
                self.unlatch_start_time = None

        return self.state

def calculate_atm_strike(future_price: float, interval: int = 50, direction: str = "LONG") -> str:
    """
    Rounds future price to the nearest strike interval.
    Returns the formatted option strike string (CE for long, PE for short).
    """
    if future_price == 0.0:
        return "N/A"
        
    rounded_strike = int(round(future_price / interval) * interval)
    opt_type = "CE" if direction == "LONG" else "PE"
    return f"NIFTY {rounded_strike} {opt_type}"

class OrchestratedPipeline(NiftyFuturePipeline):
    """
    Unified runtime execution orchestrator.
    Pipes streaming ticks into the DOMEngine and manages alerting logic.
    """
    def __init__(self, config: dict):
        super().__init__(
            security_id=config["SECURITY_ID"],
            token=os.environ.get("TRISHUL_AUTH_TOKEN", config.get("TOKEN")),
            client_id=os.environ.get("TRISHUL_CLIENT_ID", config.get("CLIENT_ID"))
        )
        self.config = config
        self.engine = DOMEngine(
            tick_size=config.get("TICK_SIZE", 0.05), 
            value_area_pct=config.get("VALUE_AREA_PCT", 0.70)
        )
        self.chop_filter = ChopProtectionFilter()
        
        # In-memory arrays for full order book math
        self.bids_cache = np.empty((0, 3))
        self.asks_cache = np.empty((0, 3))
        self.last_future_price = 0.0
        self.last_signal_time = 0.0

    def _parse_depth(self, payload: bytes, side: str, num_rows: int):
        """Overrides base depth parsing to assemble full numpy arrays for the engine."""
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
            
        # Trigger system evaluation on depth change
        self._evaluate_state()

    def _parse_tick(self, payload: bytes):
        """Overrides base tick parsing to route trade flow into AVP and CVD."""
        if len(payload) >= 12:
            price, qty = struct.unpack("<d I", payload[:12])
            self.last_future_price = price
            
            # Pipe to Engine
            self.engine.update_avp(price, qty)
            self.engine.update_cvd(price, qty, self.best_bid, self.best_ask)
            
            # Optional: Parent logging
            # super()._parse_tick(payload)
            self._evaluate_state()

    def _evaluate_state(self):
        """Core decision loop: evaluates math engine metrics and dispatches alerts."""
        if self.bids_cache.size == 0 or self.asks_cache.size == 0 or self.last_future_price == 0.0:
            return
            
        # 1. Math Engine Calculations
        spoof_filtered_obi = self.engine.calculate_spoof_filtered_obi(self.bids_cache, self.asks_cache)
        micro_price = self.engine.calculate_micro_price(self.bids_cache, self.asks_cache)
        cvd_val = self.engine.cvd
        
        # 2. Chop Filter State Machine
        regime = self.chop_filter.update(spoof_filtered_obi, cvd_val)
        
        # 3. Breakout Logic (Simulated Threshold Trigger)
        # Using 0.65 as an example threshold for a breakout signal
        is_breakout = abs(spoof_filtered_obi) > 0.65 
        
        # Throttle signals to max 1 per 2 seconds to avoid async race flooding
        current_time = time.time()
        if is_breakout and (current_time - self.last_signal_time > 2.0):
            self.last_signal_time = current_time
            direction = "LONG" if spoof_filtered_obi > 0 else "SHORT"
            
            # Additional execution math
            book_side = self.asks_cache if direction == "LONG" else self.bids_cache
            sweep_vwap = self.engine.calculate_sweep_vwap(book_side)
            atm_strike_str = calculate_atm_strike(self.last_future_price, self.config.get("STRIKE_INTERVAL", 50), direction)
            
            # Is there a massive anomaly between best price and sweep? (Spoof warning flag)
            spoof_anomaly = abs(sweep_vwap - micro_price) > self.config.get("SLIPPAGE_CAP", 2.0) * 2

            # Data Payloads
            snapshot_data = {
                "ticker": "NIFTY_FUT",
                "direction": direction,
                "spoof_filtered_obi": float(spoof_filtered_obi),
                "cvd_value": float(cvd_val),
                "avp_phase_status": regime,
                "micro_price": float(micro_price),
                "sweep_vwap": float(sweep_vwap)
            }
            
            alert_payload = {
                "direction": direction,
                "micro_price": float(micro_price),
                "sweep_vwap": float(sweep_vwap),
                "slippage_cap": self.config.get("SLIPPAGE_CAP", 2.0),
                "atm_strike": atm_strike_str,
                "spoof_anomaly": spoof_anomaly,
                "avp_vah": float(self.engine.vah),
                "avp_poc": float(self.engine.poc),
                "avp_val": float(self.engine.val),
                "cvd_velocity": f"{cvd_val:.1f} Δ"
            }

            # 4. Route Outbound Events (Fire-and-forget)
            # Always log to Supabase regardless of regime
            asyncio.create_task(log_signal(snapshot_data))
            
            # Dispatch to Discord ONLY if clean
            if regime != "REGIME_CHOP":
                logger.info(f"Firing {direction} Breakout Alert!")
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
    logger.info("Initializing Trishul Unified Runtime Execution Engine...")
    
    # Establish loops and connections
    await orchestrator.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Orchestrator graceful shutdown triggered by operator.")
