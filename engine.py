import numpy as np

class DOMEngine:
    """
    High-frequency trading mathematics engine for Project Trishul.
    Optimized for low-latency processing of 200-level market depth and trade ticks.
    """
    def __init__(self, tick_size: float = 0.05, value_area_pct: float = 0.70):
        self.tick_size = tick_size
        self.value_area_pct = value_area_pct
        self.volume_profile = {}  # In-memory store: {price: total_volume}
        self.cvd = 0.0
        
        # AVP State
        self.poc = 0.0
        self.vah = 0.0
        self.val = 0.0
        self.total_volume = 0.0

    def update_avp(self, price: float, volume: int):
        """
        Maintains an in-memory Anchored Volume Profile.
        Calculates Point of Control (POC) and Value Area (VAH/VAL).
        """
        # Round to nearest tick for consistency
        price = round(price / self.tick_size) * self.tick_size
        
        # Update profile
        self.volume_profile[price] = self.volume_profile.get(price, 0) + volume
        self.total_volume += volume
        
        # Extract arrays for numpy operations
        prices = np.array(list(self.volume_profile.keys()))
        volumes = np.array(list(self.volume_profile.values()))
        
        # Sort by price
        idx = np.argsort(prices)
        prices = prices[idx]
        volumes = volumes[idx]
        
        # 1. Calculate POC
        self.poc = prices[np.argmax(volumes)]
        
        # 2. Calculate Value Area (VAH/VAL)
        target_va_vol = self.total_volume * self.value_area_pct
        
        poc_idx = np.where(prices == self.poc)[0][0]
        v_low_idx = poc_idx
        v_high_idx = poc_idx
        current_va_vol = volumes[poc_idx]
        
        while current_va_vol < target_va_vol:
            # Check expansion limits
            can_expand_down = v_low_idx > 0
            can_expand_up = v_high_idx < len(prices) - 1
            
            if not can_expand_down and not can_expand_up:
                break
                
            # Compare volume of next two rows (standard TPO/Volume Profile expansion logic)
            vol_down = volumes[v_low_idx - 1] if can_expand_down else -1
            vol_up = volumes[v_high_idx + 1] if can_expand_up else -1
            
            if vol_down >= vol_up:
                v_low_idx -= 1
                current_va_vol += vol_down
            else:
                v_high_idx += 1
                current_va_vol += vol_up
                
        self.val = prices[v_low_idx]
        self.vah = prices[v_high_idx]
        
        return self.poc, self.vah, self.val

    def calculate_spoof_filtered_obi(self, bids: np.ndarray, asks: np.ndarray):
        """
        Processes 200 depth layers grouped into 4 macro-brackets.
        Applies filters for Quantity-to-Order ratio and AVP Low Volume Nodes (LVN).
        
        bids/asks format: [[price, qty, orders], ...]
        """
        # Ensure input is numpy
        bids = np.asanyarray(bids)
        asks = np.asanyarray(asks)
        
        def process_side(depth_array):
            if depth_array.size == 0:
                return 0.0
            
            # Split into 4 brackets of 50 layers
            num_layers = len(depth_array)
            brackets = np.array_split(depth_array, 4)
            weighted_volume = 0.0
            
            # Bracket weights (closer layers have higher impact)
            bracket_weights = [1.0, 0.5, 0.25, 0.125]
            
            for i, bracket in enumerate(brackets):
                prices = bracket[:, 0]
                qtys = bracket[:, 1]
                orders = bracket[:, 2]
                
                # 1. Spoof Filter: Quantity-to-Order ratio
                # Penalize layers where orders are suspiciously low relative to qty
                qto_ratio = qtys / np.maximum(orders, 1)
                # Mean QTO for normalizing (approximation)
                mean_qto = np.median(qto_ratio) if qto_ratio.size > 0 else 1.0
                spoof_penalty = np.where(qto_ratio > mean_qto * 2.5, 0.2, 1.0)
                
                # 2. LVN Filter: Penalize volume in Low Volume Nodes
                # We check if the price is far from POC and has low historical volume
                lvn_penalty = np.ones_like(prices)
                for j, p in enumerate(prices):
                    hist_vol = self.volume_profile.get(p, 0)
                    if self.total_volume > 0:
                        rel_vol = hist_vol / (self.total_volume / len(self.volume_profile))
                        if rel_vol < 0.2: # LVN threshold
                            lvn_penalty[j] = 0.5
                
                # Apply filters and sum
                effective_qty = qtys * spoof_penalty * lvn_penalty
                weighted_volume += np.sum(effective_qty) * bracket_weights[i]
                
            return weighted_volume

        bid_pressure = process_side(bids)
        ask_pressure = process_side(asks)
        
        total_pressure = bid_pressure + ask_pressure
        if total_pressure == 0:
            return 0.0
            
        return (bid_pressure - ask_pressure) / total_pressure

    def calculate_micro_price(self, bids: np.ndarray, asks: np.ndarray):
        """
        Computes L1 volume-weighted fair value (Micro-price).
        bids/asks: L1 [price, qty, orders]
        """
        bid_px, bid_qty = bids[0, 0], bids[0, 1]
        ask_px, ask_qty = asks[0, 0], asks[0, 1]
        
        return (bid_px * ask_qty + ask_px * bid_qty) / (bid_qty + ask_qty)

    def calculate_sweep_vwap(self, book_side: np.ndarray, target_qty: int = 2500):
        """
        Evaluates execution slippage (Sweep VWAP) for a target quantity.
        """
        prices = book_side[:, 0]
        qtys = book_side[:, 1]
        
        cum_qty = np.cumsum(qtys)
        full_fill_idx = np.searchsorted(cum_qty, target_qty)
        
        if full_fill_idx >= len(prices):
            # Not enough liquidity to fill the full sweep
            return np.average(prices, weights=qtys)
            
        # Partial fill of the last layer
        sweep_prices = prices[:full_fill_idx + 1]
        sweep_qtys = qtys[:full_fill_idx + 1].copy()
        
        overfill = cum_qty[full_fill_idx] - target_qty
        sweep_qtys[-1] -= overfill
        
        return np.average(sweep_prices, weights=sweep_qtys)

    def update_cvd(self, trade_price: float, trade_qty: int, best_bid: float, best_ask: float):
        """
        Calculates Cumulative Volume Delta (CVD) based on aggressor side.
        """
        delta = 0
        if trade_price >= best_ask:
            delta = trade_qty  # Aggressive Buy
        elif trade_price <= best_bid:
            delta = -trade_qty # Aggressive Sell
            
        self.cvd += delta
        return self.cvd
