import unittest
import struct
import time
import numpy as np
from parser import NiftyFuturePipeline
from engine import DOMEngine
from orchestrator import ChopProtectionFilter

class TestTrishulPipeline(unittest.TestCase):
    def test_parser_binary_safety(self):
        pipeline = NiftyFuturePipeline(security_id=13, token="mock", client_id="mock")
        
        num_rows = 2
        header_size = 12
        row_size = 16
        msg_len = header_size + (num_rows * row_size)
        
        # Header: MsgLen(int16), Code(byte), Segment(byte), SecID(int32), NumRows(uint32)
        header = struct.pack("<h b b i I", msg_len, 41, 1, 13, num_rows)
        # Rows: Price(float64), Qty(uint32), Orders(uint32)
        row1 = struct.pack("<d I I", 22000.0, 1000, 10)
        row2 = struct.pack("<d I I", 21999.0, 500, 5)
        
        payload = header + row1 + row2
        
        # Test BID depth parsing
        pipeline._parse_binary_stream(payload)
        self.assertEqual(pipeline.best_bid, 22000.0)

    def test_engine_spoof_filter(self):
        engine = DOMEngine()
        
        # Seed volume profile to establish a baseline and define LVNs
        for p in range(22000, 22010):
            engine.update_avp(float(p), 100)
            
        normal_bids = np.array([
            [22000.0, 500, 10],
            [21999.0, 500, 10]
        ])
        
        # Spoof layer: High qty (5000) backed by a single order (1) in an LVN (21900.0)
        spoof_asks = np.array([
            [22001.0, 500, 10],
            [21900.0, 5000, 1] 
        ])
        
        obi = engine.calculate_spoof_filtered_obi(normal_bids, spoof_asks)
        
        # Without filter, 5000 qty would dominate (OBI heavily negative).
        # With spoof + LVN penalty filters (F_i drops to 0.05), the weight drops significantly.
        self.assertGreater(obi, -0.5)

    def test_chop_protection_regime(self):
        chop_filter = ChopProtectionFilter(window_seconds=60, max_flips=5)
        
        # Simulate 6 polarity flips within a 45 second window
        timestamps = [0, 5, 10, 15, 20, 25, 30]
        obis = [0.6, -0.6, 0.6, -0.6, 0.6, -0.6, 0.6]
        
        original_time = time.time
        state = "REGIME_TREND"
        try:
            for ts, obi in zip(timestamps, obis):
                time.time = lambda t=ts: t
                state = chop_filter.update(obi, cvd=100.0)
        finally:
            time.time = original_time
            
        self.assertEqual(state, "REGIME_CHOP")

    def test_update_cvd_calculation(self):
        engine = DOMEngine()
        
        best_bid = 22000.0
        best_ask = 22001.0
        
        # Aggressive Buy (Trade hits the Ask)
        cvd_1 = engine.update_cvd(22001.0, 50, best_bid, best_ask)
        self.assertEqual(cvd_1, 50)
        
        # Aggressive Sell (Trade hits the Bid)
        cvd_2 = engine.update_cvd(22000.0, 20, best_bid, best_ask)
        self.assertEqual(cvd_2, 30)
        
        # Trade inside the spread (Neutral)
        cvd_3 = engine.update_cvd(22000.5, 100, best_bid, best_ask)
        self.assertEqual(cvd_3, 30)

if __name__ == '__main__':
    unittest.main()
