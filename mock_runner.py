import asyncio
import struct
import json
import logging
from orchestrator import OrchestratedPipeline

# Set up logging for the mock runner
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MockRunner")

class MockPipeline(OrchestratedPipeline):
    """
    Overrides the WebSocket connection logic to feed synthetic binary data.
    """
    async def run(self):
        logger.info(f"Booting Mock Pipeline for Security ID: {self.security_id}")
        
        # Give the orchestrator a moment to initialize
        await asyncio.sleep(1)
        
        # 1. Simulate setting up the initial Market Depth
        # We will create an unbalanced book (stronger bids) to simulate a LONG bias
        num_rows = 50
        sec_id = self.security_id
        
        # Generate Bid Depth (Stronger)
        bids_payload = bytearray()
        for i in range(num_rows):
            price = 22000.0 - (i * 0.05)
            qty = 1000 - (i * 10)  # High quantity on bids
            orders = 5
            bids_payload.extend(struct.pack("<d I I", price, qty, orders))
            
        header_len = 12 + len(bids_payload)
        bid_header = struct.pack("<h b b i I", header_len, 41, 1, sec_id, num_rows)
        self._parse_binary_stream(bid_header + bids_payload)

        # Generate Ask Depth (Weaker)
        asks_payload = bytearray()
        for i in range(num_rows):
            price = 22001.0 + (i * 0.05)
            qty = 500 - (i * 5)  # Lower quantity on asks
            orders = 5
            asks_payload.extend(struct.pack("<d I I", price, qty, orders))
            
        header_len = 12 + len(asks_payload)
        ask_header = struct.pack("<h b b i I", header_len, 51, 1, sec_id, num_rows)
        self._parse_binary_stream(ask_header + asks_payload)
        
        logger.info("Mock Depth Loaded.")
        
        # 2. Simulate Trade Ticks to build CVD and AVP
        # Simulating aggressive buying
        for _ in range(10):
            tick_payload = struct.pack("<d I", 22001.0, 50) # Hitting the ask
            header_len = 12 + len(tick_payload)
            tick_header = struct.pack("<h b b i I", header_len, 8, 1, sec_id, 0)
            self._parse_binary_stream(tick_header + tick_payload)
            await asyncio.sleep(0.1)

        logger.info("Mock Ticks Loaded.")
        
        # 3. Simulate a massive spoofed order on the ASK side to trigger a breakout signal and spoof warning
        # An order of 10000 qty with just 1 order count
        asks_payload = bytearray()
        asks_payload.extend(struct.pack("<d I I", 22001.0, 500, 5)) # L1
        asks_payload.extend(struct.pack("<d I I", 22005.0, 10000, 1)) # Massive Spoof Order at L2
        for i in range(2, num_rows):
            price = 22001.0 + (i * 0.05)
            asks_payload.extend(struct.pack("<d I I", price, 100, 5))
            
        header_len = 12 + len(asks_payload)
        ask_header = struct.pack("<h b b i I", header_len, 51, 1, sec_id, num_rows)
        self._parse_binary_stream(ask_header + asks_payload)
        
        # Give asyncio tasks time to execute dispatch_webhook and log_signal
        await asyncio.sleep(2)
        logger.info("Mock Execution Completed.")

async def main():
    with open("config.json", "r") as f:
        config = json.load(f)
        
    pipeline = MockPipeline(config)
    await pipeline.run()

if __name__ == "__main__":
    asyncio.run(main())
