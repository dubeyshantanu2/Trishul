import asyncio
import struct
import logging
import websockets
from urllib.parse import urlencode

# Configure logging for production tracing
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("DhanPipeline")

class NiftyFuturePipeline:
    """
    Asynchronous data pipeline for DhanHQ API Nifty Futures market data.
    Maintains dual WebSocket connections for 200-depth and live trade ticks.
    """
    DEPTH_WSS_URL = "wss://full-depth-api.dhan.co/twohundreddepth"
    TICK_WSS_URL = "wss://api-feed.dhan.co"
    
    # 12-byte response header: Message Length (int16), Response Code (byte), Exchange Segment (byte), Security ID (int32), No. of Rows (uint32)
    HEADER_FMT = "<h b b i I"
    HEADER_SIZE = struct.calcsize(HEADER_FMT)
    
    # 16-byte depth row structure: Price (float64), Quantity (uint32), Orders (uint32)
    DEPTH_ROW_FMT = "<d I I"
    DEPTH_ROW_SIZE = struct.calcsize(DEPTH_ROW_FMT)

    # Payload Protocol Response Codes
    CODE_BID_DEPTH = 41
    CODE_ASK_DEPTH = 51
    CODE_TRADE_TICK = 8 

    def __init__(self, security_id: int, token: str, client_id: str):
        self.security_id = security_id
        self.token = token
        self.client_id = client_id
        # State for aggressive side determination
        self.best_bid = 0.0
        self.best_ask = 0.0

    def _parse_depth(self, payload: bytes, side: str, num_rows: int):
        """Unpacks 16-byte depth structures based on row count from header."""
        if num_rows == 0:
            return

        # Read Top of Book for aggressive side calculation
        top_price, top_qty, _ = struct.unpack(self.DEPTH_ROW_FMT, payload[:self.DEPTH_ROW_SIZE])
        if side == "BID":
            self.best_bid = top_price
        else:
            self.best_ask = top_price

        # Full depth parsing
        for i in range(num_rows):
            offset = i * self.DEPTH_ROW_SIZE
            if offset + self.DEPTH_ROW_SIZE > len(payload):
                break
            price, qty, orders = struct.unpack(self.DEPTH_ROW_FMT, payload[offset:offset + self.DEPTH_ROW_SIZE])
            # Suppress deep logging for performance, but parse fully
            if i == 0:
                logger.debug(f"[DEPTH] {side} L1 | Px: {price:.2f} | Sz: {qty} | Ord: {orders}")

    def _parse_tick(self, payload: bytes):
        """Extracts live trade tick and computes aggressive execution side."""
        # Assuming tick starts with 12 bytes: Price (float64) and Qty (uint32)
        if len(payload) >= 12:
            price, qty = struct.unpack("<d I", payload[:12])
            
            # Execution side logic
            side = "UNKNOWN"
            if price >= self.best_ask and self.best_ask > 0:
                side = "AGGRESSIVE_BUYER"
            elif price <= self.best_bid and self.best_bid > 0:
                side = "AGGRESSIVE_SELLER"

            logger.info(f"[TICK] {side} | Px: {price:.2f} | Sz: {qty}")

    def _parse_binary_stream(self, data: bytes):
        """Low-latency binary parser."""
        if len(data) < self.HEADER_SIZE:
            return
            
        msg_len, response_code, segment, sec_id, num_rows = struct.unpack(self.HEADER_FMT, data[:self.HEADER_SIZE])
        
        # Security routing filter
        if sec_id != self.security_id:
            return

        payload = data[self.HEADER_SIZE:]

        if response_code == self.CODE_BID_DEPTH:
            self._parse_depth(payload, side="BID", num_rows=num_rows)
        elif response_code == self.CODE_ASK_DEPTH:
            self._parse_depth(payload, side="ASK", num_rows=num_rows)
        elif response_code == self.CODE_TRADE_TICK:
            self._parse_tick(payload)

    async def _connect_and_stream(self, url: str, name: str):
        """Maintains concurrent WebSocket connections with auto-reconnect."""
        params = {'token': self.token}
        if "twohundreddepth" in url:
            params.update({'clientId': self.client_id, 'authType': '2'})
            
        ws_url = f"{url}?{urlencode(params)}"
        
        while True:
            try:
                logger.info(f"[{name}] Establishing connection to {url}")
                async with websockets.connect(ws_url) as ws:
                    logger.info(f"[{name}] Connected.")
                    async for message in ws:
                        if isinstance(message, bytes):
                            self._parse_binary_stream(message)
                        
            except websockets.ConnectionClosed:
                logger.warning(f"[{name}] WebSocket disconnected. Retrying...")
            except Exception as e:
                logger.error(f"[{name}] Pipeline exception: {e}. Reconnecting in 3s...")
                await asyncio.sleep(3)

    async def run(self):
        """Bootstraps the asynchronous data pipeline."""
        logger.info(f"Booting Nifty Future Data Pipeline for Security ID: {self.security_id}")
        await asyncio.gather(
            self._connect_and_stream(self.DEPTH_WSS_URL, "DEPTH_STREAM"),
            self._connect_and_stream(self.TICK_WSS_URL, "TICK_STREAM")
        )

if __name__ == "__main__":
    # Project Trishul Execution Entry Point
    SECURITY_ID = 13  # Nifty Future ID representation
    TOKEN = "TRISHUL_AUTH_TOKEN_ENV"
    CLIENT_ID = "TRISHUL_CLIENT_ID_ENV"
    
    pipeline = NiftyFuturePipeline(security_id=SECURITY_ID, token=TOKEN, client_id=CLIENT_ID)
    try:
        asyncio.run(pipeline.run())
    except KeyboardInterrupt:
        logger.info("Pipeline shutdown triggered by operator.")
