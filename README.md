# 🔱 Project Trishul

Project Trishul is a high-frequency trading (HFT) data pipeline and execution orchestrator designed for real-time analysis of the Nifty Future order book. It ingests 200-level market depth data, applies advanced mathematical filtering to detect spoofing and institutional flow, and dispatches automated breakout alerts.

## 🏗️ Architecture

The system is built asynchronously using Python (`asyncio`), ensuring low-latency data parsing, processing, and downstream alerting.

### Core Modules

1. **`parser.py` (Data Ingestion)**
   - Connects to the **DhanHQ 200-level Market Depth API** via WebSockets.
   - Handles raw binary stream parsing (unpacking 12-byte headers and 16-byte depth rows).
   - Manages automatic reconnection and authentication.

2. **`engine.py` (Mathematical Core)**
   - Utilizes `numpy` for high-speed array operations on the 200-level order book.
   - **Anchored Volume Profile (AVP):** Dynamically tracks volume distribution, computing the Point of Control (POC), Value Area High (VAH), and Value Area Low (VAL).
   - **Spoof-Filtered Order Book Imbalance (OBI):** Analyzes the book in 4 macro-brackets (50 layers each). Applies a Quantity-to-Order (QTO) penalty to drop the mathematical weight of anomalous, single-order high-volume blocks, specifically penalizing liquidity residing in Low Volume Nodes (LVNs).
   - **Sweep VWAP:** Calculates expected slippage for block executions (e.g., 2500 quantities) by walking the book.
   - **Cumulative Volume Delta (CVD):** Tracks aggressive market buys vs. sells crossing the inside spread.

3. **`orchestrator.py` (Runtime Execution)**
   - Acts as the central nervous system, piping binary ticks directly into the `DOMEngine`.
   - **Chop Protection Filter:** A multi-variable state machine that prevents alert spam during ranging markets. It tracks OBI polarity. If OBI flips more than 5 times in a 60-second rolling window, it enters `REGIME_CHOP`. It only unlatches when OBI > |0.50| stabilizes for 15 seconds, backed by confirming CVD momentum.
   - **Textual ATM Selection:** Dynamically rounds the live Nifty Future price to the nearest 50-point strike interval (e.g., `NIFTY 22550 CE`) based on breakout direction.

4. **`adapters.py` (Downstream Integration)**
   - **Supabase Integration:** Asynchronously logs all system states and breakout metrics into a database for quantitative auditing.
   - **Discord Webhooks:** Dispatches clean, scannable markdown alerts. Alerts include directional bias (🟢/🔴), AVP value tiers, execution parameters (Micro-price, Sweep VWAP, Slippage), and explicit warnings if spoofing anomalies are detected in the deep book.

5. **`test_suite.py` (Quality Assurance)**
   - Comprehensive `unittest` suite operating without external connections.
   - Validates binary parsing safety, spoof-filter efficacy (asserting weight reduction of mocked spoof layers), Chop Protection regime transitions, and CVD arithmetic.

## ⚙️ Configuration (`config.json`)

All operational parameters are centralized in `config.json`:

```json
{
  "SECURITY_ID": 51437,
  "TOKEN": "TRISHUL_AUTH_TOKEN_ENV",
  "CLIENT_ID": "TRISHUL_CLIENT_ID_ENV",
  "TICK_SIZE": 0.05,
  "VALUE_AREA_PCT": 0.70,
  "STRIKE_INTERVAL": 50,
  "SLIPPAGE_CAP": 2.0
}
```
*Note: Sensitive tokens should be injected via environment variables (`TRISHUL_AUTH_TOKEN`, `SUPABASE_URL`, `DISCORD_WEBHOOK_URL`).*

## 🚀 Execution

Ensure dependencies are installed:
```bash
pip install numpy websockets aiohttp supabase
```

**Run the orchestrator:**
```bash
python orchestrator.py
```

**Run the test suite:**
```bash
python test_suite.py
```