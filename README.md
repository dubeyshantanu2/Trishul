# Project Trishul

## 1. Project Vision & Architecture Big Picture
Project Trishul is a local-first, low-latency market microstructure engine optimized exclusively for **Nifty Futures** (NSE Derivatives Segment). The system runs as a lightweight headless container pipeline deployed on **Fly.io**, stores transactional execution audits inside a **Supabase PostgreSQL database**, and routes trading signals directly to a **Discord channel via Webhooks**. 

This system does **not** execute automated broker trades. It acts purely as a high-precision data pipeline designed to give a manual options/futures trader an absolute execution edge.

```
       [ Dhan HQ WebSockets ] 
          /              \ 
 (200-Level Depth)    (Live Trade Ticks)
        |                  |
        v                  v
 [ parser.py ] --------> [ engine.py ] (AVP, CVD, Spoof-Filtered OBI)
                           |
                           v
                     [ orchestrator.py ] (Regime/Chop State Filter & ATM Rounding)
                          /         \
                         v           v
             [ Supabase Logging ]   [ Discord Alerts ]
```

### Critical Operational Boundaries (No-Go's)
* **Zero Options Data Streams:** The engine **never** pulls options chains, options Greeks, or options Implied Volatility (IV) over the network. This eliminates 90% of data bloat.
* **Pure Futures Ingestion:** The engine streams data for a **single** active Nifty Future Security ID at a time.
* **Textual Strike Recommendations:** Instead of reading options data, the engine reads the live Nifty Future price, applies a 50-point mathematical rounding calculation, and explicitly recommends the exact At-The-Money (ATM) option contract strike as a plain text string in the Discord alert.
* **Manual Contract Rollover:** There is no calendar-based expiry tracking logic. The user manually switches the tracked target Future Security ID inside `config.json` on expiry days to roll over positions and avoid decay.

---

## 2. The Trimmed Structural Triad
The core analytical engine abandons standard lagging indicator charting models and focuses on three microstructure vectors to confirm institutional activity:

1. **Anchored Volume Profile (AVP - The Map):** Tracks historical execution nodes (Point of Control, Value Area High, Value Area Low) starting from a fixed 9:15 AM IST market open anchor time.
2. **200-Level Full Market Depth (DOM - The Trigger):** Scans the entire visible order book depth across 200 bid and ask rows to measure immediate liquidity walls and directional pull.
3. **Cumulative Volume Delta (CVD - The Fuel):** Analyzes incoming tick-by-tick transaction data to calculate the net aggressive market buying versus selling pressure crossing the spread.

---

## 3. Mathematical Specifications (Plain Text Edition)

### A. Spoof-Filtered Order Book Imbalance (OBI_200)
The engine processes all 200 depth layers by segregating them into four sequential macro-brackets of 50 layers each. To eliminate high-frequency algorithmic manipulation (spoofing), every layer must pass an **Order-Count Ratio Filter** combined with an **AVP Reality Check** before impacting the final imbalance score.

```text
OBI_200 = (Sum(w_i * Q_bid_i * F_bid_i) - Sum(w_i * Q_ask_i * F_ask_i)) / (Sum(w_i * Q_bid_i * F_bid_i) + Sum(w_i * Q_ask_i * F_ask_i))
```
* **Where:**
    * `w_i` = 1 / i (Inverse distance decay weight for depth layer `i`)
    * `Q_bid_i` / `Q_ask_i` = Raw volume quantity at layer `i`
    * `F_bid_i` / `F_ask_i` = Spoof Penalty Factor (`1.0` if genuine, scales down to `0.05` if single-order wall inside an AVP Low Volume Node)
    * If a depth row contains a massive quantity (`Q_i`) driven by a single order entry (`Orders_i = 1`) **AND** that price row resides inside a calculated Low Volume Node (LVN) on the AVP map, `F_i` scales aggressively down to `0.05` (95% weight penalty).
    * If the layer represents a distributed volume backed by a large group of network participants (`Orders_i >= 3`), `F_i = 1.0` (Trusted Level).

### B. Micro-Price (P_micro)
Computes an unmanipulated, leading fair-value asset price based on Level 1 (L1) volume distribution:

```text
P_micro = (P_bid_1 * Q_ask_1 + P_ask_1 * Q_bid_1) / (Q_bid_1 + Q_ask_1)
```
* **Where:**
    * `P_bid_1` / `P_ask_1` = Level 1 Best Bid and Best Ask prices
    * `Q_bid_1` / `Q_ask_1` = Level 1 Best Bid and Best Ask quantities

### C. Order Book Sweep VWAP (VWAP_sweep)
Simulates filling a specific target size (`Q_target = 2500` contracts) down sequential depth rows to calculate the exact maximum execution price ceiling and slippage model before alerting:

```text
VWAP_sweep = Sum(P_i * q_i) / Q_target
```
* **Where:**
    * `P_i` = Price at depth layer `i`
    * `q_i` = Fractional quantity taken from layer `i` to fulfill the execution block
    * `Q_target` = Fixed target fill allocation size (Set to `2500` contracts for Nifty Futures)

### D. Cumulative Volume Delta (CVD)
Maintains a rolling metric based on trade execution pricing relative to the inside touch spread:

```text
If Trade_Price >= P_ask_1 Then:
    CVD_current = CVD_previous + Trade_Qty

If Trade_Price <= P_bid_1 Then:
    CVD_current = CVD_previous - Trade_Qty
```

---

## 4. Algorithmic State Control & Safety Rules

### The Chop Protection Filter (Sign-Flip Counter)
To ensure the engine completely neutralizes false signals inside low-volume sideways consolidation boundaries, the orchestrator tracks the polarity of the calculated `OBI_200` across a rolling 60-second window.

```text
If (OBI Sign Changes > 5 within 60s) Then:
    State = REGIME_CHOP
```
* **Operational State Lock:** While inside `REGIME_CHOP`, the system continues processing calculations in memory and logs records to Supabase, but **completely blocks and throttles all outbound Discord Webhooks**.
* **Unlatching Condition:** The engine exits chop mode and resumes alerts only when a sustained directional OBI vector breaks past absolute `|0.50|` and holds structural stability for more than 15 consecutive seconds, backed by a clean upward or downward acceleration in the CVD trend line.

### Textual ATM Strike Selection Logic
When a breakout is validated, the system reads the current Nifty Future price (`P_future`) and rounds it to the nearest 50-point interval to determine the text label for the Discord payload:

```text
Suggested Strike = round(P_future / 50) * 50
```

---

## 5. Relational Database Schema (schema.sql)
The coding agent must ensure the Supabase integration perfectly maps to this database schema. Run this script inside the Supabase SQL editor:

```sql
-- Enable UUID extension if not present
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Signal Audit Logs Table
CREATE TABLE IF NOT EXISTS trishul_signals (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    timestamp_epoch BIGINT NOT NULL,
    instrument_ticker VARCHAR(50) NOT NULL,
    signal_direction VARCHAR(10) NOT NULL, -- 'LONG' or 'SHORT'
    spoof_filtered_obi NUMERIC(5,2) NOT NULL,
    current_cvd BIGINT NOT NULL,
    avp_poc NUMERIC(10,2) NOT NULL,
    avp_vah NUMERIC(10,2) NOT NULL,
    avp_val NUMERIC(10,2) NOT NULL,
    calculated_micro_price NUMERIC(10,2) NOT NULL,
    calculated_sweep_vwap NUMERIC(10,2) NOT NULL,
    suggested_atm_strike VARCHAR(20) NOT NULL,
    current_regime VARCHAR(20) NOT NULL -- 'TRENDING' or 'CHOP'
);

-- Performance Indexes for Quick Post-Trade Analysis
CREATE INDEX IF NOT EXISTS idx_trishul_ticker ON trishul_signals(instrument_ticker);
CREATE INDEX IF NOT EXISTS idx_trishul_timestamp ON trishul_signals(timestamp_epoch DESC);
```

---

## 6. Comprehensive Project Workspace Layout

The agent must structure the python application into these exact 5 functional modules and 1 static configuration block:

### 1. config.json (System Configurations)
```json
{
  "SYSTEM_SETTINGS": {
    "supabase_logging_enabled": true,
    "chop_tracking_window_seconds": 60,
    "discord_webhook_url": "YOUR_DISCORD_WEBHOOK_SECURE_URL"
  },
  "NIFTY_FUTURES_PROFILE": {
    "security_id": "13",
    "strike_interval": 50,
    "lot_size": 50,
    "trade_lots": 50,
    "obi_trigger_threshold": 0.50,
    "chop_max_sign_flips": 5,
    "spoof_min_orders_per_layer": 3
  }
}
```

### Dynamic Environment Configuration (Fly.io Secrets)
The orchestrator is designed to prioritize environment variables over `config.json`. This allows for zero-downtime hot-swapping of trading instruments directly from your deployment's secrets panel (e.g., Fly.io Secrets) without touching the codebase.

Available overrides:
* `TARGET_SECURITY_ID`: The DhanHQ exchange security ID (e.g., `13`).
* `TARGET_SYMBOL_NAME`: *(Optional)* The prefix for the discord alert (e.g., `NIFTY`). If omitted, the alert will just output the strike (e.g., `22500 CE`).
* `TARGET_TICK_SIZE`: *(Optional)* The tick size decimal. Defaults to `0.05` (NSE standard).
* `TARGET_STRIKE_INTERVAL`: *(Optional)* The ATM rounding interval. Defaults to `50`.
* `TARGET_LOT_SIZE`: *(Optional)* The underlying lot size of the instrument. Defaults to `50`.
* `TARGET_TRADE_LOTS`: *(Optional)* The number of lots you intend to trade. The engine multiplies this by `TARGET_LOT_SIZE` to calculate the exact slippage sweep depth. Defaults to `50`.

#### Why do Lot Size and Trade Lots matter?
While your trade size does **NOT** affect the core LONG/SHORT algorithmic signals (which track pure institutional flow), it is critical for two personalized execution metrics:
1. **The Slippage Reality (Sweep VWAP):** If the breakout fires and you want to execute 100 lots, the engine needs to know your size. It multiplies `TARGET_LOT_SIZE` by `TARGET_TRADE_LOTS` and mathematically "walks down" the live 200-level depth book until it fills that exact quantity. This gives you the precise execution price (Sweep VWAP) and slippage you will incur.
2. **The Spoof Anomaly Flag:** The engine compares the `Micro-price` (Level 1 true price) to your personalized `Sweep VWAP`. If the order book is "hollow" (e.g., a fake massive order at Level 1, but empty behind it), your VWAP will drop significantly. The system will detect this massive slippage and trigger a Discord warning: `⚠️ WARNING: SPOOFING ANOMALY DETECTED`.

### 2. parser.py (Low-Latency Binary Data Ingestion)
* Opens and handles asynchronous WebSocket loops utilizing `websockets`.
* Stream A: `wss://full-depth-api.dhan.co/twohundreddepth` (200-level binary book payload).
* Stream B: `wss://api-feed.dhan.co` (Live order trade ticks).
* Unpacks 12-byte headers and 16-byte data packets cleanly using Python's `struct` library without off-by-one errors.
* Implements background `asyncio` loop tasks to process exchange connection heartbeat rules cleanly.

### 3. engine.py (Microstructure Calculations Core)
* Formulates and maintains the in-memory Anchored Volume Profile (AVP) arrays using `numpy`.
* Calculates the spoof-filtered OBI, parsing 200 depth layers down into the 4 macro-brackets.
* Implements the L1 Micro-price calculator and the depth sweeping execution simulator loop.
* Updates the running CVD line frame-by-frame based on incoming market order trade side matchings.

### 4. adapters.py (Outbound Notification & Logging Connectors)
* Leverages `aiohttp` for asynchronous HTTP requests.
* Contains `log_signal()` to push execution audits straight into the Supabase database schema.
* Contains `dispatch_webhook()` to structure clean, highly visual Discord embeds featuring bold metric arrays, warnings if single-order spoofing walls are detected, and separate highlighted rows for entries, stops, targets, and the textual strike price recommendation.

### 5. orchestrator.py (Unified Event Loop Routing)
* Acts as the core execution thread binding the parser, engine, and adapters together.
* Processes incoming ticks, runs the state configuration limits, handles the active sign-flip chop detection counter, handles the option strike mathematical calculation, and locks down communication channels during chop intervals.

### 6. test_suite.py (Offline Test Matrix)
* Built using the native `unittest` framework to isolate and debug components without needing a live connection to the Dhan HQ API or Supabase.
* Validates binary packet header slicing, spoof filter calculations inside low volume nodes, and verifies that the system automatically shifts its global state to `REGIME_CHOP` when erratic imbalance polarity strings are introduced.
