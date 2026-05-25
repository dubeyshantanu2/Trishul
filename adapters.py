import os
import logging
import asyncio
import aiohttp
from datetime import datetime, timezone
from supabase import create_client, Client

logger = logging.getLogger("TrishulAdapters")

# Environment variables for configuration
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

# Initialize Supabase client
supabase: Client = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        logger.error(f"Failed to initialize Supabase client: {e}")

async def log_signal(snapshot_data: dict) -> bool:
    """
    Asynchronously inserts breakout audit data into a Supabase table.
    """
    if not supabase:
        logger.warning("Supabase client not initialized. Skipping log_signal.")
        return False
        
    try:
        payload = {
            "timestamp_epoch": snapshot_data.get("timestamp_epoch", int(datetime.now(timezone.utc).timestamp())),
            "instrument_ticker": snapshot_data.get("instrument_ticker", "UNKNOWN"),
            "signal_direction": snapshot_data.get("signal_direction", "UNKNOWN"),
            "spoof_filtered_obi": snapshot_data.get("spoof_filtered_obi", 0.0),
            "current_cvd": snapshot_data.get("current_cvd", 0),
            "avp_poc": snapshot_data.get("avp_poc", 0.0),
            "avp_vah": snapshot_data.get("avp_vah", 0.0),
            "avp_val": snapshot_data.get("avp_val", 0.0),
            "calculated_micro_price": snapshot_data.get("calculated_micro_price", 0.0),
            "calculated_sweep_vwap": snapshot_data.get("calculated_sweep_vwap", 0.0),
            "suggested_atm_strike": snapshot_data.get("suggested_atm_strike", "N/A"),
            "current_regime": snapshot_data.get("current_regime", "UNKNOWN")
        }
        
        # Run synchronous Supabase client operation in a thread pool
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(
            None, 
            lambda: supabase.table("trishul_signals").insert(payload).execute()
        )
        
        logger.info(f"Successfully logged signal to Supabase: {response.data}")
        return True
        
    except Exception as e:
        logger.error(f"Error logging signal to Supabase: {e}")
        return False

async def dispatch_webhook(alert_payload: dict) -> bool:
    """
    Asynchronously sends an HTTP POST request to a Discord channel webhook.
    Constructs a highly formatted markdown payload for scannability.
    """
    # Fallback to config or env variable if present in payload
    webhook_url = alert_payload.get("webhook_url", DISCORD_WEBHOOK_URL)
    
    if not webhook_url:
        logger.warning("DISCORD_WEBHOOK_URL not configured. Skipping webhook dispatch.")
        return False

    direction = alert_payload.get("direction", "UNKNOWN")
    color = 0x00FF00 if direction == "LONG" else 0xFF0000  # Green or Red
    emoji = "🟢" if direction == "LONG" else "🔴"
    
    spoof_warning = ""
    if alert_payload.get("spoof_anomaly", False):
        spoof_warning = "\n> ⚠️ **WARNING: SPOOFING ANOMALY DETECTED IN 200-LEVEL QUEUES** ⚠️"

    description = f"""
> {emoji} **DIRECTIONAL BIAS:** `{direction}`
{spoof_warning}

**AVP Value Tiers:**
> VAH: `{alert_payload.get('avp_vah', 0.0):.2f}`
> POC: `{alert_payload.get('avp_poc', 0.0):.2f}`
> VAL: `{alert_payload.get('avp_val', 0.0):.2f}`

**Order Flow Dynamics:**
> CVD Volume Velocity Line: `{alert_payload.get('cvd_velocity', 'N/A')}`

**Execution Parameters:**
```yaml
Micro-price:    {alert_payload.get('micro_price', 0.0):.2f}
Sweep VWAP:     {alert_payload.get('sweep_vwap', 0.0):.2f}
```
**Recommended Action:**
> 🎯 **ATM Strike:** `{alert_payload.get('atm_strike', 'N/A')}`
"""

    embed = {
        "title": "🔱 PROJECT TRISHUL BREAKOUT ALERT",
        "description": description,
        "color": color,
        "footer": {
            "text": f"Generated at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
        }
    }
    
    payload = {
        "embeds": [embed]
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(webhook_url, json=payload) as response:
                if response.status in (200, 204):
                    logger.info("Successfully dispatched Discord webhook.")
                    return True
                else:
                    logger.error(f"Discord webhook failed with status {response.status}: {await response.text()}")
                    return False
    except aiohttp.ClientError as e:
        logger.error(f"Network error dispatching webhook: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error dispatching webhook: {e}")
        return False
