"""
Kafka producer: Simulate a real-time stock price feed for 8 scottish equities.
Publishes one price update per symbol every 2 seconds to Kafka topic: stock_prices.

Price simulation logic
    - Fetches the latest closing price from yfinance as the base price
    - Applies a small random walk (Geometric Brownian Motion) on each tick
    - This mimics realistic intraday price fluctuation

Useage:
    python kafka_producer.py
"""

import json
import time
import random
import logging
from datetime import datetime, timezone

import numpy as np
import yfinance as yf
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

#Config
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = "stock_prices"

# 8 scottish equities tracked in this pipeline
SYMBOLS = [
    "NWG.L",  # NatWest Group
    "ABDN.L",  # Aberdeen Standard Investments
    "SMT.L",  # Scottish Mortgage Investment Trust
    "MNKS.L", #Monks Investment Trust
    "AV.L",   # Aviva
    "HIK.L",  # Hikma Pharmaceuticals
    "SSE.L",  # SSE plc(energy)
    "WEIR.L", # Weir Group
]

# Price simulation parameters
TICK_INTERVAL_SEC = 2   # Seconds between each batch of price updates
VOLATILITY =0.005    # Per-tick volatility (0.5% std dev — realistic for intraday)
TRADING_HOURS_ONLY = False   # Set True to pause outside LSE trading hours (optional)

# STEP 1 Fetch base prices from yfinance

def fetch_base_prices(symbols: list[str]) -> dict[str, float]:
    """
    Download the most recent closing price for each symbol using yfinance.
    These serve as the starting point for our simulated price walk.

    Returns:
       dict mapping symbol  -> base price, e.g. {"NWG.L": 3.12,...} 
    """
    logger.info("Fetching base prices from yfinance...")
    base_prices = {}

    for symbol in symbols:
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="5d") # Last 5 days in case today has no data yet

            if hist.empty:
                logger.warning(f"No data returned for {symbol}, using fallback price 100.0")
                base_prices[symbol] = 100.0
            else:
                last_close = float(hist["Close"].iloc[-1])
                base_prices[symbol] = last_close
                logger.info(f" {symbol}: base price = {last_close:.4f} GBX")
            
        except Exception as e:
            logger.error(f"Failed to fetch price for {symbol}: {e}")
            base_prices[symbol] = 100.0   # Fallback to avoid crashing
    
    logger.info(f"Base prices loaded for {len(base_prices)} symbols.")
    return base_prices

# STEP 2: Simulate price movement

def simulate_next_price(current_price: float, volatility: float = VOLATILITY) -> float:
    """ 
    Generate the next simulated price using Geometric Brownian Motion(GBM).
    GBM is the standard model for stock price simulation:
       P(t+1) = P(t) * exp(random_shock)
    where random_shock ~ Normal(0, volatility)

        - Prices can never go negative(realistic)
        - Percentage changes are normally distributed (matches real market data)
        - Used in Black-Scholes options pricing model
    
    Args:
        current_price: the price from the previous tick
        volatility: per-tick std deviation of log returns
    
    Returns:
        float: the new simulated price
    """
    shock = np.random.normal(loc=0,scale=volatility)
    return round(current_price * np.exp(shock), 4)

def simulate_volume() -> int:
    """
    Generate a random trade volume for the tick.
    Based on a log-normal distribution - volumes are always positive and occasionally spike (mimicking real market bursts).
    
    Returns:
        int: simulated volume in number of shares
    """
    return int(np.random.lognormal(mean=9.5,sigma=1.2))  # typical range: ~1k-200k

# STEP 3: Build the Kafka message payload

def build_message(
        symbol: str,
        price: float,
        prev_price: float,
        volume: int    
) -> dict:
    """
    Construct the JSON message payload for one price tick.
    
    Schema:
        symbol (str) : ticker symbol, e.g. "NWG.L
        price  (float) : current simulated price in GBX (pence)
        prev_price  (float) : price from the previous tick
        change_pct  (float) : percentage change vs prev tick
        volume  (int) : simulated trade volume
        timestamp  (str) : ISO 8601 UTC timestamp
        source  (str) : always "simulated" to distinguish from real data

    Args:
        symbol: stock ticker
        price: current price
        prev_price: previous tick price (used to compute change_pct)
        volume: simulated volume

    Returns:
        dict representing one Kafka message
    """
    change_pct = round((price - prev_price) / prev_price * 100, 4) if prev_price else 0.0

    return  {
        "symbol": symbol,
        "price": price,
        "prev_price": prev_price,
        "change_pct": change_pct,
        "volume": volume,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "simulated"
    }

#STEP 4: Create Kafka Producer 
def create_producer() -> KafkaProducer:
    """
    Initialise and return a KafkaProducer instance.
   
    Key settings:
        bootstrap_servers: Kafka broker address
        value_serializer: converts Python dict -> UTF-8 JSON bytes before sending
        acks='all' : wait for all in-sync replicas to acknowledge(safe write)
        retries=3 : retry up to 3 times on transient failures
        linger_ms=10 : batch messages for 10ms to improve throughput
    """
    logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"), # key = symbol, used for partitioning
        acks="all",
        retries=3,
        linger_ms=10,
        request_timeout_ms=30000,
    )
    logger.info("Kafka Producer connected successfully.")
    return producer

# STEP 5: Delivery callbacks
def on_send_success(record_metadata):
    """Called when a message is successfully delivered to Kafka."""
    logger.debug(
        f"Delivered to topic={record_metadata.topic}"
        f"partition={record_metadata.partition}"
        f"offset={record_metadata.offset}"
    )

def on_send_error(exc):
    """Called when a message fails to deliver."""
    logger.error(f"Failed to deliver message: {exc}")

# STEP 6: Main producer loop

def run_producer():
    """
    Main loop: continuously simulate and publish stock prices.
    
    Flow per tick:
        1. For each symbol, simulate the next price
        2. Build the JSON message
        3. Send to Kafka (async, with callbacks)
        4. Update the current price state
        5. Sleep for TICK_INTERVAL_SEC seconds
        6. Repeat indefinitely (Ctrl+C to stop)
    """

    #Load base prices once at startup
    current_prices = fetch_base_prices(SYMBOLS)

    #Create Kafka producer
    producer = create_producer()

    tick = 0
    logger.info(f"Starting price feed - publishing to topic '{KAFKA_TOPIC}' EVERY {TICK_INTERVAL_SEC}s")
    logger.info("Press Ctrl+C to stop.\n")

    try:
        while True:
            tick += 1
            logger.info(f"-- Tick {tick} ------------------")

            for symbol in SYMBOLS:
                prev_price = current_prices[symbol]
                new_price = simulate_next_price(prev_price)
                volume = simulate_volume()

                message = build_message(
                    symbol=symbol,
                    price=new_price,
                    prev_price=prev_price,
                    volume=volume
                )

                #Send to Kafka - use symbol as the message key
                #This ensures all messages for the same stock always go to the same partition
                producer.send(
                    topic=KAFKA_TOPIC,
                    key=symbol,
                    value=message
                ).add_callback(on_send_success).add_errback(on_send_error)

                #Log the price update
                direction = "↑" if new_price > prev_price else "↓" if new_price < prev_price else "-"
                logger.info(
                    f" {symbol:<8} {direction} {new_price:>10.4f} GBX "
                    f"({message['change_pct']:+.4f}%) vol={volume:,}" 
                )

                #Update state for next tick
                current_prices[symbol] = new_price
            #Flush ensures all buffered messages are sent before sleeping
            producer.flush()
            logger.info(f" All {len(SYMBOLS)} messages sent. Sleeping for {TICK_INTERVAL_SEC}s...\n")
            time.sleep(TICK_INTERVAL_SEC)
    except KeyboardInterrupt:
        logger.info("\nProducer stopped by user (KeyboardInterrupt).")

    except KafkaError as e: 
        logger.error(f"Kafka error: {e}")

    finally:
        producer.flush()
        producer.close()
        logger.info("Kafka Producer closed.")

# Entry point
if __name__ == "__main__":
    run_producer()