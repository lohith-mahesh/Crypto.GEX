# Crypto.Gex | Options Market Structure Terminal

This terminal provides real-time monitoring of Deribit options liquidity and volatility dynamics. It utilizes a hybrid-compute architecture to identify dealer positioning and price magnets across BTC, ETH, and SOL.

![Terminal Dashboard](Demo.png)

---

## Technical Architecture

The system operates on a split-execution model to balance precision with low-latency updates:

* **Backend (Python/FastAPI):** Executes a closed-form Black-Scholes-Merton (BSM) model to calculate Greeks for every instrument in the Deribit universe. It handles data ingestion, index price tracking, and multi-currency book consolidation.
* **Frontend (Vanilla JS/Plotly):** Performs real-time aggregation of Net Gamma Exposure (GEX) and geometric rendering. This offloads the high-frequency summation of the option chain to the client hardware.

---

## Core Analytics

### 1. Net Gamma Exposure (GEX)
The terminal calculates the dollar value that dealers must hedge per 1% move in the spot price. This identifies "Gamma Walls" where hedging activity either dampens or accelerates price volatility.

$$GEX = \Gamma \times \text{Open Interest} \times \text{Spot}^{2} \times 0.01$$

### 2. Oracle 1-Sigma Range
The system projects the expected move for the current session or specific expiry using a weighted Implied Volatility ($\sigma$) metric.

$$\text{Expected Move} = \text{Spot} \times \sigma \times \sqrt{T}$$

### 3. Max Pain and Pinning
The engine identifies the "Max Pain" strike by iterating through the strike ladder to find the local minimum of the total loss function for option buyers. This serves as a center of gravity for price action as expiration approaches.

### 4. VWAP (Volume Structure)
Calculates the volume-weighted average strike price to identify where the bulk of trading activity is concentrated.

### 5. Sentiment (PCR)
The Put/Call Ratio based on Open Interest. Values greater than 1 indicate bearish sentiment, while values below 0.7 indicate bullish sentiment.

### 6. Term Structure
Aggregates the weighted Implied Volatility (IV) grouped by expiration dates to visualize the volatility surface across time.

---

## Data Pipeline Logic

### Real-Time Ingestion
* **Concurrency:** Utilizes `asyncio.gather` and `aiohttp` to fetch index prices and book summaries for Coin-margined and USDC-margined instruments simultaneously.
* **Caching:** Implements a 5-second TTL (Time-To-Live) cache to stay within Deribit public rate limits of 20 requests per second.

### Filtering and Assumptions
* **Moneyness:** Only strikes within a 20% to 250% range of the spot price are processed to eliminate illiquid data.
* **Temporal:** Expired or near-instantaneous contracts are discarded to prevent mathematical artifacts in GEX spikes.
* **Risk-Free Rate:** Hardcoded at 5% (0.05) to approximate the cost of carry in crypto-native margin environments.
* **Skew:** The 25-delta skew is calculated as a ±10% spot price approximation for computational efficiency.

---

## API & WebSocket Documentation

The backend streams real-time updates to the client via WebSockets.

* **Endpoint:** `ws://<host>:<port>/ws`
* **Subscription Payload:**
  To subscribe to a specific ticker's option chain, send the following JSON payload:
  ```json
  {
    "action": "sub",
    "ticker": "BTC"
  }
