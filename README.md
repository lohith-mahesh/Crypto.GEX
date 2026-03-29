# Crypto.Gex | Options Liquidity & Volatility Terminal

A high-performance analytics environment designed for real-time monitoring of Deribit options market structure. This project utilizes server-side Black-Scholes modeling and liquidity integration to identify dealer positioning and volatility-driven price magnets across BTC, ETH, and SOL.

![Terminal Dashboard](Demo.png)

## Core Technical Architecture

### 1. Net Gamma Exposure (GEX) Modeling

The engine calculates the dollar value of the underlying asset that option dealers must hedge per 1% move in the spot price. This identifies "Gamma Walls" where dealer hedging activity either dampens or accelerates price volatility.

**Standard GEX Equation:**
`GEX = Γ × Open Interest × Spot² × 0.01`

The terminal aggregates Net GEX across all active strikes, providing a visual heatmap of support (Positive GEX) and resistance (Negative GEX) zones.

### 2. Server-Side Greeks Engine (Black-Scholes)

Unlike standard front-end implementations, this terminal executes a closed-form Black-Scholes-Merton model on the backend to ensure precision and low-latency updates. 

* **State Estimation:** The model calculates d1 and d2 to derive Delta (Δ), Gamma (Γ), Theta (Θ), and Vega (ν) for every instrument in the Deribit universe.
* **Risk-Free Dynamics:** Incorporates a dynamic risk-free rate (r), default 5%, to account for the cost of carry in crypto-native margin environments.

### 3. Oracle 1-Sigma Range Projection

The system utilizes a weighted Implied Volatility (σ) metric to project the expected move for the current session based on time to expiry (T).

**Expected Move Formula:**
`1σ Move = Spot × Average σ × √(T)`

This provides a "Volatility Cone" on the chart, identifying where the market is pricing a 68% probability of price containment.

### 4. Max Pain & Pinning Analysis

The terminal identifies the "Max Pain" strike—the price level where the aggregate value of outstanding options is minimized at expiry.

* **Mechanism:** The engine iterates through the entire strike ladder to find the local minimum of the total loss function for option buyers.

This metric serves as a secondary "Center of Gravity" for price action as expiration approaches, highlighting potential pinning behavior.

### 5. Volume Structure (VWAP)

The system calculates the Volume-Weighted Average Price (Strike) to identify the center of gravity for today’s trading activity.

**Strike VWAP Calculation:**
`Strike VWAP = Σ(Strike × Volume) / Σ(Volume)`

This identifies whether the current day's volume is concentrating at OTM (Out-of-the-Money) strikes (K) or ITM (In-the-Money) directional hedging.

## Data Pipeline & Rigor

### Real-Time Ingestion

The scanner utilizes an asynchronous WebSocket loop to fetch market data from Deribit. To manage API rate limits and memory overhead, the backend implements a secondary caching layer with a 5-second TTL (Time-To-Live).

### Data Filtering

To maintain signal integrity, the engine applies strict filtering protocols:
* **Moneyness Filter:** Only strikes within a 20% to 250% range of the spot price are processed to remove illiquid "dust" strikes.
* **Temporal Filter:** Expired or near-instantaneous expiries (T ≈ 0) are discarded to prevent Gamma spikes from distorting the aggregate GEX profile.

## Logic Stack

* **Language:** Python 3.10+
* **Backend:** FastAPI (Asynchronous WebSocket handling and REST API)
* **Frontend:** Vanilla JS, Tailwind CSS, Plotly.js (Real-time bar and line geometry)
* **Statistics:** NumPy (Vectorized math for Greeks and GEX)
* **Network:** Aiohttp (Concurrent API fetching)

## Deployment

The project is containerized via Docker and optimized for Hugging Face Spaces. It utilizes a slim Debian-based Python image to minimize cold-start times while maintaining the computational overhead required for real-time BSM calculations.
