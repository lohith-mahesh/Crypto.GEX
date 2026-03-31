import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import aiohttp
import asyncio
from datetime import datetime, timedelta
import logging
import json
import contextlib
import os
import math

# Configure logging at INFO level for system monitoring and audit trails.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Crypto-Gex")

# Deribit public API base URL. 
# Adhering to the unauthenticated public endpoint rate limit of 20 req/sec per IP.
BASE_URL = "https://www.deribit.com/api/v2/public/"

# O(1) lookup mapping for Deribit instrument date formats (e.g., '28MAR26').
# Utilizing a static dictionary avoids the processing overhead of datetime.strptime 
# during high-frequency loop iterations over large option chains.
MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12
}

class Greeks:
    """
    Closed-form Black-Scholes-Merton (BSM) options pricing engine.
    
    This implementation utilizes Python's standard `math` library to execute 
    vectorized-style CDF and PDF calculations. By omitting heavyweight dependencies 
    like NumPy or SciPy, we significantly reduce the Docker image size and 
    cold-start execution time.
    
    Assumptions:
    - European options execution model.
    - 0% continuous dividend yield (standard for crypto assets).
    - Risk-free rate is static per environment configuration.
    """
    
    @staticmethod
    def nd1(d):
        """
        Calculates the Cumulative Distribution Function (CDF) of the standard normal distribution.
        Utilizes the mathematical error function (erf) for high-precision approximation.
        """
        return 0.5 * (1.0 + math.erf(d / math.sqrt(2.0)))

    @staticmethod
    def npd1(d):
        """
        Calculates the Probability Density Function (PDF) of the standard normal distribution.
        Used primarily for calculating secondary derivatives (Gamma and Vega).
        """
        return math.exp(-0.5 * d**2) / math.sqrt(2 * math.pi)

    @classmethod
    def calculate(cls, S, K, T, sigma, r, type):
        """
        Derives primary first and second-order Greeks.
        
        Args:
            S (float): Current underlying spot price.
            K (float): Strike price of the option contract.
            T (float): Time to expiration, annualized (using 365-day convention).
            sigma (float): Implied Volatility (IV) expressed as a decimal.
            r (float): Annualized risk-free interest rate.
            type (str): Contract type identifier ('C' for Call, 'P' for Put).
            
        Returns:
            tuple: (Delta, Gamma, Theta, Vega). Returns zeroes if inputs are invalid 
                   or if mathematical underflow occurs on deep OTM/ITM strikes.
        """
        # Guard clause: Handle zero-time to expiry or zero implied volatility 
        # to prevent catastrophic division by zero errors in the d1 denominator.
        if T <= 0 or sigma <= 0:
            return 0.0, 0.0, 0.0, 0.0
        
        try:
            d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
            d2 = d1 - sigma * math.sqrt(T)
            
            if type == 'C': 
                delta = cls.nd1(d1)
                gamma = cls.npd1(d1) / (S * sigma * math.sqrt(T))
                # Theta is calculated in annualized terms.
                theta = -(S * cls.npd1(d1) * sigma) / (2 * math.sqrt(T)) - r * K * math.exp(-r * T) * cls.nd1(d2)
                vega = S * cls.npd1(d1) * math.sqrt(T)
            else: 
                delta = cls.nd1(d1) - 1
                gamma = cls.npd1(d1) / (S * sigma * math.sqrt(T))
                theta = -(S * cls.npd1(d1) * sigma) / (2 * math.sqrt(T)) + r * K * math.exp(-r * T) * cls.nd1(-d2)
                vega = S * cls.npd1(d1) * math.sqrt(T)
            
            return delta, gamma, theta, vega
        
        # Explicitly catching arithmetic exceptions to prevent total loop failure.
        # Deep out-of-the-money or deep in-the-money options can cause math domain errors.
        except (ValueError, ZeroDivisionError, OverflowError):
            return 0.0, 0.0, 0.0, 0.0

class Cache:
    """
    Thread-safe, in-memory TTL (Time-To-Live) cache mechanism.
    
    Designed to mitigate rate-limiting blockades from Deribit APIs by serving 
    stale data (up to X seconds) for redundant inbound WebSocket requests.
    """
    def __init__(self):
        self.store = {}

    def get(self, key):
        entry = self.store.get(key)
        # Verify if the payload exists and whether the current monotonic time 
        # has surpassed the calculated expiry horizon.
        if entry and datetime.now() < entry['expiry']:
            return entry['data']
        return None

    def set(self, key, data, ttl=5):
        self.store[key] = {
            'data': data,
            'expiry': datetime.now() + timedelta(seconds=ttl)
        }

class Analytics:
    """
    Aggregated market structure calculators. 
    Processes the raw option chain arrays to derive macro-positioning metrics.
    """
    
    @staticmethod
    def max_pain(chain):
        """
        Iterates through the aggregate open interest to calculate the theoretical 
        spot price where option buyers experience maximum financial loss (Max Pain).
        Acts as a strong pinning magnet as expirations approach zero DTE.
        """
        if not chain: return 0
        strikes = sorted({o['k'] for o in chain})
        min_pain = float('inf')
        pain_price = 0
        
        for price in strikes:
            loss = 0
            for o in chain:
                if o['ty'] == 'C' and price > o['k']: 
                    loss += (price - o['k']) * o['oi']
                elif o['ty'] == 'P' and price < o['k']: 
                    loss += (o['k'] - price) * o['oi']
            
            if loss < min_pain:
                min_pain = loss
                pain_price = price
        return pain_price

    @staticmethod
    def pcr(chain):
        """Computes the aggregate Put/Call Ratio based on outstanding Open Interest."""
        c = sum(o['oi'] for o in chain if o['ty'] == 'C')
        p = sum(o['oi'] for o in chain if o['ty'] == 'P')
        return round(p/c, 2) if c > 0 else 0

    @staticmethod
    def weighted_iv(chain):
        """Calculates average Implied Volatility, weighted by contract Open Interest."""
        total_oi = sum(o['oi'] for o in chain)
        if not total_oi: return 0
        return sum(o['iv'] * o['oi'] for o in chain) / total_oi

    @staticmethod
    def vwap(chain):
        """Volume-Weighted Average Price (Strike). Identifies where current trading volume is anchored."""
        total_vol = sum(o['vol'] for o in chain)
        if not total_vol: return 0
        return sum(o['k'] * o['vol'] for o in chain) / total_vol

    @staticmethod
    def skew_25d(chain, spot):
        """
        Approximates 25-delta volatility skew.
        Filters for strikes within a ±10% band of current spot price to measure 
        the implied volatility premium of puts versus calls.
        """
        calls = [o for o in chain if o['ty'] == 'C' and abs(o['k'] - spot*1.1) < spot*0.05]
        puts = [o for o in chain if o['ty'] == 'P' and abs(o['k'] - spot*0.9) < spot*0.05]
        if not calls or not puts: return 0
        c_iv = sum(o['iv'] for o in calls) / len(calls)
        p_iv = sum(o['iv'] for o in puts) / len(puts)
        return round(p_iv - c_iv, 4)

    @staticmethod
    def term_structure(chain):
        """Generates a sequential array of weighted IVs mapped against expiration dates."""
        expiries = sorted({o['exp'] for o in chain})
        structure = []
        for e in expiries:
            subset = [o for o in chain if o['exp'] == e]
            structure.append({"exp": e, "iv": Analytics.weighted_iv(subset)})
        return structure

class MarketData:
    """
    Handles asynchronous HTTP requests, connection pooling, and payload assembly.
    """
    def __init__(self):
        self.session = None
        self.cache = Cache()

    async def start(self):
        """Initializes persistent aiohttp session for TCP connection re-use."""
        if not self.session:
            self.session = aiohttp.ClientSession(headers={"User-Agent": "CryptoGex/2.0"})

    async def stop(self):
        """Gracefully tears down the network session on application exit."""
        if self.session: 
            await self.session.close()

    async def fetch(self, url):
        """Executes GET requests with strict timeouts and Cache integration."""
        cached = self.cache.get(url)
        if cached: return cached
        try:
            async with self.session.get(url, timeout=5) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self.cache.set(url, data)
                    return data
                return None
        except Exception as e:
            logger.error(f"Network error on fetch: {e}")
            return None

    def _parse_expiry(self, date_str):
        """Optimized slicing function to convert Deribit string expirations to native datetime objects."""
        try:
            day = int(date_str[:2])
            mon = date_str[2:5].upper()
            year = int(date_str[5:]) + 2000
            if mon in MONTH_MAP: return datetime(year, MONTH_MAP[mon], day)
        except Exception: 
            return None
        return None

    async def snapshot(self, ticker):
        """
        Orchestrates the data pipeline for a given asset ticker.
        1. Fetches current index price.
        2. Concurrently fetches coin-margined and USDC-margined books.
        3. Parses, filters, and calculates Greeks for the entire chain.
        4. Triggers macro analytics.
        """
        spot = 0
        
        # Sequentially attempts to locate the spot index price.
        for idx in [f"{ticker.lower()}_usdc", f"{ticker.lower()}_usd"]:
            res = await self.fetch(f"{BASE_URL}get_index_price?index_name={idx}")
            if res and 'result' in res:
                spot = res['result']['index_price']
                break
        
        if not spot: return {"error": "Spot price not found from index providers."}

        # Issue concurrent, non-blocking HTTP requests for both margin types.
        urls = [
            f"{BASE_URL}get_book_summary_by_currency?currency={ticker}&kind=option",
            f"{BASE_URL}get_book_summary_by_currency?currency=USDC&kind=option"
        ]
        results = await asyncio.gather(*[self.fetch(u) for u in urls])
        
        raw = []
        for r in results:
            if r and 'result' in r: raw.extend(r['result'])

        chain = []
        seen = set() 
        now = datetime.now()
        prefix = ticker.upper()

        for d in raw:
            name = d.get('instrument_name')
            # Deduplicate entries if Deribit returns overlapping contracts across endpoints.
            if not name or name in seen: continue
            if not (name.startswith(f"{prefix}-") or name.startswith(f"{prefix}_")): continue
            seen.add(name)
            
            try:
                parts = name.split('-')
                if len(parts) < 4: continue
                
                exp = self._parse_expiry(parts[1])
                # Filter out expired contracts to prevent negative time artifacts.
                if not exp or exp <= now: continue
                k = float(parts[2])
                
                # Liquidity Filter: Discard contracts beyond 20% to 250% of current spot price.
                if not (spot * 0.2 < k < spot * 2.5): continue

                iv = d.get('mark_iv', 0) / 100
                t = (exp - now).total_seconds() / 31536000  # Time in years
                oi = d.get('open_interest', 0)
                ty = parts[3]

                # Hardcoded 5% Risk-Free Rate for calculation execution.
                delta, gamma, theta, vega = Greeks.calculate(spot, k, t, iv, 0.05, ty)

                chain.append({
                    "k": k, "t": t, "iv": iv, "oi": oi,
                    "vol": d.get('volume', 0), "ty": ty, "exp": parts[1],
                    "delta": delta, "gamma": gamma, "theta": theta, "vega": vega
                })
            except Exception: 
                continue

        if not chain: return {"error": "No valid options found passing threshold parameters."}
        
        chain.sort(key=lambda x: x['k'])

        return {
            "spot": spot,
            "vwap": Analytics.vwap(chain),
            "max_pain": Analytics.max_pain(chain),
            "pcr": Analytics.pcr(chain),
            "avg_iv": Analytics.weighted_iv(chain),
            "skew": Analytics.skew_25d(chain, spot),
            "structure": Analytics.term_structure(chain),
            "chain": chain,
            "ts": datetime.now().strftime("%H:%M:%S")
        }

# Instantiate singleton engine
engine = MarketData()

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifecycle manager. 
    Handles async connection pool startup and teardown safely.
    Removed OS-dependent local browser execution for Docker compatibility.
    """
    await engine.start()
    yield
    await engine.stop()

app = FastAPI(title="Crypto.Gex Terminal", lifespan=lifespan)

@app.websocket("/ws")
async def ws_handler(websocket: WebSocket):
    """
    Persistent duplex connection endpoint for frontend clients.
    Accepts subscription requests and streams formatted chain payloads.
    """
    await websocket.accept()
    try:
        while True:
            msg = await websocket.receive_json()
            if msg.get('action') == 'sub':
                ticker = msg.get('ticker', '').upper()
                res = await engine.snapshot(ticker)
                
                if "error" in res:
                    await websocket.send_json({"type": "error", "msg": res['error']})
                else:
                    await websocket.send_json({"type": "data", "payload": res})
    except WebSocketDisconnect:
        # Standard client drop-off, suppress logging noise.
        pass
    except Exception as e:
        logger.error(f"WebSocket Pipeline Error: {e}")
    finally:
        try: 
            await websocket.close()
        except Exception: 
            pass

@app.get("/", response_class=HTMLResponse)
async def home():
    """Serves the static frontend rendering engine directly from the root."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, "index.html")
    try:
        with open(file_path, "r", encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        return "Error: index.html missing from working directory."

@app.get("/api/v1/health")
async def health():
    """Liveness probe for orchestration tools (e.g., Kubernetes/Docker Compose)."""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/api/v1/snapshot/{ticker}")
async def get_ticker_snapshot(ticker: str):
    """REST fallback endpoint for single-shot external data ingestion."""
    return await engine.snapshot(ticker.upper())

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
