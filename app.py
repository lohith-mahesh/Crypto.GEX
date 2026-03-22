import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import aiohttp
import asyncio
import numpy as np
from datetime import datetime, timedelta
import webbrowser
import logging
import json
import contextlib
import os
import math

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("CRYPTO-GEX")

BASE_URL = "https://www.deribit.com/api/v2/public/"
MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12
}

class Greeks:
    @staticmethod
    def nd1(d):
        return 0.5 * (1.0 + math.erf(d / math.sqrt(2.0)))

    @staticmethod
    def npd1(d):
        return math.exp(-0.5 * d**2) / math.sqrt(2 * math.pi)

    @classmethod
    def calculate(cls, S, K, T, sigma, r, type):
        if T <= 0 or sigma <= 0:
            return 0.0, 0.0, 0.0, 0.0
        
        try:
            d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
            d2 = d1 - sigma * math.sqrt(T)
            
            if type == 'C':
                delta = cls.nd1(d1)
                gamma = cls.npd1(d1) / (S * sigma * math.sqrt(T))
                theta = -(S * cls.npd1(d1) * sigma) / (2 * math.sqrt(T)) - r * K * math.exp(-r * T) * cls.nd1(d2)
                vega = S * cls.npd1(d1) * math.sqrt(T)
            else:
                delta = cls.nd1(d1) - 1
                gamma = cls.npd1(d1) / (S * sigma * math.sqrt(T))
                theta = -(S * cls.npd1(d1) * sigma) / (2 * math.sqrt(T)) + r * K * math.exp(-r * T) * cls.nd1(-d2)
                vega = S * cls.npd1(d1) * math.sqrt(T)
            
            return delta, gamma, theta, vega
        except:
            return 0.0, 0.0, 0.0, 0.0

class Cache:
    def __init__(self):
        self.store = {}

    def get(self, key):
        entry = self.store.get(key)
        if entry and datetime.now() < entry['expiry']:
            return entry['data']
        return None

    def set(self, key, data, ttl=5):
        self.store[key] = {
            'data': data,
            'expiry': datetime.now() + timedelta(seconds=ttl)
        }

class Analytics:
    @staticmethod
    def max_pain(chain):
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
        c = sum(o['oi'] for o in chain if o['ty'] == 'C')
        p = sum(o['oi'] for o in chain if o['ty'] == 'P')
        return round(p/c, 2) if c > 0 else 0

    @staticmethod
    def weighted_iv(chain):
        total_oi = sum(o['oi'] for o in chain)
        if not total_oi: return 0
        return sum(o['iv'] * o['oi'] for o in chain) / total_oi

    @staticmethod
    def vwap(chain):
        total_vol = sum(o['vol'] for o in chain)
        if not total_vol: return 0
        return sum(o['k'] * o['vol'] for o in chain) / total_vol

    @staticmethod
    def skew_25d(chain, spot):
        calls = [o for o in chain if o['ty'] == 'C' and abs(o['k'] - spot*1.1) < spot*0.05]
        puts = [o for o in chain if o['ty'] == 'P' and abs(o['k'] - spot*0.9) < spot*0.05]
        if not calls or not puts: return 0
        c_iv = sum(o['iv'] for o in calls) / len(calls)
        p_iv = sum(o['iv'] for o in puts) / len(puts)
        return round(p_iv - c_iv, 4)

    @staticmethod
    def term_structure(chain):
        expiries = sorted({o['exp'] for o in chain})
        structure = []
        for e in expiries:
            subset = [o for o in chain if o['exp'] == e]
            structure.append({"exp": e, "iv": Analytics.weighted_iv(subset)})
        return structure

class MarketData:
    def __init__(self):
        self.session = None
        self.cache = Cache()

    async def start(self):
        if not self.session:
            self.session = aiohttp.ClientSession(headers={"User-Agent": "CryptoGEX/2.0"})

    async def stop(self):
        if self.session: 
            await self.session.close()

    async def fetch(self, url):
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
            logger.error(f"Network error: {e}")
            return None

    def _parse_expiry(self, date_str):
        try:
            day = int(date_str[:2])
            mon = date_str[2:5].upper()
            year = int(date_str[5:]) + 2000
            if mon in MONTH_MAP: return datetime(year, MONTH_MAP[mon], day)
        except: return None
        return None

    async def snapshot(self, ticker):
        spot = 0
        for idx in [f"{ticker.lower()}_usdc", f"{ticker.lower()}_usd"]:
            res = await self.fetch(f"{BASE_URL}get_index_price?index_name={idx}")
            if res and 'result' in res:
                spot = res['result']['index_price']
                break
        
        if not spot: return {"error": "Spot price not found"}

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
            if not name or name in seen: continue
            if not (name.startswith(f"{prefix}-") or name.startswith(f"{prefix}_")): continue
            seen.add(name)
            
            try:
                parts = name.split('-')
                if len(parts) < 4: continue
                exp = self._parse_expiry(parts[1])
                if not exp or exp <= now: continue
                k = float(parts[2])
                
                if not (spot * 0.2 < k < spot * 2.5): continue

                iv = d.get('mark_iv', 0) / 100
                t = (exp - now).total_seconds() / 31536000
                oi = d.get('open_interest', 0)
                ty = parts[3]

                delta, gamma, theta, vega = Greeks.calculate(spot, k, t, iv, 0.05, ty)

                chain.append({
                    "k": k, "t": t, "iv": iv, "oi": oi,
                    "vol": d.get('volume', 0), "ty": ty, "exp": parts[1],
                    "delta": delta, "gamma": gamma, "theta": theta, "vega": vega
                })
            except: continue

        if not chain: return {"error": "No options found"}
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

engine = MarketData()

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    await engine.start()
    webbrowser.open("http://127.0.0.1:8000")
    yield
    await engine.stop()

app = FastAPI(title="Crypto.GEX Terminal", lifespan=lifespan)

@app.websocket("/ws")
async def ws_handler(websocket: WebSocket):
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
        pass
    except Exception as e:
        logger.error(f"WS Error: {e}")
    finally:
        try: 
            await websocket.close()
        except: 
            pass

@app.get("/", response_class=HTMLResponse)
async def home():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, "index.html")
    try:
        with open(file_path, "r", encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        return f"Error: index.html not found."

@app.get("/api/v1/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/api/v1/snapshot/{ticker}")
async def get_ticker_snapshot(ticker: str):
    data = await engine.snapshot(ticker.upper())
    return data

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
