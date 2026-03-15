import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import aiohttp
import asyncio
from datetime import datetime, timezone
import logging
import json
import contextlib
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("CRYPTO-GEX")

BASE_URL = "https://www.deribit.com/api/v2/public/"
MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12
}

class Analytics:
    @staticmethod
    def max_pain(chain):
        if not chain: return 0
        strikes = sorted({o['k'] for o in chain})
        step = max(1, len(strikes)//100)
        min_pain = float('inf')
        pain_price = 0
        
        for price in strikes[::step]:
            loss = 0
            for o in chain:
                if o['ty'] == 'C' and price > o['k']: loss += (price - o['k']) * o['oi']
                elif o['ty'] == 'P' and price < o['k']: loss += (o['k'] - price) * o['oi']
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

class MarketData:
    def __init__(self):
        self.session = None
        self.cache = {}
        self.active_tickers = set()

    async def start(self):
        if not self.session:
            self.session = aiohttp.ClientSession(headers={"User-Agent": "CryptoGEX/1.0"})
        asyncio.create_task(self.polling_loop())

    async def stop(self):
        if self.session: await self.session.close()

    async def fetch(self, url):
        try:
            async with self.session.get(url, timeout=5) as resp:
                return await resp.json() if resp.status == 200 else None
        except Exception as e:
            logger.error(f"Fetch Error {url}: {e}")
            return None

    def _parse_expiry(self, date_str):
        try:
            day = int(date_str[:2])
            mon = date_str[2:5].upper()
            year = int(date_str[5:]) + 2000
            if mon in MONTH_MAP: return datetime(year, MONTH_MAP[mon], day, 8, 0, 0, tzinfo=timezone.utc)
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
        now = datetime.now(timezone.utc)
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
                
                if not (spot * 0.1 < k < spot * 3.0): continue

                chain.append({
                    "k": k,
                    "t": (exp - now).total_seconds() / 31536000,
                    "iv": d.get('mark_iv', 0) / 100,
                    "oi": d.get('open_interest', 0),
                    "vol": d.get('volume', 0),
                    "ty": parts[3],
                    "exp": parts[1]
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
            "chain": chain,
            "ts": now.strftime("%H:%M:%S")
        }

    async def polling_loop(self):
        while True:
            for ticker in list(self.active_tickers):
                try:
                    data = await self.snapshot(ticker)
                    self.cache[ticker] = data
                except Exception as e:
                    logger.error(f"Polling error for {ticker}: {e}")
            await asyncio.sleep(4)

engine = MarketData()

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    await engine.start()
    yield
    await engine.stop()

app = FastAPI(title="Crypto.GEX", lifespan=lifespan)

@app.websocket("/ws")
async def ws_handler(websocket: WebSocket):
    await websocket.accept()
    logger.info("Client connected")
    current_ticker = None
    try:
        while True:
            try:
                msg = await asyncio.wait_for(websocket.receive_text(), timeout=1.0)
                data = json.loads(msg)
                
                if data.get('action') == 'sub':
                    ticker = data['ticker'].upper()
                    current_ticker = ticker
                    logger.info(f"Subscribing to {ticker}")
                    engine.active_tickers.add(ticker)
                    
                    if ticker in engine.cache:
                        res = engine.cache[ticker]
                    else:
                        res = await engine.snapshot(ticker)
                        engine.cache[ticker] = res

                    if "error" in res:
                        await websocket.send_json({"type": "error", "msg": res['error']})
                    else:
                        await websocket.send_json({"type": "data", "payload": res})
                        
            except asyncio.TimeoutError:
                if current_ticker and current_ticker in engine.cache:
                    res = engine.cache[current_ticker]
                    if "error" not in res:
                        await websocket.send_json({"type": "data", "payload": res})
                
    except WebSocketDisconnect:
        logger.info("Client disconnected")
    except Exception as e:
        logger.error(f"WebSocket Error: {e}")
    finally:
        try: await websocket.close()
        except: pass

@app.get("/", response_class=HTMLResponse)
async def home():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, "index.html")
    
    try:
        with open(file_path, "r", encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        return "Error: index.html not found."

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
