"""
SMC On-Demand Signal Bot
━━━━━━━━━━━━━━━━━━━━━━━━
Send any symbol to Telegram → get SMC analysis back.

No Binance / Bybit / MT5 account needed.
Data fetched from PUBLIC endpoints (free, no API key).

Supported formats:
  BTCUSDT   → Binance public API (crypto)
  ETHUSDT
  EURUSD    → yfinance (forex/indices)
  GBPUSD
  NIFTY50
  etc.
"""

import os
import time
import logging
import requests
import yfinance as yf
import concurrent.futures
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple
from enum import Enum
from dotenv import load_dotenv

# ── Telegram library ──────────────────────────
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes
)

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
log = logging.getLogger('SMCBot')

# ═══════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════

class Config:
    TELEGRAM_BOT_TOKEN   = os.getenv('TELEGRAM_BOT_TOKEN', '')
    TELEGRAM_CHAT_ID     = os.getenv('TELEGRAM_CHAT_ID', '')
    HTF                  = '1h'
    LTF                  = '5m'
    CANDLE_LIMIT         = 100   # reduced for speed
    SWING_LOOKBACK       = 5
    MIN_CONFLUENCE_SCORE = 3
    MIN_RR_RATIO         = 1.2
    SL_BUFFER_PCT        = 0.002
    DISPLACEMENT_MULT    = 1.0

# ═══════════════════════════════════════════════
# ENUMS
# ═══════════════════════════════════════════════

class SignalDirection(Enum):
    LONG     = 'LONG'
    SHORT    = 'SHORT'
    NO_TRADE = 'NO TRADE'

# ═══════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════

@dataclass
class Candle:
    time  : str
    open  : float
    high  : float
    low   : float
    close : float
    volume: float = 0.0

    def body_high(self) : return max(self.open, self.close)
    def body_low(self)  : return min(self.open, self.close)
    def is_bullish(self): return self.close > self.open
    def is_bearish(self): return self.close < self.open
    def body_size(self) : return abs(self.close - self.open)
    def range_size(self): return self.high - self.low


@dataclass
class SwingPoint:
    type  : str
    price : float
    index : int
    broken: bool = False


@dataclass
class OrderBlock:
    direction: str
    zone_low : float
    zone_high: float
    midpoint : float
    index    : int
    status   : str = 'fresh'


@dataclass
class FVG:
    direction: str
    zone_low : float
    zone_high: float
    midpoint : float
    index    : int
    status   : str = 'fresh'


@dataclass
class SMCSignal:
    direction        : SignalDirection
    symbol           : str
    entry_low        : float
    entry_high       : float
    stop_loss        : float
    target_1         : float
    target_2         : float
    target_3         : float
    rr_ratio         : float
    confluence_score : int
    trend            : str
    block_type       : str
    reasons          : List[str] = field(default_factory=list)
    warnings         : List[str] = field(default_factory=list)
    timestamp        : str = ''

    def is_valid(self) -> bool:
        return (self.direction != SignalDirection.NO_TRADE and
                self.rr_ratio  >= Config.MIN_RR_RATIO and
                self.confluence_score >= Config.MIN_CONFLUENCE_SCORE)

# ═══════════════════════════════════════════════
# DATA FETCHER (PUBLIC — NO API KEY NEEDED)
# ═══════════════════════════════════════════════

class PublicDataFetcher:

    BINANCE_TF_MAP = {
        '1m' :'1m' , '3m' :'3m' , '5m' :'5m' , '15m':'15m',
        '30m':'30m', '1h' :'1h' , '2h' :'2h' , '4h' :'4h' ,
        '6h' :'6h' , '1d' :'1d' , '1w' :'1w' , '1M' :'1M'
    }

    YFINANCE_TF_MAP = {
        '5m' :'5m' , '15m':'15m', '30m':'30m',
        '1h' :'1h' , '4h' :'1h' , '1d' :'1d' ,
        '1w' :'1wk', '1M' :'1mo'
    }

    FOREX_PAIRS = [
        'EURUSD','GBPUSD','USDJPY','AUDUSD','USDCAD',
        'USDCHF','NZDUSD','GBPJPY','EURJPY','EURGBP',
        'AUDJPY','CADJPY','GBPAUD','EURAUD','CHFJPY',
        'CHFUSD','EURCAD','EURCHF','EURNZD','GBPCAD',
        'GBPCHF','GBPNZD','NZDCAD','NZDCHF','NZDJPY',
        'AUDCAD','AUDCHF','AUDNZD','CADCHF','USDHKD',
        'USDSGD','USDZAR','USDMXN','USDINR','USDCNH',
        'XAUUSD','XAGUSD'
    ]

    def detect_type(self, symbol: str) -> str:
        crypto_endings = ['USDT','BTC','ETH','BNB','BUSD']
        s = symbol.upper().replace('/', '').replace('-', '')
        if any(s.endswith(e) for e in crypto_endings):
            return 'crypto'
        return 'forex'

    def normalize(self, symbol: str) -> str:
        return (symbol.upper()
                .replace('/', '')
                .replace('-', '')
                .replace(' ', ''))

    def fetch(self, symbol: str, tf: str,
              limit: int = None) -> List[Candle]:
        limit = limit or Config.CANDLE_LIMIT
        s     = self.normalize(symbol)
        kind  = self.detect_type(s)
        if kind == 'crypto':
            return self._binance(s, tf, limit)
        return self._yfinance(s, tf, limit)

    def _binance(self, symbol: str, tf: str,
                 limit: int) -> List[Candle]:
        try:
            url    = 'https://api.binance.com/api/v3/klines'
            params = {
                'symbol'  : symbol,
                'interval': self.BINANCE_TF_MAP.get(tf, '1h'),
                'limit'   : limit
            }
            r = requests.get(url, params=params, timeout=10)
            if r.status_code != 200:
                log.error(f'Binance error {r.status_code}: {r.text[:100]}')
                return []
            return [
                Candle(
                    time  = str(datetime.fromtimestamp(
                        row[0]/1000, tz=timezone.utc)),
                    open  = float(row[1]),
                    high  = float(row[2]),
                    low   = float(row[3]),
                    close = float(row[4]),
                    volume= float(row[5])
                )
                for row in r.json()
            ]
        except Exception as e:
            log.error(f'Binance fetch error: {e}')
            return []

    def _yfinance(self, symbol: str, tf: str,
                  limit: int) -> List[Candle]:
        try:
            yf_symbol = self._to_yfinance_symbol(symbol)
            period    = self._limit_to_period(tf, limit)
            yf_tf     = self.YFINANCE_TF_MAP.get(tf, '1h')

            ticker = yf.Ticker(yf_symbol)
            df     = ticker.history(period=period, interval=yf_tf)

            if df.empty:
                return []

            candles = []
            for ts, row in df.iterrows():
                candles.append(Candle(
                    time  = str(ts),
                    open  = float(row['Open']),
                    high  = float(row['High']),
                    low   = float(row['Low']),
                    close = float(row['Close']),
                    volume= float(row.get('Volume', 0))
                ))
            return candles[-limit:]

        except Exception as e:
            log.error(f'yfinance fetch error {symbol}: {e}')
            return []

    def _to_yfinance_symbol(self, symbol: str) -> str:
        # Crypto map (fallback if Binance fails)
        crypto_map = {
            'BTCUSDT' : 'BTC-USD',
            'ETHUSDT' : 'ETH-USD',
            'SOLUSDT' : 'SOL-USD',
            'BNBUSDT' : 'BNB-USD',
            'XRPUSDT' : 'XRP-USD',
            'ADAUSDT' : 'ADA-USD',
            'DOGEUSDT': 'DOGE-USD',
            'DOTUSDT' : 'DOT-USD',
            'MATICUSDT':'MATIC-USD',
            'AVAXUSDT' :'AVAX-USD',
            'LINKUSDT' :'LINK-USD',
            'LTCUSDT'  :'LTC-USD',
            'ATOMUSDT' :'ATOM-USD',
            'UNIUSDT'  :'UNI-USD',
            'ETCUSDT'  :'ETC-USD',
        }
        if symbol in crypto_map:
            return crypto_map[symbol]

        if symbol in self.FOREX_PAIRS:
            return symbol[:3] + symbol[3:] + '=X'

        index_map = {
            'NIFTY50'   : '^NSEI'   ,
            'NIFTY'     : '^NSEI'   ,
            'BANKNIFTY' : '^NSEBANK',
            'SPX'       : '^GSPC'   ,
            'NDX'       : '^NDX'    ,
            'DJI'       : '^DJI'    ,
            'FTSE'      : '^FTSE'   ,
            'DAX'       : '^GDAXI'
        }
        return index_map.get(symbol, symbol)

    def _limit_to_period(self, tf: str, limit: int) -> str:
        period_map = {
            '1m' : '7d'  , '5m' : '60d' , '15m': '60d',
            '30m': '60d' , '1h' : '730d', '4h' : '730d',
            '1d' : '5y'  , '1w' : '10y' , '1M' : '10y'
        }
        return period_map.get(tf, '60d')

    def current_price(self, symbol: str) -> float:
        candles = self.fetch(symbol, '1m', limit=1)
        return candles[-1].close if candles else 0.0

# ═══════════════════════════════════════════════
# SMC ANALYSIS ENGINE
# ═══════════════════════════════════════════════

class SMCAnalysisEngine:

    def detect_swings(self, candles: List[Candle],
                       lb: int = None) -> List[SwingPoint]:
        lb  = lb or Config.SWING_LOOKBACK
        pts = []
        for i in range(lb, len(candles) - lb):
            c = candles[i]
            if all(c.high > candles[i-k].high and
                   c.high > candles[i+k].high
                   for k in range(1, lb+1)):
                pts.append(SwingPoint('swing_high', c.high, i))
            if all(c.low < candles[i-k].low and
                   c.low < candles[i+k].low
                   for k in range(1, lb+1)):
                pts.append(SwingPoint('swing_low', c.low, i))
        return sorted(pts, key=lambda x: x.index)

    def classify_structure(self,
                            swings: List[SwingPoint]) -> List[SwingPoint]:
        result = []
        ph = pl = None
        for s in swings:
            if s.type == 'swing_high':
                s.type = 'HH' if (ph is None or s.price > ph.price) else 'LH'
                ph = s
            else:
                s.type = 'HL' if (pl is None or s.price > pl.price) else 'LL'
                pl = s
            result.append(s)
        return result

    def get_trend(self, swings: List[SwingPoint]) -> str:
        if len(swings) < 4: return 'ranging'
        last = [s.type for s in swings[-6:]]
        if last.count('HH') + last.count('HL') >= 4: return 'uptrend'
        if last.count('LL') + last.count('LH') >= 4: return 'downtrend'
        return 'ranging'

    def get_choch(self, candles: List[Candle],
                   swings: List[SwingPoint],
                   trend: str) -> Optional[float]:
        if trend == 'uptrend':
            hls = [s for s in swings if s.type == 'HL']
            if not hls: return None
            last_hl = max(hls, key=lambda x: x.index)
            for i in range(last_hl.index + 1, len(candles)):
                if candles[i].body_low() < last_hl.price:
                    return last_hl.price
        elif trend == 'downtrend':
            lhs = [s for s in swings if s.type == 'LH']
            if not lhs: return None
            last_lh = max(lhs, key=lambda x: x.index)
            for i in range(last_lh.index + 1, len(candles)):
                if candles[i].body_high() > last_lh.price:
                    return last_lh.price
        return None

    def find_obs(self, candles: List[Candle],
                  direction: str,
                  start: int = 0) -> List[OrderBlock]:
        obs = []
        ar  = (sum(c.range_size() for c in candles) /
               len(candles)) if candles else 0.0001
        thr = ar * Config.DISPLACEMENT_MULT

        for i in range(start, len(candles) - 3):
            c = candles[i]
            if direction == 'bullish' and not c.is_bearish(): continue
            if direction == 'bearish' and not c.is_bullish(): continue

            disp = any(
                (candles[j].is_bullish() if direction == 'bullish'
                 else candles[j].is_bearish()) and
                candles[j].body_size() >= thr
                for j in range(i+1, min(i+4, len(candles)))
            )
            if not disp: continue

            obs.append(OrderBlock(
                direction = direction,
                zone_low  = c.low,
                zone_high = c.high,
                midpoint  = (c.low + c.high) / 2,
                index     = i
            ))

        for ob in obs:
            for c in candles[ob.index+3:]:
                if c.low <= ob.zone_high and c.high >= ob.zone_low:
                    ob.status = 'tapped'
                    break
        return obs

    def find_fvgs(self, candles: List[Candle],
                   direction: str,
                   start: int = 0) -> List[FVG]:
        fvgs = []
        for i in range(start, len(candles) - 2):
            c1, c3 = candles[i], candles[i+2]
            if direction == 'bullish' and c1.high < c3.low:
                fvgs.append(FVG(
                    'bullish', c1.high, c3.low,
                    (c1.high + c3.low)/2, i+2
                ))
            elif direction == 'bearish' and c1.low > c3.high:
                fvgs.append(FVG(
                    'bearish', c3.high, c1.low,
                    (c3.high + c1.low)/2, i+2
                ))
        return fvgs

    def find_liquidity(self, candles: List[Candle]) -> Dict:
        swings = self.detect_swings(candles)
        highs  = sorted(
            [s.price for s in swings if 'high' in s.type or s.type == 'HH'],
            reverse=True
        )
        lows   = sorted(
            [s.price for s in swings if 'low' in s.type or s.type == 'LL']
        )
        return {
            'buy_side' : highs[:3],
            'sell_side': lows[:3]
        }

    def find_idm(self, candles: List[Candle],
                  direction: str) -> Optional[float]:
        swings = self.detect_swings(candles, lb=3)
        if direction == 'bullish':
            lows = [s for s in swings if 'low' in s.type]
            if len(lows) >= 2:
                return lows[-2].price
        else:
            highs = [s for s in swings if 'high' in s.type]
            if len(highs) >= 2:
                return highs[-2].price
        return None

    def check_sweep(self, candles: List[Candle],
                     level: float,
                     direction: str,
                     lookback: int = 10) -> bool:
        recent = candles[-lookback:]
        for c in recent:
            if direction == 'bullish':
                if c.low < level and c.close > level:
                    return True
            else:
                if c.high > level and c.close < level:
                    return True
        return False

    def ltf_choch(self, ltf_candles: List[Candle],
                   direction: str) -> bool:
        if len(ltf_candles) < 20: return False
        recent  = ltf_candles[-50:]
        swings  = self.detect_swings(recent, lb=2)
        cswings = self.classify_structure(swings)
        trend   = self.get_trend(cswings)
        if direction == 'bullish':
            return (trend == 'uptrend' or
                    any(s.type in ['HL', 'HH'] for s in cswings[-6:]))
        else:
            return (trend == 'downtrend' or
                    any(s.type in ['LH', 'LL'] for s in cswings[-6:]))

    def get_bias(self, daily_candles : List[Candle],
                  weekly_candles: List[Candle]) -> Dict:
        def candle_bias(c: Candle) -> str:
            if not c: return 'neutral'
            return ('bullish' if c.close > c.open
                    else 'bearish' if c.close < c.open
                    else 'neutral')

        wb = candle_bias(weekly_candles[-1]) if weekly_candles        else 'neutral'
        db = candle_bias(daily_candles[-1])  if daily_candles         else 'neutral'
        pb = candle_bias(daily_candles[-2])  if len(daily_candles)>=2 else 'neutral'

        votes = [wb, db, pb]
        bulls = votes.count('bullish')
        bears = votes.count('bearish')

        if bulls >= 2  : combined = 'bullish'
        elif bears >= 2: combined = 'bearish'
        else           : combined = 'neutral'

        daily_open = daily_candles[-1].open if daily_candles else 0

        return {
            'weekly'    : wb,
            'daily'     : db,
            'combined'  : combined,
            'daily_open': daily_open
        }

    def get_location(self, price: float,
                      candles: List[Candle]) -> str:
        highs = [c.high for c in candles[-50:]]
        lows  = [c.low  for c in candles[-50:]]
        sh, sl = max(highs), min(lows)
        eq     = (sh + sl) / 2
        if price > eq: return 'premium'
        if price < eq: return 'discount'
        return 'equilibrium'

    def analyse(self,
                symbol        : str,
                htf_candles   : List[Candle],
                ltf_candles   : List[Candle],
                daily_candles : List[Candle],
                weekly_candles: List[Candle]) -> SMCSignal:

        ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        no = SMCSignal(
            SignalDirection.NO_TRADE, symbol,
            0,0,0,0,0,0,0,0,'ranging','none', timestamp=ts
        )

        if len(htf_candles) < 50 or len(ltf_candles) < 30:
            no.warnings = ['Insufficient candle data']
            return no

        cp = htf_candles[-1].close

        swings    = self.classify_structure(
            self.detect_swings(htf_candles)
        )
        trend     = self.get_trend(swings)
        if trend == 'ranging':
            no.warnings = ['Market ranging — no clear trend']
            return no

        direction = 'bullish' if trend == 'uptrend' else 'bearish'

        bias     = self.get_bias(daily_candles, weekly_candles)
        bias_dir = bias['combined']
        warns    = []
        if bias_dir != 'neutral' and bias_dir != direction:
            warns.append(
                f'Bias conflict: structure {direction} but bias {bias_dir}'
            )

        location         = self.get_location(cp, htf_candles)
        correct_location = (
            (direction == 'bullish' and location == 'discount') or
            (direction == 'bearish' and location == 'premium')
        )

        idm_level = self.find_idm(htf_candles, direction)
        idm_swept = False
        if idm_level:
            idm_swept = self.check_sweep(htf_candles, idm_level, direction)

        obs       = self.find_obs(htf_candles, direction)
        fresh_obs = [ob for ob in obs if ob.status == 'fresh']

        fvgs       = self.find_fvgs(htf_candles, direction)
        fresh_fvgs = [f for f in fvgs if f.status == 'fresh']

        liq = self.find_liquidity(htf_candles)

        ltf_ok = self.ltf_choch(ltf_candles, direction)

        best_ob = None
        for ob in reversed(fresh_obs):
            in_zone = ob.zone_low <= cp <= ob.zone_high
            near    = abs(cp - ob.midpoint) / max(ob.midpoint, 1e-9) < 0.02
            if in_zone or near:
                best_ob = ob
                break
        if not best_ob and fresh_obs:
            best_ob = fresh_obs[-1]

        if not best_ob:
            no.warnings = ['No valid Order Block found']
            return no

        best_fvg = None
        for fvg in reversed(fresh_fvgs):
            if (fvg.zone_low  <= best_ob.zone_high and
                    fvg.zone_high >= best_ob.zone_low):
                best_fvg = fvg
                break

        score   = 0
        reasons = []

        if trend != 'ranging':
            score += 1
            reasons.append(f'HTF {trend}')

        if correct_location:
            score += 1
            reasons.append(f'Correct location: {location}')
        else:
            warns.append(f'Location: {location} (not ideal)')

        if idm_swept:
            score += 1
            reasons.append(f'IDM swept @ {idm_level:.5f}')

        if best_fvg:
            score += 1
            reasons.append(
                f'FVG present {best_fvg.zone_low:.5f}–{best_fvg.zone_high:.5f}'
            )
        else:
            warns.append('No FVG at OB zone')

        if ltf_ok:
            score += 2
            reasons.append(f'LTF CHOCH confirmed {direction}')
        else:
            warns.append('LTF CHOCH not yet confirmed')

        if bias_dir == direction:
            score += 1
            reasons.append(
                f'Bias aligned: W:{bias["weekly"]} D:{bias["daily"]}'
            )

        if direction == 'bullish' and cp < bias['daily_open']:
            score += 1
            reasons.append(
                f'Below daily open {bias["daily_open"]:.5f} — buy zone'
            )
        if direction == 'bearish' and cp > bias['daily_open']:
            score += 1
            reasons.append(
                f'Above daily open {bias["daily_open"]:.5f} — sell zone'
            )

        if score < Config.MIN_CONFLUENCE_SCORE:
            no.warnings = [
                f'Score {score}/{Config.MIN_CONFLUENCE_SCORE} — setup not ready',
                *warns
            ]
            return no

        if not ltf_ok:
            no.warnings = [
                'Waiting for LTF CHOCH confirmation', *warns
            ]
            return no

        if best_fvg:
            el = max(best_ob.zone_low,  best_fvg.zone_low)
            eh = min(best_ob.zone_high, best_fvg.zone_high)
            if el >= eh:
                el, eh = best_ob.zone_low, best_ob.zone_high
            block_type = 'OB + FVG'
        else:
            el, eh     = best_ob.zone_low, best_ob.zone_high
            block_type = 'Order Block'

        entry = (el + eh) / 2

        buf = entry * Config.SL_BUFFER_PCT
        sl  = el - buf if direction == 'bullish' else eh + buf

        risk = abs(entry - sl)
        if risk == 0:
            no.warnings = ['Invalid SL calculation']
            return no

        m = 1 if direction == 'bullish' else -1
        if direction == 'bullish':
            tgts = sorted([p for p in liq['buy_side']  if p > entry])
        else:
            tgts = sorted([p for p in liq['sell_side'] if p < entry],
                          reverse=True)

        t1 = tgts[0] if len(tgts) > 0 else entry + risk * 1.5 * m
        t2 = tgts[1] if len(tgts) > 1 else entry + risk * 2.5 * m
        t3 = tgts[2] if len(tgts) > 2 else entry + risk * 4.0 * m
        rr = round(abs(t2 - entry) / risk, 2)

        if rr < Config.MIN_RR_RATIO:
            no.warnings = [f'RR {rr} below minimum {Config.MIN_RR_RATIO}']
            return no

        sig_dir = (SignalDirection.LONG  if direction == 'bullish'
                   else SignalDirection.SHORT)

        return SMCSignal(
            direction        = sig_dir,
            symbol           = symbol,
            entry_low        = round(el, 6),
            entry_high       = round(eh, 6),
            stop_loss        = round(sl, 6),
            target_1         = round(t1, 6),
            target_2         = round(t2, 6),
            target_3         = round(t3, 6),
            rr_ratio         = rr,
            confluence_score = score,
            trend            = trend,
            block_type       = block_type,
            reasons          = reasons,
            warnings         = warns,
            timestamp        = ts
        )

# ═══════════════════════════════════════════════
# MESSAGE FORMATTER
# ═══════════════════════════════════════════════

def format_signal(sig: SMCSignal, symbol: str) -> str:

    if not sig.is_valid():
        warn_text = (
            '\n'.join(f'  ⚠️ {w}' for w in sig.warnings)
            or '  No setup found'
        )
        return (
            f'🔍 <b>Analysis: {symbol.upper()}</b>\n'
            f'━━━━━━━━━━━━━━━━━━━━━━\n'
            f'⏳ <b>No Trade Setup Yet</b>\n\n'
            f'<b>Reasons:</b>\n{warn_text}\n\n'
            f'💡 Try again later or check a different pair.\n'
            f'⏰ {sig.timestamp}'
        )

    em    = '🟢' if sig.direction == SignalDirection.LONG else '🔴'
    stars = ('⭐⭐⭐' if sig.confluence_score >= 7
             else '⭐⭐' if sig.confluence_score >= 5
             else '⭐')
    rsns  = '\n'.join(f'  ✅ {r}' for r in sig.reasons)
    warns = '\n'.join(f'  ⚠️ {w}' for w in sig.warnings)

    if sig.direction == SignalDirection.LONG:
        verify = (
            f'📋 <b>HOW TO VERIFY (TradingView)</b>\n'
            f'━━━━━━━━━━━━━━━━━━━━━━\n'
            f'<b>Step 1 → Open 1H Chart</b>\n'
            f'  🔍 Trend should be: UPTREND\n'
            f'     (higher highs + higher lows)\n'
            f'  🔍 Look for Bullish OB or FVG near:\n'
            f'     {sig.entry_low} – {sig.entry_high}\n'
            f'  🔍 Price should be in DISCOUNT zone\n'
            f'     (lower half of recent range)\n\n'
            f'<b>Step 2 → Open 1D Chart (Daily)</b>\n'
            f'  🔍 Daily candle should be BULLISH\n'
            f'     (green candle or closing higher)\n'
            f'  🔍 Price should be BELOW daily open\n'
            f'  🔍 Weekly candle should be BULLISH\n\n'
            f'<b>Step 3 → Open 5M Chart</b>\n'
            f'  🔍 Look for a recent LOW sweep\n'
            f'     (wick below swing low then close back up)\n'
            f'  🔍 CHOCH UP should be visible\n'
            f'     (price broke a short term high)\n'
            f'  🔍 Price near or inside entry zone:\n'
            f'     {sig.entry_low} – {sig.entry_high}\n\n'
            f'<b>Step 4 → Check Targets on Chart</b>\n'
            f'  🎯 T1 {sig.target_1} — swing high here?\n'
            f'  🎯 T2 {sig.target_2} — liquidity here?\n\n'
            f'<b>✅ All steps match → Take the trade</b>\n'
            f'<b>❌ Any step conflicts → Skip this trade</b>'
        )
    else:
        verify = (
            f'📋 <b>HOW TO VERIFY (TradingView)</b>\n'
            f'━━━━━━━━━━━━━━━━━━━━━━\n'
            f'<b>Step 1 → Open 1H Chart</b>\n'
            f'  🔍 Trend should be: DOWNTREND\n'
            f'     (lower highs + lower lows)\n'
            f'  🔍 Look for Bearish OB or FVG near:\n'
            f'     {sig.entry_low} – {sig.entry_high}\n'
            f'  🔍 Price should be in PREMIUM zone\n'
            f'     (upper half of recent range)\n\n'
            f'<b>Step 2 → Open 1D Chart (Daily)</b>\n'
            f'  🔍 Daily candle should be BEARISH\n'
            f'     (red candle or closing lower)\n'
            f'  🔍 Price should be ABOVE daily open\n'
            f'  🔍 Weekly candle should be BEARISH\n\n'
            f'<b>Step 3 → Open 5M Chart</b>\n'
            f'  🔍 Look for a recent HIGH sweep\n'
            f'     (wick above swing high then close back down)\n'
            f'  🔍 CHOCH DOWN should be visible\n'
            f'     (price broke a short term low)\n'
            f'  🔍 Price near or inside entry zone:\n'
            f'     {sig.entry_low} – {sig.entry_high}\n\n'
            f'<b>Step 4 → Check Targets on Chart</b>\n'
            f'  🎯 T1 {sig.target_1} — swing low here?\n'
            f'  🎯 T2 {sig.target_2} — liquidity here?\n\n'
            f'<b>✅ All steps match → Take the trade</b>\n'
            f'<b>❌ Any step conflicts → Skip this trade</b>'
        )

    return (
        f'{em} <b>{sig.direction.value} — {symbol.upper()}</b>\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'📦 <b>Setup:</b>  {sig.block_type}\n'
        f'📈 <b>Trend:</b>  {sig.trend.upper()}\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'📍 <b>Entry Zone:</b>\n'
        f'     {sig.entry_low} – {sig.entry_high}\n\n'
        f'🛑 <b>Stop Loss:</b>   {sig.stop_loss}\n\n'
        f'🎯 <b>Target 1:</b>    {sig.target_1}\n'
        f'   └ Close 50% here\n'
        f'🎯 <b>Target 2:</b>    {sig.target_2}\n'
        f'   └ Move SL to breakeven\n'
        f'🎯 <b>Target 3:</b>    {sig.target_3}\n'
        f'   └ Let runner go\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'📊 <b>RR Ratio:</b>    1:{sig.rr_ratio}\n'
        f'⭐ <b>Score:</b>       {sig.confluence_score}/9  {stars}\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'<b>Confluences:</b>\n{rsns}\n'
        + (f'\n<b>Warnings:</b>\n{warns}\n' if warns else '') +
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'{verify}\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'⏰ {sig.timestamp}'
    )

# ═══════════════════════════════════════════════
# TELEGRAM HANDLERS
# ═══════════════════════════════════════════════

fetcher = PublicDataFetcher()
engine  = SMCAnalysisEngine()

HELP_TEXT = """
🤖 <b>SMC Signal Bot</b>
━━━━━━━━━━━━━━━━━━━━━━

<b>How to use:</b>
Just send any symbol and I will analyse it.

<b>Crypto Examples:</b>
  BTCUSDT   ETHUSDT   SOLUSDT
  BNBUSDT   XRPUSDT   ADAUSDT

<b>Forex Examples:</b>
  EURUSD    GBPUSD    USDJPY
  AUDUSD    CHFJPY    GBPJPY
  USDCAD    NZDUSD    USDCHF

<b>Metals:</b>
  XAUUSD (Gold)   XAGUSD (Silver)

<b>Index Examples:</b>
  NIFTY50   BANKNIFTY
  SPX       NDX       DAX

<b>Commands:</b>
  /start — show this message
  /help  — show this message

<b>What you get:</b>
  ✅ Trend direction
  ✅ Order Block + FVG zone
  ✅ Entry zone (exact prices)
  ✅ Stop loss level
  ✅ 3 profit targets
  ✅ RR ratio
  ✅ Confluence score
  ✅ Bias check
  ✅ Step by step verification guide
"""


async def start_handler(update: Update,
                         context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT, parse_mode='HTML')


async def help_handler(update: Update,
                        context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT, parse_mode='HTML')


async def symbol_handler(update: Update,
                          context: ContextTypes.DEFAULT_TYPE):
    raw    = update.message.text.strip()
    symbol = fetcher.normalize(raw)

    start_time = time.time()

    loading = await update.message.reply_text(
        f'🔍 Analysing <b>{symbol}</b>...\n'
        f'⏳ Fetching 4 timeframes simultaneously...\n'
        f'📡 Please wait 5-10 seconds...',
        parse_mode='HTML'
    )

    try:
        # ─��� Parallel fetch — all 4 timeframes at once ──
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
            f_htf    = ex.submit(fetcher.fetch, symbol, Config.HTF, 200)
            f_ltf    = ex.submit(fetcher.fetch, symbol, Config.LTF, 100)
            f_daily  = ex.submit(fetcher.fetch, symbol, '1d', 30)
            f_weekly = ex.submit(fetcher.fetch, symbol, '1w', 20)
            htf_candles    = f_htf.result()
            ltf_candles    = f_ltf.result()
            daily_candles  = f_daily.result()
            weekly_candles = f_weekly.result()

        if not htf_candles:
            await loading.edit_text(
                f'❌ <b>Could not fetch data for: {symbol}</b>\n\n'
                f'Please check the symbol and try again.\n\n'
                f'<b>Crypto:</b>  BTCUSDT  ETHUSDT  SOLUSDT\n'
                f'<b>Forex:</b>   EURUSD   GBPUSD   CHFJPY\n'
                f'<b>Metals:</b>  XAUUSD   XAGUSD\n'
                f'<b>Index:</b>   NIFTY50  SPX  NDX',
                parse_mode='HTML'
            )
            return

        signal = engine.analyse(
            symbol, htf_candles, ltf_candles,
            daily_candles, weekly_candles
        )

        elapsed = round(time.time() - start_time, 1)
        msg     = format_signal(signal, symbol)

        # Add elapsed time to end of message
        msg += f'\n⚡ <i>Analysis completed in {elapsed}s</i>'

        await loading.edit_text(msg, parse_mode='HTML')

        log.info(
            f'Analysed {symbol}: '
            f'{signal.direction.value} | '
            f'Score:{signal.confluence_score} | '
            f'RR:{signal.rr_ratio} | '
            f'Time:{elapsed}s'
        )

    except Exception as e:
        log.error(f'Error analysing {symbol}: {e}')
        await loading.edit_text(
            f'❌ Error analysing {symbol}\n{str(e)[:100]}',
            parse_mode='HTML'
        )

# ═══════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════

def main():
    token = Config.TELEGRAM_BOT_TOKEN
    if not token:
        log.error('TELEGRAM_BOT_TOKEN not set in .env')
        return

    log.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    log.info('SMC Signal Bot Starting...')
    log.info(f'HTF              : {Config.HTF}')
    log.info(f'LTF              : {Config.LTF}')
    log.info(f'Min Score        : {Config.MIN_CONFLUENCE_SCORE}')
    log.info(f'Min RR           : {Config.MIN_RR_RATIO}')
    log.info(f'Displacement Mult: {Config.DISPLACEMENT_MULT}')
    log.info('Send any symbol to the bot on Telegram.')
    log.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')

    app = ApplicationBuilder().token(token).build()

    app.add_handler(CommandHandler('start', start_handler))
    app.add_handler(CommandHandler('help',  help_handler))
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND,
        symbol_handler
    ))

    log.info('Bot is running. Press Ctrl+C to stop.')
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()