"""
SMC On-Demand Signal Bot — v2 (Improved Accuracy)
"""

import os
import time
import logging
import requests
import concurrent.futures
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional, List, Dict
from enum import Enum
from dotenv import load_dotenv

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


# ═══════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════
class Config:
    TELEGRAM_BOT_TOKEN   = os.getenv('TELEGRAM_BOT_TOKEN', '')
    TELEGRAM_CHAT_ID     = os.getenv('TELEGRAM_CHAT_ID', '')
    HTF                  = '1h'
    LTF                  = '5m'
    CANDLE_LIMIT         = 100
    SWING_LOOKBACK       = 5
    MIN_CONFLUENCE_SCORE = 5      # raised from 3 → 5
    MIN_RR_RATIO         = 2.0    # raised from 1.2 → 2.0
    SL_BUFFER_PCT        = 0.002
    DISPLACEMENT_MULT    = 1.0

    # Per-symbol overrides
    SYMBOL_SETTINGS: Dict = {
        'XAUUSD': {
            'min_score'     : 6,
            'min_rr'        : 2.0,
            'sl_buffer_pct' : 0.005,   # wider SL for gold
            'session_only'  : 'london', # London only
        },
        'GBPUSD': {
            'min_score'     : 5,
            'min_rr'        : 2.0,
            'sl_buffer_pct' : 0.002,
            'session_only'  : 'any',
        },
        'USDCAD': {
            'min_score'     : 5,
            'min_rr'        : 2.0,
            'sl_buffer_pct' : 0.002,
            'session_only'  : 'any',
            'check_oil'     : True,
        },
    }

    @classmethod
    def for_symbol(cls, symbol: str) -> Dict:
        defaults = {
            'min_score'    : cls.MIN_CONFLUENCE_SCORE,
            'min_rr'       : cls.MIN_RR_RATIO,
            'sl_buffer_pct': cls.SL_BUFFER_PCT,
            'session_only' : 'any',
            'check_oil'    : False,
        }
        overrides = cls.SYMBOL_SETTINGS.get(symbol, {})
        return {**defaults, **overrides}


# ═══════════════════════════════════════════════════
#  SESSION & NEWS FILTER
# ═══════════════════════════════════════════════════
class SessionFilter:

    # High-impact news times UTC (hour, minute)
    NEWS_TIMES_UTC = [
        (8,  30),   # UK news
        (9,  30),   # EU news
        (12, 30),   # US CPI / NFP / Retail Sales
        (13, 30),   # US data
        (14, 0),    # Fed speeches / US data
        (14, 30),   # US data
        (18, 0),    # FOMC / Fed rate decision
        (18, 30),   # Fed press conference
    ]
    NEWS_BUFFER_MINS = 30   # avoid 30 min before + after news

    @staticmethod
    def current_session() -> str:
        hour = datetime.now(timezone.utc).hour
        if 7  <= hour < 10 : return 'london_open'
        if 10 <= hour < 12 : return 'london'
        if 12 <= hour < 16 : return 'overlap'     # best session
        if 16 <= hour < 21 : return 'newyork'
        return 'dead'    # Asian / off hours

    @staticmethod
    def is_session_ok(session_only: str) -> bool:
        sess = SessionFilter.current_session()
        if session_only == 'london':
            return sess in ['london_open', 'london', 'overlap']
        if session_only == 'any':
            return sess != 'dead'
        return True

    @staticmethod
    def is_news_time() -> bool:
        now  = datetime.now(timezone.utc)
        hour = now.hour
        mins = now.minute
        for (nh, nm) in SessionFilter.NEWS_TIMES_UTC:
            news_total = nh * 60 + nm
            curr_total = hour * 60 + mins
            if abs(curr_total - news_total) <= SessionFilter.NEWS_BUFFER_MINS:
                return True
        return False

    @staticmethod
    def next_good_session() -> str:
        hour = datetime.now(timezone.utc).hour
        if hour < 7 : return 'London open at 07:00 UTC'
        if hour < 12: return 'Overlap starts at 12:00 UTC'
        if hour < 16: return 'NY session starts at 16:00 UTC'
        return 'London open tomorrow at 07:00 UTC'

    @classmethod
    def check(cls, symbol: str) -> Optional[str]:
        """Returns warning string if should not trade, None if ok."""
        cfg = Config.for_symbol(symbol)

        if cls.is_news_time():
            return (f'⚠️ HIGH IMPACT NEWS TIME\n'
                    f'   Avoid trading ±30 min around news.\n'
                    f'   Try again after news settles.')

        if not cls.is_session_ok(cfg['session_only']):
            sess = cls.current_session()
            if sess == 'dead':
                return (f'😴 DEAD SESSION (Asian hours)\n'
                        f'   {cls.next_good_session()}\n'
                        f'   Best time: London/NY overlap 12-16 UTC')
            if cfg['session_only'] == 'london' and sess == 'newyork':
                return (f'⚠️ {symbol} trades best during London session\n'
                        f'   Current: NY session\n'
                        f'   Best time: 07:00–16:00 UTC for {symbol}')

        return None   # all good


# ═══════════════════════════════════════════════════
#  DATA CLASSES
# ═══════════════════════════════════════════════════
class SignalDirection(Enum):
    LONG     = 'LONG'
    SHORT    = 'SHORT'
    NO_TRADE = 'NO TRADE'

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
    session          : str = ''
    reasons          : List[str] = field(default_factory=list)
    warnings         : List[str] = field(default_factory=list)
    timestamp        : str = ''

    def is_valid(self) -> bool:
        cfg = Config.for_symbol(self.symbol)
        return (self.direction != SignalDirection.NO_TRADE and
                self.rr_ratio  >= cfg['min_rr'] and
                self.confluence_score >= cfg['min_score'])


# ═══════════════════════════════════════════════════
#  DATA FETCHER
# ═══════════════════════════════════════════════════
class PublicDataFetcher:

    BINANCE_TF_MAP = {
        '1m':'1m','3m':'3m','5m':'5m','15m':'15m',
        '30m':'30m','1h':'1h','2h':'2h','4h':'4h',
        '6h':'6h','1d':'1d','1w':'1w','1M':'1M'
    }

    YAHOO_INTERVAL_MAP = {
        '5m':'5m','15m':'15m','30m':'30m',
        '1h':'1h','4h':'1h','1d':'1d',
        '1w':'1wk','1M':'1mo'
    }

    YAHOO_RANGE_MAP = {
        '5m':'7d','15m':'60d','30m':'60d',
        '1h':'2y','4h':'2y','1d':'5y',
        '1w':'10y','1M':'10y'
    }

    FOREX_PAIRS = [
        'EURUSD','GBPUSD','USDJPY','AUDUSD','USDCAD',
        'USDCHF','NZDUSD','USDSGD','USDHKD','USDCNH',
        'USDZAR','USDMXN','USDINR','USDTHB','USDTRY',
        'USDNOK','USDSEK','USDDKK','USDPLN','USDHUF',
        'USDCZK','USDBRL','USDKRW','USDIDR','USDPHP',
        'EURJPY','EURGBP','EURAUD','EURCAD','EURCHF',
        'EURNZD','EURSGD','EURHKD','EURTRY','EURNOK',
        'EURSEK','EURDKK','EURPLN','EURHUF','EURCZK',
        'EURMXN','EURZAR',
        'GBPJPY','GBPAUD','GBPCAD','GBPCHF','GBPNZD',
        'GBPSGD','GBPHKD','GBPTRY','GBPNOK','GBPSEK',
        'GBPZAR','GBPMXN',
        'AUDJPY','AUDCAD','AUDCHF','AUDNZD','AUDSGD','AUDHKD',
        'NZDJPY','NZDCAD','NZDCHF','NZDSGD',
        'CADJPY','CADCHF','CADSGD',
        'CHFJPY','CHFSGD',
        'SGDJPY','HKDJPY','NOKJPY','SEKJPY','DKKJPY',
        'XAUUSD','XAGUSD','XPTUSD','XPDUSD',
        'USOIL','UKOIL',
    ]

    COINGECKO_MAP = {
        'BTCUSDT':'bitcoin','ETHUSDT':'ethereum',
        'SOLUSDT':'solana','BNBUSDT':'binancecoin',
        'XRPUSDT':'ripple','ADAUSDT':'cardano',
        'DOGEUSDT':'dogecoin','DOTUSDT':'polkadot',
        'AVAXUSDT':'avalanche-2','LINKUSDT':'chainlink',
        'LTCUSDT':'litecoin','ATOMUSDT':'cosmos',
        'UNIUSDT':'uniswap','ETCUSDT':'ethereum-classic',
    }

    COINGECKO_TF_DAYS = {
        '5m':'1','15m':'1','30m':'2',
        '1h':'7','4h':'30','1d':'365','1w':'365',
    }

    YAHOO_SYMBOL_MAP = {
        'XAUUSD':'GC=F','XAGUSD':'SI=F',
        'XPTUSD':'PL=F','XPDUSD':'PA=F',
        'USOIL' :'CL=F','UKOIL' :'BZ=F',
        'NIFTY50':'^NSEI','NIFTY':'^NSEI',
        'BANKNIFTY':'^NSEBANK',
        'SPX':'^GSPC','NDX':'^NDX',
        'DJI':'^DJI','FTSE':'^FTSE','DAX':'^GDAXI',
        'BTCUSDT':'BTC-USD','ETHUSDT':'ETH-USD',
        'SOLUSDT':'SOL-USD','BNBUSDT':'BNB-USD',
        'XRPUSDT':'XRP-USD','ADAUSDT':'ADA-USD',
        'DOGEUSDT':'DOGE-USD','DOTUSDT':'DOT-USD',
        'AVAXUSDT':'AVAX-USD','LINKUSDT':'LINK-USD',
        'LTCUSDT':'LTC-USD',
    }

    def detect_type(self, symbol: str) -> str:
        crypto_endings = ['USDT','BTC','ETH','BNB','BUSD']
        s = symbol.upper().replace('/','').replace('-','')
        if any(s.endswith(e) for e in crypto_endings):
            return 'crypto'
        return 'forex'

    def normalize(self, symbol: str) -> str:
        return (symbol.upper()
                .replace('/','').replace('-','').replace(' ',''))

    def fetch(self, symbol: str, tf: str,
              limit: int = None) -> List[Candle]:
        limit = limit or Config.CANDLE_LIMIT
        s     = self.normalize(symbol)
        kind  = self.detect_type(s)
        if kind == 'crypto':
            candles = self._binance(s, tf, limit)
            if not candles:
                candles = self._coingecko(s, tf, limit)
            if not candles:
                candles = self._yahoo(s, tf, limit)
            return candles
        return self._yahoo(s, tf, limit)

    def _binance(self, symbol, tf, limit):
        try:
            r = requests.get(
                'https://api.binance.com/api/v3/klines',
                params={'symbol':symbol,
                        'interval':self.BINANCE_TF_MAP.get(tf,'1h'),
                        'limit':limit},
                timeout=10
            )
            if r.status_code != 200: return []
            return [Candle(
                time  = str(datetime.fromtimestamp(
                    row[0]/1000, tz=timezone.utc)),
                open  = float(row[1]), high  = float(row[2]),
                low   = float(row[3]), close = float(row[4]),
                volume= float(row[5])
            ) for row in r.json()]
        except Exception as e:
            log.error(f'Binance error: {e}')
            return []

    def _coingecko(self, symbol, tf, limit):
        try:
            coin_id = self.COINGECKO_MAP.get(symbol)
            if not coin_id: return []
            days = self.COINGECKO_TF_DAYS.get(tf,'7')
            r = requests.get(
                f'https://api.coingecko.com/api/v3/coins/{coin_id}'
                f'/ohlc?vs_currency=usd&days={days}',
                headers={'User-Agent':'Mozilla/5.0'}, timeout=15
            )
            if r.status_code != 200: return []
            data = r.json()
            if not data: return []
            return [Candle(
                time  = str(datetime.fromtimestamp(
                    row[0]/1000, tz=timezone.utc)),
                open  = float(row[1]), high  = float(row[2]),
                low   = float(row[3]), close = float(row[4]),
                volume= 0.0
            ) for row in data][-limit:]
        except Exception as e:
            log.error(f'CoinGecko error: {e}')
            return []

    def _yahoo(self, symbol, tf, limit):
        try:
            yf_sym   = self._to_yahoo_symbol(symbol)
            interval = self.YAHOO_INTERVAL_MAP.get(tf,'1d')
            range_   = self.YAHOO_RANGE_MAP.get(tf,'2y')
            headers  = {
                'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                               'AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36'),
                'Accept': 'application/json',
            }
            for host in ['query1','query2']:
                try:
                    url = (f'https://{host}.finance.yahoo.com/v8/finance/chart/'
                           f'{yf_sym}?interval={interval}&range={range_}')
                    r   = requests.get(url, headers=headers, timeout=15)
                    if r.status_code != 200: continue
                    res = r.json().get('chart',{}).get('result',[])
                    if not res: continue
                    res     = res[0]
                    ts_list = res.get('timestamp',[])
                    q       = res['indicators']['quote'][0]
                    candles = []
                    for i, ts in enumerate(ts_list):
                        try:
                            o,h,l,c = (q['open'][i], q['high'][i],
                                       q['low'][i],  q['close'][i])
                            if None in (o,h,l,c): continue
                            candles.append(Candle(
                                time  = str(datetime.fromtimestamp(
                                    ts, tz=timezone.utc)),
                                open  = float(o), high  = float(h),
                                low   = float(l), close = float(c),
                                volume= float((q.get('volume') or
                                               [0]*len(ts_list))[i] or 0)
                            ))
                        except Exception:
                            continue
                    if candles:
                        return candles[-limit:]
                except Exception as e:
                    log.error(f'Yahoo {host} error: {e}')
                    continue
            return []
        except Exception as e:
            log.error(f'Yahoo fetch error {symbol}: {e}')
            return []

    def _to_yahoo_symbol(self, symbol):
        if symbol in self.YAHOO_SYMBOL_MAP:
            return self.YAHOO_SYMBOL_MAP[symbol]
        if symbol in self.FOREX_PAIRS:
            return symbol[:3] + symbol[3:] + '=X'
        return symbol

    def get_oil_bias(self) -> str:
        """Returns oil trend for USDCAD correlation check."""
        try:
            candles = self._yahoo('USOIL','1d', 10)
            if len(candles) < 5: return 'neutral'
            recent = candles[-5:]
            if recent[-1].close > recent[0].close:
                return 'bullish'   # oil up → USDCAD down
            return 'bearish'       # oil down → USDCAD up
        except Exception:
            return 'neutral'

    def current_price(self, symbol):
        candles = self.fetch(symbol,'1d', limit=1)
        return candles[-1].close if candles else 0.0


# ═══════════════════════════════════════════════════
#  SMC ANALYSIS ENGINE
# ═══════════════════════════════════════════════════
class SMCAnalysisEngine:

    def detect_swings(self, candles, lb=None):
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

    def classify_structure(self, swings):
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

    def get_trend(self, swings):
        if len(swings) < 4: return 'ranging'
        last = [s.type for s in swings[-6:]]
        if last.count('HH') + last.count('HL') >= 4: return 'uptrend'
        if last.count('LL') + last.count('LH') >= 4: return 'downtrend'
        return 'ranging'

    def trend_strength(self, swings) -> int:
        """0-3 score for trend strength across last 3 swing pairs."""
        if len(swings) < 6: return 0
        last = [s.type for s in swings[-8:]]
        bull = last.count('HH') + last.count('HL')
        bear = last.count('LL') + last.count('LH')
        if bull >= 6 or bear >= 6: return 3
        if bull >= 4 or bear >= 4: return 2
        if bull >= 2 or bear >= 2: return 1
        return 0

    def get_atr(self, candles, period=14) -> float:
        if len(candles) < period + 1: return 0.0
        trs = []
        for i in range(1, len(candles)):
            c, p = candles[i], candles[i-1]
            tr   = max(c.high - c.low,
                       abs(c.high - p.close),
                       abs(c.low  - p.close))
            trs.append(tr)
        return sum(trs[-period:]) / period

    def is_high_volatility(self, candles, mult=2.0) -> bool:
        """True if last candle range >> ATR (news spike)."""
        atr  = self.get_atr(candles)
        if atr == 0: return False
        last = candles[-1].range_size()
        return last > atr * mult

    def find_obs(self, candles, direction, start=0):
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
                direction=direction,
                zone_low=(c.low), zone_high=(c.high),
                midpoint=(c.low+c.high)/2, index=i
            ))
        for ob in obs:
            for c in candles[ob.index+3:]:
                if c.low <= ob.zone_high and c.high >= ob.zone_low:
                    ob.status = 'tapped'
                    break
        return obs

    def find_fvgs(self, candles, direction, start=0):
        fvgs = []
        for i in range(start, len(candles) - 2):
            c1, c3 = candles[i], candles[i+2]
            if direction == 'bullish' and c1.high < c3.low:
                fvgs.append(FVG('bullish', c1.high, c3.low,
                                (c1.high+c3.low)/2, i+2))
            elif direction == 'bearish' and c1.low > c3.high:
                fvgs.append(FVG('bearish', c3.high, c1.low,
                                (c3.high+c1.low)/2, i+2))
        return fvgs

    def find_liquidity(self, candles):
        swings = self.detect_swings(candles)
        highs  = sorted([s.price for s in swings
                         if 'high' in s.type or s.type == 'HH'],
                        reverse=True)
        lows   = sorted([s.price for s in swings
                         if 'low' in s.type or s.type == 'LL'])
        return {'buy_side': highs[:3], 'sell_side': lows[:3]}

    def find_idm(self, candles, direction):
        swings = self.detect_swings(candles, lb=3)
        if direction == 'bullish':
            lows = [s for s in swings if 'low' in s.type]
            if len(lows) >= 2: return lows[-2].price
        else:
            highs = [s for s in swings if 'high' in s.type]
            if len(highs) >= 2: return highs[-2].price
        return None

    def check_sweep(self, candles, level, direction, lookback=10):
        for c in candles[-lookback:]:
            if direction == 'bullish':
                if c.low < level and c.close > level: return True
            else:
                if c.high > level and c.close < level: return True
        return False

    def ltf_choch(self, ltf_candles, direction):
        if len(ltf_candles) < 20: return False
        recent  = ltf_candles[-50:]
        swings  = self.detect_swings(recent, lb=2)
        cswings = self.classify_structure(swings)
        trend   = self.get_trend(cswings)
        if direction == 'bullish':
            return (trend == 'uptrend' or
                    any(s.type in ['HL','HH'] for s in cswings[-6:]))
        return (trend == 'downtrend' or
                any(s.type in ['LH','LL'] for s in cswings[-6:]))

    def get_bias(self, daily_candles, weekly_candles):
        def cb(c):
            if not c: return 'neutral'
            return ('bullish' if c.close > c.open
                    else 'bearish' if c.close < c.open else 'neutral')
        wb = cb(weekly_candles[-1]) if weekly_candles        else 'neutral'
        db = cb(daily_candles[-1])  if daily_candles         else 'neutral'
        pb = cb(daily_candles[-2])  if len(daily_candles)>=2 else 'neutral'
        votes = [wb, db, pb]
        bulls, bears = votes.count('bullish'), votes.count('bearish')
        combined   = ('bullish' if bulls >= 2 else
                      'bearish' if bears >= 2 else 'neutral')
        daily_open = daily_candles[-1].open if daily_candles else 0
        return {'weekly':wb,'daily':db,'combined':combined,
                'daily_open':daily_open}

    def get_location(self, price, candles):
        highs = [c.high for c in candles[-50:]]
        lows  = [c.low  for c in candles[-50:]]
        eq    = (max(highs) + min(lows)) / 2
        if price > eq: return 'premium'
        if price < eq: return 'discount'
        return 'equilibrium'

    def mtf_trend_aligned(self, htf_candles, daily_candles,
                           weekly_candles, direction) -> bool:
        """All 3 timeframes must agree on direction."""
        def trend_of(candles):
            if len(candles) < 10: return 'ranging'
            swings = self.classify_structure(
                self.detect_swings(candles))
            return self.get_trend(swings)
        htf_t = trend_of(htf_candles)
        d_t   = trend_of(daily_candles)
        w_t   = trend_of(weekly_candles)
        if direction == 'bullish':
            return (htf_t == 'uptrend' and
                    d_t   == 'uptrend' and
                    w_t   == 'uptrend')
        return (htf_t == 'downtrend' and
                d_t   == 'downtrend' and
                w_t   == 'downtrend')

    def analyse(self, symbol, htf_candles, ltf_candles,
                daily_candles, weekly_candles,
                oil_bias='neutral') -> SMCSignal:

        ts  = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        cfg = Config.for_symbol(symbol)
        sess= SessionFilter.current_session()

        no  = SMCSignal(
            SignalDirection.NO_TRADE, symbol,
            0,0,0,0,0,0,0,0,'ranging','none',
            session=sess, timestamp=ts
        )

        if len(htf_candles) < 50 or len(ltf_candles) < 30:
            no.warnings = ['Insufficient candle data']
            return no

        cp     = htf_candles[-1].close
        swings = self.classify_structure(self.detect_swings(htf_candles))
        trend  = self.get_trend(swings)

        if trend == 'ranging':
            no.warnings = ['Market ranging — no clear trend']
            return no

        direction = 'bullish' if trend == 'uptrend' else 'bearish'

        # ── High volatility check (spike candle) ──────────
        if self.is_high_volatility(ltf_candles):
            no.warnings = ['⚠️ High volatility spike detected — wait for candle to close']
            return no

        # ── USDCAD oil correlation ─────────────────────────
        warns = []
        if cfg.get('check_oil') and oil_bias != 'neutral':
            # Oil up = CAD strong = USDCAD bearish
            oil_implies = 'bearish' if oil_bias == 'bullish' else 'bullish'
            if oil_implies != direction:
                warns.append(
                    f'Oil correlation conflict: oil {oil_bias} '
                    f'implies USDCAD {oil_implies} but structure={direction}'
                )

        bias     = self.get_bias(daily_candles, weekly_candles)
        bias_dir = bias['combined']
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
        idm_swept = (self.check_sweep(htf_candles, idm_level, direction)
                     if idm_level else False)

        obs        = self.find_obs(htf_candles, direction)
        fresh_obs  = [ob for ob in obs if ob.status == 'fresh']
        fvgs       = self.find_fvgs(htf_candles, direction)
        fresh_fvgs = [f for f in fvgs  if f.status == 'fresh']
        liq        = self.find_liquidity(htf_candles)
        ltf_ok     = self.ltf_choch(ltf_candles, direction)
        t_strength = self.trend_strength(swings)
        mtf_ok     = self.mtf_trend_aligned(
            htf_candles, daily_candles, weekly_candles, direction)

        best_ob = None
        for ob in reversed(fresh_obs):
            if (ob.zone_low <= cp <= ob.zone_high or
                    abs(cp - ob.midpoint)/max(ob.midpoint,1e-9) < 0.02):
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

        # ── Confluence Scoring (max 10) ────────────────────
        score   = 0
        reasons = []

        # 1. HTF trend
        score += 1
        reasons.append(f'HTF {trend}')

        # 2. Trend strength
        if t_strength >= 2:
            score += 1
            reasons.append(f'Strong trend (strength {t_strength}/3)')
        else:
            warns.append(f'Weak trend strength ({t_strength}/3)')

        # 3. MTF alignment (all 3 TFs agree) — worth 2 points
        if mtf_ok:
            score += 2
            reasons.append('All 3 TFs aligned ✅')
        else:
            warns.append('Multi-timeframe not fully aligned')

        # 4. Correct location
        if correct_location:
            score += 1
            reasons.append(f'Correct location: {location}')
        else:
            warns.append(f'Location: {location} (not ideal)')

        # 5. IDM swept
        if idm_swept:
            score += 1
            reasons.append(f'IDM swept @ {idm_level:.5f}')

        # 6. FVG present
        if best_fvg:
            score += 1
            reasons.append(
                f'FVG {best_fvg.zone_low:.5f}–{best_fvg.zone_high:.5f}')
        else:
            warns.append('No FVG at OB zone')

        # 7. LTF CHOCH
        if ltf_ok:
            score += 2
            reasons.append(f'LTF CHOCH confirmed {direction}')
        else:
            warns.append('LTF CHOCH not yet confirmed')

        # 8. Bias aligned
        if bias_dir == direction:
            score += 1
            reasons.append(
                f'Bias aligned: W:{bias["weekly"]} D:{bias["daily"]}')

        if (direction == 'bullish' and cp < bias['daily_open']):
            score += 1
            reasons.append(
                f'Below daily open {bias["daily_open"]:.5f} — buy zone')
        if (direction == 'bearish' and cp > bias['daily_open']):
            score += 1
            reasons.append(
                f'Above daily open {bias["daily_open"]:.5f} — sell zone')

        min_score = cfg['min_score']
        if score < min_score:
            no.warnings = [
                f'Score {score}/{min_score} — setup not ready', *warns]
            return no

        if not ltf_ok:
            no.warnings = ['Waiting for LTF CHOCH confirmation', *warns]
            return no

        if not mtf_ok:
            no.warnings = ['Multi-TF not aligned — skip this trade', *warns]
            return no

        # ── Entry / SL / TP ───────────────────────────────
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
        buf   = entry * cfg['sl_buffer_pct']
        sl    = el - buf if direction == 'bullish' else eh + buf
        risk  = abs(entry - sl)
        if risk == 0:
            no.warnings = ['Invalid SL calculation']
            return no

        m = 1 if direction == 'bullish' else -1
        tgts = (sorted([p for p in liq['buy_side']  if p > entry])
                if direction == 'bullish' else
                sorted([p for p in liq['sell_side'] if p < entry],
                       reverse=True))

        t1 = tgts[0] if len(tgts) > 0 else entry + risk * 1.5 * m
        t2 = tgts[1] if len(tgts) > 1 else entry + risk * 2.5 * m
        t3 = tgts[2] if len(tgts) > 2 else entry + risk * 4.0 * m
        rr = round(abs(t2 - entry) / risk, 2)

        min_rr = cfg['min_rr']
        if rr < min_rr:
            no.warnings = [f'RR {rr} below minimum {min_rr}']
            return no

        sig_dir = (SignalDirection.LONG if direction == 'bullish'
                   else SignalDirection.SHORT)

        return SMCSignal(
            direction=sig_dir, symbol=symbol,
            entry_low=round(el,6), entry_high=round(eh,6),
            stop_loss=round(sl,6),
            target_1=round(t1,6), target_2=round(t2,6),
            target_3=round(t3,6),
            rr_ratio=rr, confluence_score=score,
            trend=trend, block_type=block_type,
            session=sess, reasons=reasons,
            warnings=warns, timestamp=ts
        )


# ═══════════════════════════════════════════════════
#  FORMAT SIGNAL
# ═════════════════════════════════════════���═════════
SESSION_EMOJI = {
    'london_open': '🟢 London Open',
    'london'     : '🟡 London',
    'overlap'    : '🟢 London/NY Overlap ⭐ Best',
    'newyork'    : '🟡 New York',
    'dead'       : '🔴 Dead Session',
}

def format_signal(sig: SMCSignal, symbol: str) -> str:
    sess_label = SESSION_EMOJI.get(sig.session, sig.session)
    cfg        = Config.for_symbol(symbol)

    if not sig.is_valid():
        warn_text = (
            '\n'.join(f'  ⚠️ {w}' for w in sig.warnings)
            or '  No setup found'
        )
        return (
            f'🔍 <b>Analysis: {symbol.upper()}</b>\n'
            f'━━━━━━━━━━━━━━━━━━━━━━\n'
            f'🕐 <b>Session:</b> {sess_label}\n'
            f'━━━━━━━━━━━━━━━━━━━━━━\n'
            f'⏳ <b>No Trade Setup Yet</b>\n\n'
            f'<b>Reasons:</b>\n{warn_text}\n\n'
            f'💡 Try again later or check a different pair.\n'
            f'⏰ {sig.timestamp}'
        )

    em    = '🟢' if sig.direction == SignalDirection.LONG else '🔴'
    stars = ('⭐⭐⭐' if sig.confluence_score >= 8
             else '⭐⭐' if sig.confluence_score >= 6
             else '⭐')
    rsns  = '\n'.join(f'  ✅ {r}' for r in sig.reasons)
    warns = '\n'.join(f'  ⚠️ {w}' for w in sig.warnings)

    if sig.direction == SignalDirection.LONG:
        verify = (
            f'📋 <b>HOW TO VERIFY (TradingView)</b>\n'
            f'━━━━━━━━━━━━━━━━━━━━━━\n'
            f'<b>Step 1 → Open 1H Chart</b>\n'
            f'  🔍 Trend: UPTREND (HH + HL structure)\n'
            f'  🔍 Bullish OB or FVG near:\n'
            f'     {sig.entry_low} – {sig.entry_high}\n'
            f'  🔍 Price in DISCOUNT zone\n\n'
            f'<b>Step 2 → Open Daily + Weekly</b>\n'
            f'  🔍 Both should be BULLISH\n'
            f'  🔍 Price BELOW daily open\n\n'
            f'<b>Step 3 → Open 5M Chart</b>\n'
            f'  🔍 Recent LOW sweep visible\n'
            f'  🔍 CHOCH UP confirmed\n'
            f'  🔍 Price tapping entry zone\n\n'
            f'<b>✅ All match → Take trade</b>\n'
            f'<b>❌ Any conflict → SKIP</b>'
        )
    else:
        verify = (
            f'📋 <b>HOW TO VERIFY (TradingView)</b>\n'
            f'━━━━━━━━━━━━━━━━━━━━━━\n'
            f'<b>Step 1 → Open 1H Chart</b>\n'
            f'  🔍 Trend: DOWNTREND (LH + LL structure)\n'
            f'  🔍 Bearish OB or FVG near:\n'
            f'     {sig.entry_low} – {sig.entry_high}\n'
            f'  🔍 Price in PREMIUM zone\n\n'
            f'<b>Step 2 → Open Daily + Weekly</b>\n'
            f'  🔍 Both should be BEARISH\n'
            f'  🔍 Price ABOVE daily open\n\n'
            f'<b>Step 3 → Open 5M Chart</b>\n'
            f'  🔍 Recent HIGH sweep visible\n'
            f'  🔍 CHOCH DOWN confirmed\n'
            f'  🔍 Price tapping entry zone\n\n'
            f'<b>✅ All match → Take trade</b>\n'
            f'<b>❌ Any conflict → SKIP</b>'
        )

    return (
        f'{em} <b>{sig.direction.value} — {symbol.upper()}</b>\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'🕐 <b>Session:</b>  {sess_label}\n'
        f'📦 <b>Setup:</b>   {sig.block_type}\n'
        f'📈 <b>Trend:</b>   {sig.trend.upper()}\n'
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
        f'📊 <b>RR Ratio:</b>    1:{sig.rr_ratio}  '
        f'(min {cfg["min_rr"]})\n'
        f'⭐ <b>Score:</b>       {sig.confluence_score}/10  '
        f'{stars}  (min {cfg["min_score"]})\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'<b>Confluences:</b>\n{rsns}\n'
        + (f'\n<b>Warnings:</b>\n{warns}\n' if warns else '') +
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'{verify}\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'⏰ {sig.timestamp}'
    )


# ═══��═══════════════════════════════════════════════
#  BOT HANDLERS
# ═══════════════════════════════════════════════════
fetcher = PublicDataFetcher()
engine  = SMCAnalysisEngine()

HELP_TEXT = """
🤖 <b>SMC Signal Bot v2</b>
━━━━━━━━━━━━━━━━━━━━━━

<b>Improvements:</b>
  ✅ Score 5+ required (was 3)
  ✅ RR 2.0+ required (was 1.2)
  ✅ Session filter added
  ✅ News time warning
  ✅ MTF alignment check
  ✅ XAUUSD special settings
  ✅ Oil correlation for USDCAD

<b>Best times to use:</b>
  🟢 07:00–10:00 UTC  London Open
  🟢 12:00–16:00 UTC  London/NY Overlap
  🟡 16:00–21:00 UTC  New York
  🔴 21:00–07:00 UTC  Avoid (Asian/dead)

<b>Crypto:</b>
  BTCUSDT   ETHUSDT   SOLUSDT
  BNBUSDT   XRPUSDT   ADAUSDT

<b>Forex Majors:</b>
  EURUSD    GBPUSD    USDJPY
  AUDUSD    USDCAD    USDCHF

<b>Forex Crosses:</b>
  GBPJPY    EURJPY    GBPAUD
  AUDJPY    CADJPY    CHFJPY

<b>Metals:</b>
  XAUUSD (Gold)      XAGUSD (Silver)

<b>Oil:</b>
  USOIL    UKOIL

<b>Indices:</b>
  NIFTY50  SPX  NDX  DAX

<b>Commands:</b>
  /start — show this message
  /help  — show this message
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
    loading    = await update.message.reply_text(
        f'🔍 Analysing <b>{symbol}</b>...\n'
        f'⏳ Please wait 5-15 seconds...',
        parse_mode='HTML'
    )

    try:
        # ── Session / news check first ─────────────────
        sess_warn = SessionFilter.check(symbol)
        if sess_warn:
            sess = SessionFilter.current_session()
            label = SESSION_EMOJI.get(sess, sess)
            await loading.edit_text(
                f'🔍 <b>{symbol}</b>\n'
                f'━━━━━━━━━━━━━━━━━━━━━━\n'
                f'🕐 <b>Session:</b> {label}\n\n'
                f'{sess_warn}\n\n'
                f'⏰ {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}',
                parse_mode='HTML'
            )
            return

        # ── Fetch data ───────────────────────────────��─
        cfg = Config.for_symbol(symbol)
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
            f_htf    = ex.submit(fetcher.fetch, symbol, Config.HTF, 200)
            f_ltf    = ex.submit(fetcher.fetch, symbol, Config.LTF, 100)
            f_daily  = ex.submit(fetcher.fetch, symbol, '1d',       30)
            f_weekly = ex.submit(fetcher.fetch, symbol, '1w',       20)
            f_oil    = (ex.submit(fetcher.get_oil_bias)
                        if cfg.get('check_oil') else None)

            htf_candles    = f_htf.result()
            ltf_candles    = f_ltf.result()
            daily_candles  = f_daily.result()
            weekly_candles = f_weekly.result()
            oil_bias       = f_oil.result() if f_oil else 'neutral'

        if not htf_candles:
            await loading.edit_text(
                f'❌ <b>Could not fetch data for: {symbol}</b>\n\n'
                f'Check symbol and try again.\n\n'
                f'<b>Examples:</b>\n'
                f'Crypto: BTCUSDT  ETHUSDT\n'
                f'Forex:  EURUSD   GBPUSD\n'
                f'Metals: XAUUSD   XAGUSD',
                parse_mode='HTML'
            )
            return

        signal  = engine.analyse(
            symbol, htf_candles, ltf_candles,
            daily_candles, weekly_candles, oil_bias
        )
        elapsed = round(time.time() - start_time, 1)
        msg     = format_signal(signal, symbol)
        msg    += f'\n⚡ <i>Completed in {elapsed}s</i>'

        await loading.edit_text(msg, parse_mode='HTML')
        log.info(f'{symbol}: {signal.direction.value} | '
                 f'Score:{signal.confluence_score} | '
                 f'RR:{signal.rr_ratio} | {elapsed}s')

    except Exception as e:
        log.error(f'Error analysing {symbol}: {e}')
        await loading.edit_text(
            f'❌ Error analysing {symbol}\n{str(e)[:200]}',
            parse_mode='HTML'
        )


def main():
    token = Config.TELEGRAM_BOT_TOKEN
    if not token:
        log.error('TELEGRAM_BOT_TOKEN not set')
        return

    log.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    log.info('SMC Signal Bot v2 Starting...')
    log.info(f'Min Score : {Config.MIN_CONFLUENCE_SCORE}')
    log.info(f'Min RR    : {Config.MIN_RR_RATIO}')
    log.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')

    app = ApplicationBuilder().token(token).build()
    app.add_handler(CommandHandler('start', start_handler))
    app.add_handler(CommandHandler('help',  help_handler))
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, symbol_handler))

    log.info('Bot is running.')
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
