"""
SMC On-Demand Signal Bot — v3 (Smart Auto Alerts)
"""

import os
import time
import asyncio
import logging
import requests
import concurrent.futures
from datetime import datetime, timezone, timedelta
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
#  ALL PAIRS TO SCAN
# ═══════════════════════════════════════════════════
ALL_SCAN_PAIRS = [
    # Crypto
    'BTCUSDT','ETHUSDT','SOLUSDT','BNBUSDT',
    'XRPUSDT','ADAUSDT','DOGEUSDT','DOTUSDT',
    'AVAXUSDT','LINKUSDT','LTCUSDT',
    # Forex Majors
    'EURUSD','GBPUSD','USDJPY','AUDUSD',
    'USDCAD','USDCHF','NZDUSD',
    # Forex Crosses
    'EURJPY','EURGBP','EURAUD','EURCAD','EURCHF',
    'GBPJPY','GBPAUD','GBPCAD','GBPCHF','GBPNZD',
    'AUDJPY','AUDCAD','AUDCHF','AUDNZD',
    'NZDJPY','NZDCAD','NZDCHF',
    'CADJPY','CADCHF','CHFJPY',
    # Metals
    'XAUUSD','XAGUSD',
    # Oil
    'USOIL','UKOIL',
]


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
    MIN_CONFLUENCE_SCORE = 5
    MIN_RR_RATIO         = 2.0
    SL_BUFFER_PCT        = 0.002
    DISPLACEMENT_MULT    = 1.0

    # Auto alert thresholds (stricter than manual)
    ALERT_MIN_SCORE      = 7
    ALERT_MIN_RR         = 2.5
    ALERT_COOLDOWN_HOURS = 4     # same pair max once per 4h
    SCAN_INTERVAL_MINS   = 30    # scan every 30 min

    SYMBOL_SETTINGS: Dict = {
        'XAUUSD': {
            'min_score'    : 6,
            'min_rr'       : 2.0,
            'sl_buffer_pct': 0.005,
            'session_only' : 'london',
        },
        'GBPUSD': {
            'min_score'    : 5,
            'min_rr'       : 2.0,
            'sl_buffer_pct': 0.002,
            'session_only' : 'any',
        },
        'USDCAD': {
            'min_score'    : 5,
            'min_rr'       : 2.0,
            'sl_buffer_pct': 0.002,
            'session_only' : 'any',
            'check_oil'    : True,
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
        return {**defaults, **cls.SYMBOL_SETTINGS.get(symbol, {})}


# ═══════════════════════════════════════════════════
#  AUTO SCANNER STATE
# ═══════════════════════════════════════════════════
class ScannerState:
    auto_alerts_on : bool = True
    last_alerted   : Dict[str, datetime] = {}   # symbol → last alert time
    scan_running   : bool = False

    @classmethod
    def can_alert(cls, symbol: str) -> bool:
        if symbol not in cls.last_alerted:
            return True
        elapsed = datetime.now(timezone.utc) - cls.last_alerted[symbol]
        return elapsed >= timedelta(hours=Config.ALERT_COOLDOWN_HOURS)

    @classmethod
    def mark_alerted(cls, symbol: str):
        cls.last_alerted[symbol] = datetime.now(timezone.utc)

    @classmethod
    def cooldown_remaining(cls, symbol: str) -> str:
        if symbol not in cls.last_alerted:
            return 'ready'
        elapsed = datetime.now(timezone.utc) - cls.last_alerted[symbol]
        remaining = timedelta(hours=Config.ALERT_COOLDOWN_HOURS) - elapsed
        if remaining.total_seconds() <= 0:
            return 'ready'
        mins = int(remaining.total_seconds() / 60)
        return f'{mins}m cooldown'


# ═══════════════════════════════════════════════════
#  SESSION & NEWS FILTER
# ════════════════════════��══════════════════════════
class SessionFilter:

    NEWS_TIMES_UTC = [
        (8,30),(9,30),(12,30),(13,30),
        (14,0),(14,30),(18,0),(18,30),
    ]
    NEWS_BUFFER_MINS = 30

    @staticmethod
    def current_session() -> str:
        hour = datetime.now(timezone.utc).hour
        if 7  <= hour < 10: return 'london_open'
        if 10 <= hour < 12: return 'london'
        if 12 <= hour < 16: return 'overlap'
        if 16 <= hour < 21: return 'newyork'
        return 'dead'

    @staticmethod
    def is_session_ok(session_only: str) -> bool:
        sess = SessionFilter.current_session()
        if session_only == 'london':
            return sess in ['london_open','london','overlap']
        return sess != 'dead'

    @staticmethod
    def is_news_time() -> bool:
        now  = datetime.now(timezone.utc)
        curr = now.hour * 60 + now.minute
        for (nh, nm) in SessionFilter.NEWS_TIMES_UTC:
            if abs(curr - (nh*60+nm)) <= SessionFilter.NEWS_BUFFER_MINS:
                return True
        return False

    @staticmethod
    def next_good_session() -> str:
        hour = datetime.now(timezone.utc).hour
        if hour < 7 : return 'London open at 07:00 UTC'
        if hour < 12: return 'Overlap starts at 12:00 UTC'
        if hour < 16: return 'NY session at 16:00 UTC'
        return 'London open tomorrow at 07:00 UTC'

    @classmethod
    def check(cls, symbol: str) -> Optional[str]:
        cfg = Config.for_symbol(symbol)
        if cls.is_news_time():
            return ('⚠️ HIGH IMPACT NEWS TIME\n'
                    '   Avoid ±30 min around news.')
        if not cls.is_session_ok(cfg['session_only']):
            sess = cls.current_session()
            if sess == 'dead':
                return (f'😴 DEAD SESSION (Asian hours)\n'
                        f'   {cls.next_good_session()}\n'
                        f'   Best: London/NY Overlap 12–16 UTC')
            if cfg['session_only'] == 'london' and sess == 'newyork':
                return (f'⚠️ {symbol} trades best in London session\n'
                        f'   Best time: 07:00–16:00 UTC')
        return None


# ═══════════════════════════════════════════════════
#  DATA CLASSES
# ═══════════════════════════════════════════════════
class SignalDirection(Enum):
    LONG     = 'LONG'
    SHORT    = 'SHORT'
    NO_TRADE = 'NO TRADE'

@dataclass
class Candle:
    time: str; open: float; high: float
    low : float; close: float; volume: float = 0.0

    def body_high(self) : return max(self.open, self.close)
    def body_low(self)  : return min(self.open, self.close)
    def is_bullish(self): return self.close > self.open
    def is_bearish(self): return self.close < self.open
    def body_size(self) : return abs(self.close - self.open)
    def range_size(self): return self.high - self.low

@dataclass
class SwingPoint:
    type: str; price: float; index: int; broken: bool = False

@dataclass
class OrderBlock:
    direction: str; zone_low: float; zone_high: float
    midpoint : float; index: int; status: str = 'fresh'

@dataclass
class FVG:
    direction: str; zone_low: float; zone_high: float
    midpoint : float; index: int; status: str = 'fresh'

@dataclass
class SMCSignal:
    direction       : SignalDirection
    symbol          : str
    entry_low       : float
    entry_high      : float
    stop_loss       : float
    target_1        : float
    target_2        : float
    target_3        : float
    rr_ratio        : float
    confluence_score: int
    trend           : str
    block_type      : str
    session         : str = ''
    reasons         : List[str] = field(default_factory=list)
    warnings        : List[str] = field(default_factory=list)
    timestamp       : str = ''

    def is_valid(self) -> bool:
        cfg = Config.for_symbol(self.symbol)
        return (self.direction != SignalDirection.NO_TRADE and
                self.rr_ratio  >= cfg['min_rr'] and
                self.confluence_score >= cfg['min_score'])

    def is_alert_worthy(self) -> bool:
        return (self.is_valid() and
                self.confluence_score >= Config.ALERT_MIN_SCORE and
                self.rr_ratio         >= Config.ALERT_MIN_RR)


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
        'USOIL':'CL=F','UKOIL':'BZ=F',
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

    def detect_type(self, symbol):
        s = symbol.upper().replace('/','').replace('-','')
        return 'crypto' if any(
            s.endswith(e) for e in ['USDT','BTC','ETH','BNB','BUSD']
        ) else 'forex'

    def normalize(self, symbol):
        return symbol.upper().replace('/','').replace('-','').replace(' ','')

    def fetch(self, symbol, tf, limit=None):
        limit = limit or Config.CANDLE_LIMIT
        s     = self.normalize(symbol)
        if self.detect_type(s) == 'crypto':
            return (self._binance(s,tf,limit) or
                    self._coingecko(s,tf,limit) or
                    self._yahoo(s,tf,limit))
        return self._yahoo(s,tf,limit)

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
            r = requests.get(
                f'https://api.coingecko.com/api/v3/coins/{coin_id}'
                f'/ohlc?vs_currency=usd&days='
                f'{self.COINGECKO_TF_DAYS.get(tf,"7")}',
                headers={'User-Agent':'Mozilla/5.0'}, timeout=15
            )
            if r.status_code != 200: return []
            data = r.json()
            return [Candle(
                time  = str(datetime.fromtimestamp(
                    row[0]/1000, tz=timezone.utc)),
                open=float(row[1]),high=float(row[2]),
                low =float(row[3]),close=float(row[4]),volume=0.0
            ) for row in data][-limit:] if data else []
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
                    r = requests.get(
                        f'https://{host}.finance.yahoo.com/v8/finance/chart/'
                        f'{yf_sym}?interval={interval}&range={range_}',
                        headers=headers, timeout=15
                    )
                    if r.status_code != 200: continue
                    res = r.json().get('chart',{}).get('result',[])
                    if not res: continue
                    res     = res[0]
                    ts_list = res.get('timestamp',[])
                    q       = res['indicators']['quote'][0]
                    vols    = q.get('volume') or [0]*len(ts_list)
                    candles = []
                    for i, ts in enumerate(ts_list):
                        try:
                            o,h,l,c = (q['open'][i],q['high'][i],
                                       q['low'][i], q['close'][i])
                            if None in (o,h,l,c): continue
                            candles.append(Candle(
                                time  = str(datetime.fromtimestamp(
                                    ts, tz=timezone.utc)),
                                open=float(o),high=float(h),
                                low =float(l),close=float(c),
                                volume=float(vols[i] or 0)
                            ))
                        except Exception:
                            continue
                    if candles:
                        return candles[-limit:]
                except Exception as e:
                    log.error(f'Yahoo {host} error: {e}')
            return []
        except Exception as e:
            log.error(f'Yahoo error {symbol}: {e}')
            return []

    def _to_yahoo_symbol(self, symbol):
        if symbol in self.YAHOO_SYMBOL_MAP:
            return self.YAHOO_SYMBOL_MAP[symbol]
        if symbol in self.FOREX_PAIRS:
            return symbol[:3] + symbol[3:] + '=X'
        return symbol

    def get_oil_bias(self):
        try:
            candles = self._yahoo('USOIL','1d',10)
            if len(candles) < 5: return 'neutral'
            return ('bullish' if candles[-1].close > candles[-5].close
                    else 'bearish')
        except Exception:
            return 'neutral'


# ═══════════════════════════════════════════════════
#  SMC ANALYSIS ENGINE
# ═══════════════════════════════════════════════════
class SMCAnalysisEngine:

    def detect_swings(self, candles, lb=None):
        lb  = lb or Config.SWING_LOOKBACK
        pts = []
        for i in range(lb, len(candles)-lb):
            c = candles[i]
            if all(c.high > candles[i-k].high and
                   c.high > candles[i+k].high
                   for k in range(1,lb+1)):
                pts.append(SwingPoint('swing_high',c.high,i))
            if all(c.low < candles[i-k].low and
                   c.low < candles[i+k].low
                   for k in range(1,lb+1)):
                pts.append(SwingPoint('swing_low',c.low,i))
        return sorted(pts, key=lambda x: x.index)

    def classify_structure(self, swings):
        result = []; ph = pl = None
        for s in swings:
            if s.type == 'swing_high':
                s.type = 'HH' if (ph is None or s.price>ph.price) else 'LH'
                ph = s
            else:
                s.type = 'HL' if (pl is None or s.price>pl.price) else 'LL'
                pl = s
            result.append(s)
        return result

    def get_trend(self, swings):
        if len(swings) < 4: return 'ranging'
        last = [s.type for s in swings[-6:]]
        if last.count('HH')+last.count('HL') >= 4: return 'uptrend'
        if last.count('LL')+last.count('LH') >= 4: return 'downtrend'
        return 'ranging'

    def trend_strength(self, swings):
        if len(swings) < 6: return 0
        last = [s.type for s in swings[-8:]]
        bull = last.count('HH')+last.count('HL')
        bear = last.count('LL')+last.count('LH')
        mx   = max(bull,bear)
        return 3 if mx>=6 else 2 if mx>=4 else 1 if mx>=2 else 0

    def get_atr(self, candles, period=14):
        if len(candles) < period+1: return 0.0
        trs = [max(candles[i].high-candles[i].low,
                   abs(candles[i].high-candles[i-1].close),
                   abs(candles[i].low -candles[i-1].close))
               for i in range(1,len(candles))]
        return sum(trs[-period:]) / period

    def is_high_volatility(self, candles, mult=2.0):
        atr = self.get_atr(candles)
        return atr > 0 and candles[-1].range_size() > atr*mult

    def find_obs(self, candles, direction, start=0):
        obs = []; ar = (sum(c.range_size() for c in candles)/
                        len(candles)) if candles else 0.0001
        thr = ar * Config.DISPLACEMENT_MULT
        for i in range(start, len(candles)-3):
            c = candles[i]
            if direction=='bullish' and not c.is_bearish(): continue
            if direction=='bearish' and not c.is_bullish(): continue
            disp = any(
                (candles[j].is_bullish() if direction=='bullish'
                 else candles[j].is_bearish()) and
                candles[j].body_size() >= thr
                for j in range(i+1,min(i+4,len(candles)))
            )
            if not disp: continue
            obs.append(OrderBlock(direction=direction,
                zone_low=c.low,zone_high=c.high,
                midpoint=(c.low+c.high)/2,index=i))
        for ob in obs:
            for c in candles[ob.index+3:]:
                if c.low<=ob.zone_high and c.high>=ob.zone_low:
                    ob.status='tapped'; break
        return obs

    def find_fvgs(self, candles, direction, start=0):
        fvgs = []
        for i in range(start, len(candles)-2):
            c1,c3 = candles[i],candles[i+2]
            if direction=='bullish' and c1.high<c3.low:
                fvgs.append(FVG('bullish',c1.high,c3.low,
                                (c1.high+c3.low)/2,i+2))
            elif direction=='bearish' and c1.low>c3.high:
                fvgs.append(FVG('bearish',c3.high,c1.low,
                                (c3.high+c1.low)/2,i+2))
        return fvgs

    def find_liquidity(self, candles):
        swings = self.detect_swings(candles)
        highs  = sorted([s.price for s in swings
                         if 'high' in s.type or s.type=='HH'],reverse=True)
        lows   = sorted([s.price for s in swings
                         if 'low'  in s.type or s.type=='LL'])
        return {'buy_side':highs[:3],'sell_side':lows[:3]}

    def find_idm(self, candles, direction):
        swings = self.detect_swings(candles, lb=3)
        if direction=='bullish':
            lows = [s for s in swings if 'low' in s.type]
            return lows[-2].price if len(lows)>=2 else None
        highs = [s for s in swings if 'high' in s.type]
        return highs[-2].price if len(highs)>=2 else None

    def check_sweep(self, candles, level, direction, lookback=10):
        for c in candles[-lookback:]:
            if direction=='bullish' and c.low<level and c.close>level:
                return True
            if direction=='bearish' and c.high>level and c.close<level:
                return True
        return False

    def ltf_choch(self, ltf_candles, direction):
        if len(ltf_candles)<20: return False
        cswings = self.classify_structure(
            self.detect_swings(ltf_candles[-50:], lb=2))
        trend   = self.get_trend(cswings)
        if direction=='bullish':
            return (trend=='uptrend' or
                    any(s.type in ['HL','HH'] for s in cswings[-6:]))
        return (trend=='downtrend' or
                any(s.type in ['LH','LL'] for s in cswings[-6:]))

    def get_bias(self, daily_candles, weekly_candles):
        def cb(c):
            if not c: return 'neutral'
            return ('bullish' if c.close>c.open else
                    'bearish' if c.close<c.open else 'neutral')
        wb = cb(weekly_candles[-1]) if weekly_candles        else 'neutral'
        db = cb(daily_candles[-1])  if daily_candles         else 'neutral'
        pb = cb(daily_candles[-2])  if len(daily_candles)>=2 else 'neutral'
        votes = [wb,db,pb]
        combined = ('bullish' if votes.count('bullish')>=2 else
                    'bearish' if votes.count('bearish')>=2 else 'neutral')
        return {'weekly':wb,'daily':db,'combined':combined,
                'daily_open': daily_candles[-1].open if daily_candles else 0}

    def get_location(self, price, candles):
        highs = [c.high for c in candles[-50:]]
        lows  = [c.low  for c in candles[-50:]]
        eq    = (max(highs)+min(lows))/2
        return ('premium' if price>eq else
                'discount' if price<eq else 'equilibrium')

    def mtf_trend_aligned(self, htf, daily, weekly, direction):
        def tr(candles):
            if len(candles)<10: return 'ranging'
            return self.get_trend(
                self.classify_structure(self.detect_swings(candles)))
        h,d,w = tr(htf),tr(daily),tr(weekly)
        if direction=='bullish':
            return h=='uptrend' and d=='uptrend' and w=='uptrend'
        return h=='downtrend' and d=='downtrend' and w=='downtrend'

    def analyse(self, symbol, htf, ltf, daily, weekly,
                oil_bias='neutral') -> 'SMCSignal':
        ts  = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        cfg = Config.for_symbol(symbol)
        sess= SessionFilter.current_session()
        no  = SMCSignal(SignalDirection.NO_TRADE, symbol,
                        0,0,0,0,0,0,0,0,'ranging','none',
                        session=sess, timestamp=ts)

        if len(htf)<50 or len(ltf)<30:
            no.warnings=['Insufficient candle data']; return no

        cp     = htf[-1].close
        swings = self.classify_structure(self.detect_swings(htf))
        trend  = self.get_trend(swings)
        if trend=='ranging':
            no.warnings=['Market ranging — no clear trend']; return no

        direction = 'bullish' if trend=='uptrend' else 'bearish'

        if self.is_high_volatility(ltf):
            no.warnings=['⚠️ High volatility spike — wait']; return no

        warns = []
        if cfg.get('check_oil') and oil_bias!='neutral':
            oil_imp = 'bearish' if oil_bias=='bullish' else 'bullish'
            if oil_imp!=direction:
                warns.append(f'Oil conflict: oil {oil_bias} → USDCAD {oil_imp}')

        bias     = self.get_bias(daily, weekly)
        bias_dir = bias['combined']
        if bias_dir!='neutral' and bias_dir!=direction:
            warns.append(f'Bias conflict: {direction} vs bias {bias_dir}')

        location  = self.get_location(cp, htf)
        loc_ok    = ((direction=='bullish' and location=='discount') or
                     (direction=='bearish' and location=='premium'))
        idm_level = self.find_idm(htf, direction)
        idm_swept = (self.check_sweep(htf,idm_level,direction)
                     if idm_level else False)

        obs      = self.find_obs(htf,direction)
        fresh_ob = [o for o in obs if o.status=='fresh']
        fvgs     = self.find_fvgs(htf,direction)
        fresh_fg = [f for f in fvgs if f.status=='fresh']
        liq      = self.find_liquidity(htf)
        ltf_ok   = self.ltf_choch(ltf, direction)
        t_str    = self.trend_strength(swings)
        mtf_ok   = self.mtf_trend_aligned(htf,daily,weekly,direction)

        best_ob  = None
        for ob in reversed(fresh_ob):
            if (ob.zone_low<=cp<=ob.zone_high or
                    abs(cp-ob.midpoint)/max(ob.midpoint,1e-9)<0.02):
                best_ob=ob; break
        if not best_ob and fresh_ob:
            best_ob=fresh_ob[-1]
        if not best_ob:
            no.warnings=['No valid Order Block found']; return no

        best_fvg=None
        for fvg in reversed(fresh_fg):
            if fvg.zone_low<=best_ob.zone_high and fvg.zone_high>=best_ob.zone_low:
                best_fvg=fvg; break

        score=0; reasons=[]
        score+=1; reasons.append(f'HTF {trend}')
        if t_str>=2:
            score+=1; reasons.append(f'Strong trend ({t_str}/3)')
        else:
            warns.append(f'Weak trend ({t_str}/3)')
        if mtf_ok:
            score+=2; reasons.append('All 3 TFs aligned ✅')
        else:
            warns.append('MTF not fully aligned')
        if loc_ok:
            score+=1; reasons.append(f'Location: {location}')
        else:
            warns.append(f'Location {location} not ideal')
        if idm_swept:
            score+=1; reasons.append(f'IDM swept @ {idm_level:.5f}')
        if best_fvg:
            score+=1; reasons.append(
                f'FVG {best_fvg.zone_low:.5f}–{best_fvg.zone_high:.5f}')
        else:
            warns.append('No FVG at OB zone')
        if ltf_ok:
            score+=2; reasons.append(f'LTF CHOCH {direction}')
        else:
            warns.append('LTF CHOCH not confirmed')
        if bias_dir==direction:
            score+=1; reasons.append(
                f'Bias aligned W:{bias["weekly"]} D:{bias["daily"]}')
        if direction=='bullish' and cp<bias['daily_open']:
            score+=1; reasons.append(f'Below daily open — buy zone')
        if direction=='bearish' and cp>bias['daily_open']:
            score+=1; reasons.append(f'Above daily open — sell zone')

        if score<cfg['min_score']:
            no.warnings=[f'Score {score}/{cfg["min_score"]} — not ready',
                         *warns]; return no
        if not ltf_ok:
            no.warnings=['Waiting LTF CHOCH',*warns]; return no
        if not mtf_ok:
            no.warnings=['MTF not aligned — skip',*warns]; return no

        if best_fvg:
            el=max(best_ob.zone_low,best_fvg.zone_low)
            eh=min(best_ob.zone_high,best_fvg.zone_high)
            if el>=eh: el,eh=best_ob.zone_low,best_ob.zone_high
            block_type='OB + FVG'
        else:
            el,eh=best_ob.zone_low,best_ob.zone_high
            block_type='Order Block'

        entry=((el+eh)/2)
        buf  = entry*cfg['sl_buffer_pct']
        sl   = el-buf if direction=='bullish' else eh+buf
        risk = abs(entry-sl)
        if risk==0:
            no.warnings=['Invalid SL']; return no

        m    = 1 if direction=='bullish' else -1
        tgts = (sorted([p for p in liq['buy_side']  if p>entry])
                if direction=='bullish' else
                sorted([p for p in liq['sell_side'] if p<entry],reverse=True))
        t1=tgts[0] if len(tgts)>0 else entry+risk*1.5*m
        t2=tgts[1] if len(tgts)>1 else entry+risk*2.5*m
        t3=tgts[2] if len(tgts)>2 else entry+risk*4.0*m
        rr=round(abs(t2-entry)/risk,2)

        if rr<cfg['min_rr']:
            no.warnings=[f'RR {rr} < min {cfg["min_rr"]}']; return no

        return SMCSignal(
            direction=(SignalDirection.LONG if direction=='bullish'
                       else SignalDirection.SHORT),
            symbol=symbol,
            entry_low=round(el,6),entry_high=round(eh,6),
            stop_loss=round(sl,6),
            target_1=round(t1,6),target_2=round(t2,6),target_3=round(t3,6),
            rr_ratio=rr,confluence_score=score,
            trend=trend,block_type=block_type,
            session=sess,reasons=reasons,warnings=warns,timestamp=ts
        )


# ═══════════════════════════════════════════════════
#  FORMAT HELPERS
# ═══════════════════════════════════════════════════
SESSION_EMOJI = {
    'london_open': '🟢 London Open',
    'london'     : '🟡 London',
    'overlap'    : '🟢 London/NY Overlap ⭐',
    'newyork'    : '🟡 New York',
    'dead'       : '🔴 Dead Session',
}

def format_signal(sig: SMCSignal, symbol: str,
                  is_alert: bool = False) -> str:
    sess  = SESSION_EMOJI.get(sig.session, sig.session)
    cfg   = Config.for_symbol(symbol)
    badge = '🚨 <b>AUTO ALERT</b>\n' if is_alert else ''

    if not sig.is_valid():
        warn_text = '\n'.join(f'  ⚠️ {w}' for w in sig.warnings) \
                    or '  No setup found'
        return (
            f'{badge}'
            f'🔍 <b>Analysis: {symbol.upper()}</b>\n'
            f'━━━━━━━━━━━━━━━━━━━━━━\n'
            f'🕐 {sess}\n'
            f'━━━━━━━━━━━━━━━━━━━━━━\n'
            f'⏳ <b>No Trade Setup Yet</b>\n\n'
            f'{warn_text}\n\n'
            f'⏰ {sig.timestamp}'
        )

    em    = '🟢' if sig.direction == SignalDirection.LONG else '🔴'
    stars = ('⭐⭐⭐' if sig.confluence_score>=8 else
             '⭐⭐'  if sig.confluence_score>=6 else '⭐')
    rsns  = '\n'.join(f'  ✅ {r}' for r in sig.reasons)
    warns = '\n'.join(f'  ⚠️ {w}' for w in sig.warnings)

    verify_dir = 'UPTREND' if sig.direction==SignalDirection.LONG else 'DOWNTREND'
    sweep_dir  = 'LOW sweep + CHOCH UP' if sig.direction==SignalDirection.LONG \
                 else 'HIGH sweep + CHOCH DOWN'
    loc_exp    = 'DISCOUNT' if sig.direction==SignalDirection.LONG else 'PREMIUM'

    return (
        f'{badge}'
        f'{em} <b>{sig.direction.value} — {symbol.upper()}</b>\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'🕐 <b>Session:</b>  {sess}\n'
        f'📦 <b>Setup:</b>    {sig.block_type}\n'
        f'📈 <b>Trend:</b>    {sig.trend.upper()}\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'📍 <b>Entry Zone:</b>\n'
        f'     {sig.entry_low} – {sig.entry_high}\n\n'
        f'🛑 <b>Stop Loss:</b>    {sig.stop_loss}\n\n'
        f'🎯 <b>Target 1:</b>     {sig.target_1}  (close 50%)\n'
        f'🎯 <b>Target 2:</b>     {sig.target_2}  (move SL BE)\n'
        f'🎯 <b>Target 3:</b>     {sig.target_3}  (let run)\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'📊 <b>RR:</b>   1:{sig.rr_ratio}  (min {cfg["min_rr"]})\n'
        f'⭐ <b>Score:</b> {sig.confluence_score}/10  {stars}  '
        f'(min {cfg["min_score"]})\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'<b>Confluences:</b>\n{rsns}\n'
        + (f'\n<b>Warnings:</b>\n{warns}\n' if warns else '') +
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'📋 <b>VERIFY ON TRADINGVIEW</b>\n'
        f'  1H: {verify_dir} structure\n'
        f'  1H: OB/FVG near {sig.entry_low}–{sig.entry_high}\n'
        f'  1H: Price in {loc_exp} zone\n'
        f'  Daily+Weekly: same direction\n'
        f'  5M: {sweep_dir}\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'⏰ {sig.timestamp}'
    )


# ═══════════════════════════════════════════════════
#  SCANNER WORKER
# ═══════════════════════════════════════════════════
fetcher = PublicDataFetcher()
engine  = SMCAnalysisEngine()

def scan_one_pair(symbol: str):
    """Scan a single pair. Returns SMCSignal or None."""
    try:
        cfg = Config.for_symbol(symbol)
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
            f_htf    = ex.submit(fetcher.fetch, symbol, Config.HTF, 200)
            f_ltf    = ex.submit(fetcher.fetch, symbol, Config.LTF, 100)
            f_daily  = ex.submit(fetcher.fetch, symbol, '1d', 30)
            f_weekly = ex.submit(fetcher.fetch, symbol, '1w', 20)
            f_oil    = (ex.submit(fetcher.get_oil_bias)
                        if cfg.get('check_oil') else None)
            htf = f_htf.result(); ltf    = f_ltf.result()
            daily = f_daily.result(); weekly = f_weekly.result()
            oil   = f_oil.result() if f_oil else 'neutral'

        if not htf: return None
        return engine.analyse(symbol, htf, ltf, daily, weekly, oil)
    except Exception as e:
        log.error(f'scan_one_pair {symbol}: {e}')
        return None


async def auto_scanner(app):
    """Background task — scans all pairs every 30 min."""
    await asyncio.sleep(60)   # wait 1 min after bot starts

    while True:
        if not ScannerState.auto_alerts_on:
            await asyncio.sleep(60)
            continue

        sess = SessionFilter.current_session()
        if sess == 'dead':
            log.info('Auto scan skipped — dead session')
            await asyncio.sleep(Config.SCAN_INTERVAL_MINS * 60)
            continue

        if SessionFilter.is_news_time():
            log.info('Auto scan skipped — news time')
            await asyncio.sleep(10 * 60)
            continue

        log.info(f'Auto scan started — {len(ALL_SCAN_PAIRS)} pairs')
        found = 0

        for symbol in ALL_SCAN_PAIRS:
            try:
                if not ScannerState.can_alert(symbol):
                    continue

                sig = scan_one_pair(symbol)
                if sig is None:
                    continue

                if sig.is_alert_worthy():
                    ScannerState.mark_alerted(symbol)
                    msg = format_signal(sig, symbol, is_alert=True)
                    await app.bot.send_message(
                        chat_id   = Config.TELEGRAM_CHAT_ID,
                        text      = msg,
                        parse_mode= 'HTML'
                    )
                    found += 1
                    log.info(f'ALERT sent: {symbol} | '
                             f'Score:{sig.confluence_score} | '
                             f'RR:{sig.rr_ratio}')
                    await asyncio.sleep(2)  # small delay between messages

            except Exception as e:
                log.error(f'Scanner error {symbol}: {e}')

            await asyncio.sleep(1)   # 1s between pairs to avoid rate limits

        log.info(f'Auto scan done — {found} alerts sent')
        await asyncio.sleep(Config.SCAN_INTERVAL_MINS * 60)


# ═══════════════════════════════════════════════════
#  COMMAND HANDLERS
# ═══════════════════════════════════════════════════
HELP_TEXT = """
🤖 <b>SMC Signal Bot v3</b>
━━━━━━━━━━━━━━━━━━━━━━

<b>Auto Alert Rules:</b>
  🚨 Alerts only when Score 7+ AND RR 2.5+
  ✅ Session: London / NY only
  ✅ No alerts during news time
  ✅ Same pair: max 1 alert per 4 hours
  ✅ Scans every 30 minutes silently

<b>Commands:</b>
  /on      — turn auto alerts ON
  /off     — turn auto alerts OFF
  /scan    — manual scan all pairs NOW
  /pairs   — show all pairs being scanned
  /status  — show bot status
  /start   — show this message
  /help    — show this message

<b>Manual Analysis:</b>
  Just send any symbol e.g. XAUUSD

<b>Best Sessions:</b>
  🟢 07:00–10:00 UTC  London Open
  🟢 12:00–16:00 UTC  London/NY Overlap
  🟡 16:00–21:00 UTC  New York
  🔴 21:00–07:00 UTC  Avoid

<b>Symbols:</b>
  Crypto: BTCUSDT ETHUSDT SOLUSDT
  Forex:  EURUSD GBPUSD USDJPY XAUUSD
  Full list: /pairs
"""

async def start_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT, parse_mode='HTML')

async def help_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT, parse_mode='HTML')

async def on_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ScannerState.auto_alerts_on = True
    await update.message.reply_text(
        '✅ <b>Auto alerts ON</b>\n\n'
        f'Scanning {len(ALL_SCAN_PAIRS)} pairs every '
        f'{Config.SCAN_INTERVAL_MINS} min\n'
        f'Alert threshold: Score {Config.ALERT_MIN_SCORE}+ '
        f'| RR {Config.ALERT_MIN_RR}+\n'
        f'Cooldown: {Config.ALERT_COOLDOWN_HOURS}h per pair',
        parse_mode='HTML'
    )

async def off_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ScannerState.auto_alerts_on = False
    await update.message.reply_text(
        '🔕 <b>Auto alerts OFF</b>\n\n'
        'You can still analyse manually by sending a symbol.\n'
        'Use /on to turn alerts back on.',
        parse_mode='HTML'
    )

async def status_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    sess  = SessionFilter.current_session()
    label = SESSION_EMOJI.get(sess, sess)
    news  = '⚠️ YES — avoid trading' if SessionFilter.is_news_time() else '✅ Clear'
    state = '✅ ON' if ScannerState.auto_alerts_on else '🔕 OFF'
    alerted = len(ScannerState.last_alerted)

    await update.message.reply_text(
        f'📊 <b>Bot Status</b>\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'🤖 Auto Alerts:   {state}\n'
        f'🕐 Session:       {label}\n'
        f'📰 News Time:     {news}\n'
        f'👁 Pairs:         {len(ALL_SCAN_PAIRS)}\n'
        f'⏱ Scan Interval: {Config.SCAN_INTERVAL_MINS} min\n'
        f'🎯 Alert Score:   {Config.ALERT_MIN_SCORE}+\n'
        f'📊 Alert RR:      {Config.ALERT_MIN_RR}+\n'
        f'⏳ Cooldown:      {Config.ALERT_COOLDOWN_HOURS}h\n'
        f'📬 Alerted today: {alerted} pairs\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'⏰ {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}',
        parse_mode='HTML'
    )

async def pairs_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    crypto = [p for p in ALL_SCAN_PAIRS if p.endswith('USDT')]
    forex  = [p for p in ALL_SCAN_PAIRS
              if not p.endswith('USDT') and
              p not in ['XAUUSD','XAGUSD','USOIL','UKOIL']]
    metals = ['XAUUSD','XAGUSD']
    oil    = ['USOIL','UKOIL']

    def fmt(lst): return '  ' + '  '.join(lst)

    await update.message.reply_text(
        f'👁 <b>Scanning {len(ALL_SCAN_PAIRS)} Pairs</b>\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n\n'
        f'<b>Crypto ({len(crypto)}):</b>\n{fmt(crypto)}\n\n'
        f'<b>Forex ({len(forex)}):</b>\n{fmt(forex)}\n\n'
        f'<b>Metals:</b>\n{fmt(metals)}\n\n'
        f'<b>Oil:</b>\n{fmt(oil)}\n\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'Alert: Score {Config.ALERT_MIN_SCORE}+ | '
        f'RR {Config.ALERT_MIN_RR}+ | '
        f'Cooldown {Config.ALERT_COOLDOWN_HOURS}h',
        parse_mode='HTML'
    )

async def scan_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    sess = SessionFilter.current_session()
    await update.message.reply_text(
        f'🔍 <b>Manual Scan Started</b>\n'
        f'Scanning {len(ALL_SCAN_PAIRS)} pairs...\n'
        f'Session: {SESSION_EMOJI.get(sess,sess)}\n\n'
        f'⏳ This takes 2-3 minutes...',
        parse_mode='HTML'
    )

    found   = []
    skipped = 0

    for symbol in ALL_SCAN_PAIRS:
        try:
            sig = scan_one_pair(symbol)
            if sig and sig.is_alert_worthy():
                found.append(sig)
        except Exception as e:
            log.error(f'Manual scan {symbol}: {e}')
            skipped += 1

    if not found:
        await update.message.reply_text(
            f'🔍 <b>Scan Complete — No Setups Found</b>\n\n'
            f'Scanned: {len(ALL_SCAN_PAIRS)} pairs\n'
            f'Skipped: {skipped} (data unavailable)\n'
            f'Threshold: Score {Config.ALERT_MIN_SCORE}+ | '
            f'RR {Config.ALERT_MIN_RR}+\n\n'
            f'💡 Try again during London/NY session\n'
            f'⏰ {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}',
            parse_mode='HTML'
        )
        return

    await update.message.reply_text(
        f'✅ <b>Scan Complete — {len(found)} Setup(s) Found!</b>',
        parse_mode='HTML'
    )
    for sig in found:
        msg = format_signal(sig, sig.symbol, is_alert=False)
        await update.message.reply_text(msg, parse_mode='HTML')
        await asyncio.sleep(1)

async def symbol_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    raw    = update.message.text.strip()
    symbol = fetcher.normalize(raw)

    start_time = time.time()
    loading    = await update.message.reply_text(
        f'🔍 Analysing <b>{symbol}</b>...\n⏳ Please wait...',
        parse_mode='HTML'
    )

    try:
        sess_warn = SessionFilter.check(symbol)
        if sess_warn:
            sess  = SessionFilter.current_session()
            label = SESSION_EMOJI.get(sess, sess)
            await loading.edit_text(
                f'🔍 <b>{symbol}</b>\n'
                f'━━━━━━━━━━━━━━━━━━━━━━\n'
                f'🕐 {label}\n\n{sess_warn}\n\n'
                f'⏰ {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}',
                parse_mode='HTML'
            )
            return

        cfg = Config.for_symbol(symbol)
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
            f_htf    = ex.submit(fetcher.fetch, symbol, Config.HTF, 200)
            f_ltf    = ex.submit(fetcher.fetch, symbol, Config.LTF, 100)
            f_daily  = ex.submit(fetcher.fetch, symbol, '1d', 30)
            f_weekly = ex.submit(fetcher.fetch, symbol, '1w', 20)
            f_oil    = (ex.submit(fetcher.get_oil_bias)
                        if cfg.get('check_oil') else None)
            htf    = f_htf.result(); ltf    = f_ltf.result()
            daily  = f_daily.result(); weekly = f_weekly.result()
            oil    = f_oil.result() if f_oil else 'neutral'

        if not htf:
            await loading.edit_text(
                f'❌ <b>Could not fetch: {symbol}</b>\n\n'
                f'Check symbol and try again.',
                parse_mode='HTML'
            )
            return

        sig     = engine.analyse(symbol, htf, ltf, daily, weekly, oil)
        elapsed = round(time.time()-start_time, 1)
        msg     = format_signal(sig, symbol)
        msg    += f'\n⚡ <i>Completed in {elapsed}s</i>'
        await loading.edit_text(msg, parse_mode='HTML')
        log.info(f'{symbol}: {sig.direction.value} | '
                 f'Score:{sig.confluence_score} | RR:{sig.rr_ratio}')

    except Exception as e:
        log.error(f'Error {symbol}: {e}')
        await loading.edit_text(
            f'❌ Error: {symbol}\n{str(e)[:200]}',
            parse_mode='HTML'
        )


# ═══════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════
def main():
    token = Config.TELEGRAM_BOT_TOKEN
    if not token:
        log.error('TELEGRAM_BOT_TOKEN not set'); return

    if not Config.TELEGRAM_CHAT_ID:
        log.warning('TELEGRAM_CHAT_ID not set — auto alerts disabled')

    log.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    log.info('SMC Signal Bot v3 Starting...')
    log.info(f'Pairs    : {len(ALL_SCAN_PAIRS)}')
    log.info(f'Interval : {Config.SCAN_INTERVAL_MINS} min')
    log.info(f'Alert    : Score {Config.ALERT_MIN_SCORE}+ | RR {Config.ALERT_MIN_RR}+')
    log.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')

    app = ApplicationBuilder().token(token).build()

    app.add_handler(CommandHandler('start' , start_handler))
    app.add_handler(CommandHandler('help'  , help_handler))
    app.add_handler(CommandHandler('on'    , on_handler))
    app.add_handler(CommandHandler('off'   , off_handler))
    app.add_handler(CommandHandler('status', status_handler))
    app.add_handler(CommandHandler('pairs' , pairs_handler))
    app.add_handler(CommandHandler('scan'  , scan_handler))
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, symbol_handler))

    # Start background scanner
    async def post_init(application):
        if Config.TELEGRAM_CHAT_ID:
            asyncio.create_task(auto_scanner(application))
            log.info('Auto scanner started')
        else:
            log.warning('No CHAT_ID — auto scanner disabled')

    app.post_init = post_init

    log.info('Bot is running.')
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
