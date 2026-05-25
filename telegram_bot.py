"""
SMC + ICT Signal Bot — v4.0
Full ICT Framework:
  Midnight Open | FPFVG | SD Zones | Macros
  8:30 & 9:30 | PO3 | OHLC | SMT | Liquidity
  Deep Premium/Discount | Kill Zones | Narrative Score
"""

import os
import time
import asyncio
import logging
import requests
import concurrent.futures
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple
from enum import Enum
from dotenv import load_dotenv

from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler,
    MessageHandler, filters, ContextTypes
)

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
log = logging.getLogger('ICTBot')

NY_TZ = ZoneInfo('America/New_York')


# ═══════════════════════════════════════════════════
#  PAIRS
# ═══════════════════════════════════════════════════
CRYPTO_PAIRS = [
    'BTCUSDT','ETHUSDT','SOLUSDT','BNBUSDT',
    'XRPUSDT','ADAUSDT','DOGEUSDT','DOTUSDT',
    'AVAXUSDT','LINKUSDT','LTCUSDT',
]
FOREX_PAIRS_SCAN = [
    'EURUSD','GBPUSD','USDJPY','AUDUSD','USDCAD','USDCHF','NZDUSD',
    'EURJPY','EURGBP','EURAUD','EURCAD','EURCHF',
    'GBPJPY','GBPAUD','GBPCAD','GBPCHF','GBPNZD',
    'AUDJPY','AUDCAD','AUDCHF','AUDNZD',
    'NZDJPY','NZDCAD','NZDCHF',
    'CADJPY','CADCHF','CHFJPY',
    'XAUUSD','XAGUSD','USOIL','UKOIL',
]
ALL_SCAN_PAIRS = CRYPTO_PAIRS + FOREX_PAIRS_SCAN

# SMT correlated pairs
SMT_PAIRS = {
    'XAUUSD' : 'XAGUSD',
    'XAGUSD' : 'XAUUSD',
    'EURUSD' : 'GBPUSD',
    'GBPUSD' : 'EURUSD',
    'AUDUSD' : 'NZDUSD',
    'NZDUSD' : 'AUDUSD',
    'BTCUSDT': 'ETHUSDT',
    'ETHUSDT': 'BTCUSDT',
    'USDCAD' : 'USDCHF',
    'USDCHF' : 'USDCAD',
}


# ═══════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════
class Config:
    TELEGRAM_BOT_TOKEN   = os.getenv('TELEGRAM_BOT_TOKEN','')
    TELEGRAM_CHAT_ID     = os.getenv('TELEGRAM_CHAT_ID','')
    HTF                  = '1h'
    LTF                  = '5m'
    CANDLE_LIMIT         = 150
    SWING_LOOKBACK       = 5
    MIN_CONFLUENCE_SCORE = 6    # out of 10
    MIN_RR_RATIO         = 1.8
    SL_BUFFER_PCT        = 0.002
    DISPLACEMENT_MULT    = 1.0
    ALERT_MIN_SCORE      = 8    # out of 10
    ALERT_MIN_RR         = 2.0
    ALERT_COOLDOWN_HOURS = 4
    SCAN_INTERVAL_MINS   = 30

    SYMBOL_SETTINGS: Dict = {
        'XAUUSD': {
            'min_score'    : 6,
            'min_rr'       : 1.8,
            'sl_buffer_pct': 0.005,
            'session_only' : 'london',
        },
        'GBPUSD': {
            'min_score'    : 6,
            'min_rr'       : 1.8,
            'sl_buffer_pct': 0.002,
        },
        'USDCAD': {
            'min_score'    : 6,
            'min_rr'       : 1.8,
            'sl_buffer_pct': 0.002,
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
        return {**defaults, **cls.SYMBOL_SETTINGS.get(symbol,{})}


# ═══════════════════════════════════════════════════
#  SCANNER STATE
# ═══════════════════════════════════════════════════
class ScannerState:
    auto_alerts_on : bool = True
    last_alerted   : Dict[str,datetime] = {}
    daily_pnl_pct  : float = 0.0
    daily_reset_date: str  = ''

    @classmethod
    def can_alert(cls, symbol:str) -> bool:
        if symbol not in cls.last_alerted: return True
        return (datetime.now(timezone.utc)-cls.last_alerted[symbol]
                >= timedelta(hours=Config.ALERT_COOLDOWN_HOURS))

    @classmethod
    def mark_alerted(cls, symbol:str):
        cls.last_alerted[symbol] = datetime.now(timezone.utc)

    @classmethod
    def check_daily_reset(cls):
        today = datetime.now(NY_TZ).strftime('%Y-%m-%d')
        if cls.daily_reset_date != today:
            cls.daily_pnl_pct  = 0.0
            cls.daily_reset_date = today

    @classmethod
    def daily_loss_ok(cls) -> bool:
        cls.check_daily_reset()
        return cls.daily_pnl_pct > -3.0   # stop at 3% (buffer before 4%)


# ═══════════════════════════════════════════════════
#  ICT TIME ENGINE
# ═══════════════════════════════════════════════════
class ICTTimeEngine:

    # Specific macro windows (NY time) hour:minute
    MACRO_WINDOWS = [
        (2,50),(3,10),   # London macro 1
        (3,50),(4,10),   # London macro 2
        (9,50),(10,10),  # NY macro 1
        (10,50),(11,10), # NY macro 2 (most important)
    ]

    # Kill zones (start_hour, end_hour) NY time
    KILL_ZONES = {
        'london_open' : (2,  5),
        'london'      : (5,  8),
        'pre_ny'      : (7,  8),
        'ny_open'     : (8, 11),
        'london_close': (10, 12),
    }

    NEWS_TIMES_UTC = [
        (8,30),(9,30),(12,30),(13,30),
        (14,0),(14,30),(18,0),(18,30),
    ]
    NEWS_BUFFER_MINS = 30

    @classmethod
    def now_ny(cls) -> datetime:
        return datetime.now(NY_TZ)

    @classmethod
    def utc_to_ny(cls, dt: datetime) -> datetime:
        return dt.astimezone(NY_TZ)

    @classmethod
    def is_macro_window(cls) -> Tuple[bool,str]:
        """Check if current time is inside any macro window."""
        now  = cls.now_ny()
        h,m  = now.hour, now.minute
        curr = h*60 + m

        # Every-hour macro: xx:50 to xx+1:10
        hour_start = h*60
        if curr >= hour_start+50 or curr <= hour_start+10:
            return True, f'{h:02d}:50–{(h+1)%24:02d}:10'

        # Named macro windows
        pairs = [(cls.MACRO_WINDOWS[i],cls.MACRO_WINDOWS[i+1])
                 for i in range(0,len(cls.MACRO_WINDOWS),2)]
        for (sh,sm),(eh,em) in pairs:
            start = sh*60+sm
            end   = eh*60+em
            if start <= curr <= end:
                return True, f'{sh:02d}:{sm:02d}–{eh:02d}:{em:02d}'
        return False, ''

    @classmethod
    def get_kill_zone(cls) -> str:
        h = cls.now_ny().hour
        for name,(start,end) in cls.KILL_ZONES.items():
            if start <= h < end:
                return name
        return 'dead'

    @classmethod
    def is_active_session(cls, is_crypto:bool=False) -> bool:
        if is_crypto: return True
        return cls.get_kill_zone() != 'dead'

    @classmethod
    def is_news_time(cls) -> bool:
        now  = datetime.now(timezone.utc)
        curr = now.hour*60+now.minute
        for (nh,nm) in cls.NEWS_TIMES_UTC:
            if abs(curr-(nh*60+nm)) <= cls.NEWS_BUFFER_MINS:
                return True
        return False

    @classmethod
    def get_midnight_open_time(cls) -> datetime:
        """Get today's midnight (12:00 AM NY)."""
        now = cls.now_ny()
        return now.replace(hour=0,minute=0,second=0,microsecond=0)

    @classmethod
    def get_830_time(cls) -> datetime:
        now = cls.now_ny()
        return now.replace(hour=8,minute=30,second=0,microsecond=0)

    @classmethod
    def get_930_time(cls) -> datetime:
        now = cls.now_ny()
        return now.replace(hour=9,minute=30,second=0,microsecond=0)

    @classmethod
    def is_830_passed(cls) -> bool:
        return cls.now_ny() >= cls.get_830_time()

    @classmethod
    def is_930_passed(cls) -> bool:
        return cls.now_ny() >= cls.get_930_time()

    @classmethod
    def session_label(cls) -> str:
        kz = cls.get_kill_zone()
        labels = {
            'london_open' : '🟢 London Open',
            'london'      : '🟡 London',
            'pre_ny'      : '🟠 Pre-NY',
            'ny_open'     : '🟢 NY Open ⭐',
            'london_close': '🟡 London Close',
            'dead'        : '🔴 Dead Session',
        }
        return labels.get(kz, kz)


# ═══════════════════════════════════════════════════
#  DATA CLASSES
# ═══════════════════════════════════════════════════
class SignalDirection(Enum):
    LONG     = 'LONG'
    SHORT    = 'SHORT'
    NO_TRADE = 'NO TRADE'

class PO3Phase(Enum):
    ACCUMULATION  = 'Accumulation'
    MANIPULATION  = 'Manipulation'
    DISTRIBUTION  = 'Distribution'
    UNKNOWN       = 'Unknown'

@dataclass
class Candle:
    time: str; open: float; high: float
    low : float; close: float; volume: float = 0.0
    def body_high(self) : return max(self.open,self.close)
    def body_low(self)  : return min(self.open,self.close)
    def is_bullish(self): return self.close>self.open
    def is_bearish(self): return self.close<self.open
    def body_size(self) : return abs(self.close-self.open)
    def range_size(self): return self.high-self.low

@dataclass
class SwingPoint:
    type:str; price:float; index:int; broken:bool=False

@dataclass
class OrderBlock:
    direction:str; zone_low:float; zone_high:float
    midpoint:float; index:int; status:str='fresh'

@dataclass
class FVG:
    direction:str; zone_low:float; zone_high:float
    midpoint:float; index:int; status:str='fresh'
    is_fpfvg:bool=False

@dataclass
class LiquidityPool:
    type   :str    # 'buy_side' or 'sell_side'
    price  :float
    touches:int
    swept  :bool=False

@dataclass
class ICTContext:
    """Full ICT narrative context for a symbol."""
    # Time
    kill_zone         :str   = 'dead'
    macro_active      :bool  = False
    macro_label       :str   = ''
    is_830_passed     :bool  = False
    is_930_passed     :bool  = False

    # Levels
    midnight_open     :float = 0.0
    open_830          :float = 0.0
    open_930          :float = 0.0

    # Deep Premium/Discount
    above_midnight    :bool  = False
    above_830         :bool  = False
    above_930         :bool  = False
    premium_discount  :str   = 'neutral'  # deep_premium/deep_discount/neutral

    # FPFVG
    fpfvg             :Optional[FVG] = None

    # SD Zones
    sd_15             :float = 0.0
    sd_25             :float = 0.0
    sd_45             :float = 0.0
    at_sd_zone        :str   = ''   # '1.5'/'2.5'/'4.5'/''

    # Liquidity
    buy_pools         :List[LiquidityPool] = field(default_factory=list)
    sell_pools        :List[LiquidityPool] = field(default_factory=list)
    liquidity_swept   :bool  = False
    swept_side        :str   = ''

    # PO3
    po3_phase         :PO3Phase = PO3Phase.UNKNOWN
    ohlc_model        :str      = ''  # 'bullish_olhc'/'bearish_ohlc'/''
    is_expansion_day  :bool     = False
    judas_swing       :bool     = False
    judas_direction   :str      = ''

    # 8:30 / 9:30
    manipulation_830  :bool  = False
    manipulation_830_dir:str = ''
    confirmed_930     :bool  = False

    # SMT
    smt_divergence    :bool  = False
    smt_direction     :str   = ''
    smt_pair          :str   = ''

    # Narrative
    narrative_score   :int   = 0
    reasons           :List[str] = field(default_factory=list)
    warnings          :List[str] = field(default_factory=list)

@dataclass
class SMCSignal:
    direction        :SignalDirection
    symbol           :str
    entry_low        :float
    entry_high       :float
    stop_loss        :float
    target_1         :float
    target_2         :float
    target_3         :float
    rr_ratio         :float
    confluence_score :int
    trend            :str
    block_type       :str
    ict              :ICTContext = field(default_factory=ICTContext)
    session          :str = ''
    reasons          :List[str] = field(default_factory=list)
    warnings         :List[str] = field(default_factory=list)
    timestamp        :str = ''

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
        '5m':'5m','15m':'15m','30m':'30m','1h':'1h',
        '4h':'1h','1d':'1d','1w':'1wk','1M':'1mo'
    }
    YAHOO_RANGE_MAP = {
        '5m':'7d','15m':'60d','30m':'60d','1h':'2y',
        '4h':'2y','1d':'5y','1w':'10y','1M':'10y'
    }
    ALL_FOREX = [
        'EURUSD','GBPUSD','USDJPY','AUDUSD','USDCAD','USDCHF','NZDUSD',
        'EURJPY','EURGBP','EURAUD','EURCAD','EURCHF','EURNZD',
        'GBPJPY','GBPAUD','GBPCAD','GBPCHF','GBPNZD',
        'AUDJPY','AUDCAD','AUDCHF','AUDNZD',
        'NZDJPY','NZDCAD','NZDCHF','CADJPY','CADCHF','CHFJPY',
        'XAUUSD','XAGUSD','XPTUSD','XPDUSD','USOIL','UKOIL',
    ]
    COINGECKO_MAP = {
        'BTCUSDT':'bitcoin','ETHUSDT':'ethereum','SOLUSDT':'solana',
        'BNBUSDT':'binancecoin','XRPUSDT':'ripple','ADAUSDT':'cardano',
        'DOGEUSDT':'dogecoin','DOTUSDT':'polkadot','AVAXUSDT':'avalanche-2',
        'LINKUSDT':'chainlink','LTCUSDT':'litecoin',
    }
    COINGECKO_TF_DAYS = {
        '5m':'1','15m':'1','30m':'2','1h':'7','4h':'30','1d':'365',
    }
    YAHOO_SYMBOL_MAP = {
        'XAUUSD':'GC=F','XAGUSD':'SI=F','XPTUSD':'PL=F','XPDUSD':'PA=F',
        'USOIL':'CL=F','UKOIL':'BZ=F',
        'BTCUSDT':'BTC-USD','ETHUSDT':'ETH-USD','SOLUSDT':'SOL-USD',
        'BNBUSDT':'BNB-USD','XRPUSDT':'XRP-USD','ADAUSDT':'ADA-USD',
        'DOGEUSDT':'DOGE-USD','DOTUSDT':'DOT-USD','AVAXUSDT':'AVAX-USD',
        'LINKUSDT':'LINK-USD','LTCUSDT':'LTC-USD',
    }

    def detect_type(self, symbol:str) -> str:
        s = symbol.upper().replace('/','').replace('-','')
        return ('crypto' if any(s.endswith(e)
                for e in ['USDT','BTC','ETH','BNB','BUSD']) else 'forex')

    def normalize(self, symbol:str) -> str:
        return symbol.upper().replace('/','').replace('-','').replace(' ','')

    def fetch(self, symbol:str, tf:str, limit:int=None) -> List[Candle]:
        limit = limit or Config.CANDLE_LIMIT
        s     = self.normalize(symbol)
        if self.detect_type(s)=='crypto':
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
                timeout=10)
            if r.status_code!=200: return []
            return [Candle(
                time=str(datetime.fromtimestamp(row[0]/1000,tz=timezone.utc)),
                open=float(row[1]),high=float(row[2]),
                low=float(row[3]),close=float(row[4]),
                volume=float(row[5])) for row in r.json()]
        except Exception as e:
            log.error(f'Binance: {e}'); return []

    def _coingecko(self, symbol, tf, limit):
        try:
            coin_id=self.COINGECKO_MAP.get(symbol)
            if not coin_id: return []
            r=requests.get(
                f'https://api.coingecko.com/api/v3/coins/{coin_id}'
                f'/ohlc?vs_currency=usd&days={self.COINGECKO_TF_DAYS.get(tf,"7")}',
                headers={'User-Agent':'Mozilla/5.0'},timeout=15)
            if r.status_code!=200: return []
            data=r.json()
            if not data: return []
            return [Candle(
                time=str(datetime.fromtimestamp(row[0]/1000,tz=timezone.utc)),
                open=float(row[1]),high=float(row[2]),
                low=float(row[3]),close=float(row[4]),volume=0.0)
                for row in data][-limit:]
        except Exception as e:
            log.error(f'CoinGecko: {e}'); return []

    def _yahoo(self, symbol, tf, limit):
        try:
            yf_sym  =self._to_yahoo(symbol)
            interval=self.YAHOO_INTERVAL_MAP.get(tf,'1d')
            range_  =self.YAHOO_RANGE_MAP.get(tf,'2y')
            headers ={'User-Agent':'Mozilla/5.0','Accept':'application/json'}
            for host in ['query1','query2']:
                try:
                    r=requests.get(
                        f'https://{host}.finance.yahoo.com/v8/finance/chart/'
                        f'{yf_sym}?interval={interval}&range={range_}',
                        headers=headers,timeout=15)
                    if r.status_code!=200: continue
                    res=r.json().get('chart',{}).get('result',[])
                    if not res: continue
                    res=res[0]
                    ts_list=res.get('timestamp',[])
                    q=res['indicators']['quote'][0]
                    vols=q.get('volume') or [0]*len(ts_list)
                    candles=[]
                    for i,ts in enumerate(ts_list):
                        try:
                            o,h,l,c=(q['open'][i],q['high'][i],
                                     q['low'][i],q['close'][i])
                            if None in (o,h,l,c): continue
                            candles.append(Candle(
                                time=str(datetime.fromtimestamp(ts,tz=timezone.utc)),
                                open=float(o),high=float(h),
                                low=float(l),close=float(c),
                                volume=float(vols[i] or 0)))
                        except Exception: continue
                    if candles: return candles[-limit:]
                except Exception as e:
                    log.error(f'Yahoo {host}: {e}')
            return []
        except Exception as e:
            log.error(f'Yahoo {symbol}: {e}'); return []

    def _to_yahoo(self, symbol):
        if symbol in self.YAHOO_SYMBOL_MAP:
            return self.YAHOO_SYMBOL_MAP[symbol]
        if symbol in self.ALL_FOREX:
            return symbol[:3]+symbol[3:]+'=X'
        return symbol

    def get_oil_bias(self) -> str:
        try:
            c=self._yahoo('USOIL','1d',10)
            if len(c)<5: return 'neutral'
            return 'bullish' if c[-1].close>c[-5].close else 'bearish'
        except Exception: return 'neutral'


# ═══════════════════════════════════════════════════
#  ICT ANALYSIS ENGINE
# ═══════════════════════════════════════════════════
class ICTAnalysisEngine:

    # ── Basic helpers ──────────────────────────────
    def get_atr(self, candles, period=14) -> float:
        if len(candles)<period+1: return 0.0
        trs=[max(candles[i].high-candles[i].low,
                 abs(candles[i].high-candles[i-1].close),
                 abs(candles[i].low-candles[i-1].close))
             for i in range(1,len(candles))]
        return sum(trs[-period:])/period

    def detect_swings(self, candles, lb=None):
        lb=lb or Config.SWING_LOOKBACK; pts=[]
        for i in range(lb,len(candles)-lb):
            c=candles[i]
            if all(c.high>candles[i-k].high and c.high>candles[i+k].high
                   for k in range(1,lb+1)):
                pts.append(SwingPoint('swing_high',c.high,i))
            if all(c.low<candles[i-k].low and c.low<candles[i+k].low
                   for k in range(1,lb+1)):
                pts.append(SwingPoint('swing_low',c.low,i))
        return sorted(pts,key=lambda x:x.index)

    def classify_structure(self, swings):
        result=[]; ph=pl=None
        for s in swings:
            if s.type=='swing_high':
                s.type='HH' if (ph is None or s.price>ph.price) else 'LH'
                ph=s
            else:
                s.type='HL' if (pl is None or s.price>pl.price) else 'LL'
                pl=s
            result.append(s)
        return result

    def get_trend(self, swings) -> str:
        if len(swings)<4: return 'ranging'
        last=[s.type for s in swings[-6:]]
        if last.count('HH')+last.count('HL')>=4: return 'uptrend'
        if last.count('LL')+last.count('LH')>=4: return 'downtrend'
        return 'ranging'

    def trend_strength(self, swings) -> int:
        if len(swings)<6: return 0
        last=[s.type for s in swings[-8:]]
        mx=max(last.count('HH')+last.count('HL'),
               last.count('LL')+last.count('LH'))
        return 3 if mx>=6 else 2 if mx>=4 else 1 if mx>=2 else 0

    # ── OB & FVG ───────────────────────────────────
    def find_obs(self, candles, direction) -> List[OrderBlock]:
        obs=[]; ar=(sum(c.range_size() for c in candles)/len(candles)) if candles else 0.0001
        thr=ar*Config.DISPLACEMENT_MULT
        for i in range(len(candles)-3):
            c=candles[i]
            if direction=='bullish' and not c.is_bearish(): continue
            if direction=='bearish' and not c.is_bullish(): continue
            disp=any((candles[j].is_bullish() if direction=='bullish'
                      else candles[j].is_bearish()) and
                     candles[j].body_size()>=thr
                     for j in range(i+1,min(i+4,len(candles))))
            if not disp: continue
            obs.append(OrderBlock(direction=direction,
                zone_low=c.low,zone_high=c.high,
                midpoint=(c.low+c.high)/2,index=i))
        for ob in obs:
            for c in candles[ob.index+3:]:
                if c.low<=ob.zone_high and c.high>=ob.zone_low:
                    ob.status='tapped'; break
        return obs

    def find_fvgs(self, candles, direction,
                  mark_fpfvg:bool=False) -> List[FVG]:
        fvgs=[]; found_first=False
        for i in range(len(candles)-2):
            c1,c3=candles[i],candles[i+2]
            if direction=='bullish' and c1.high<c3.low:
                fvg=FVG('bullish',c1.high,c3.low,
                        (c1.high+c3.low)/2,i+2)
                if mark_fpfvg and not found_first:
                    fvg.is_fpfvg=True; found_first=True
                fvgs.append(fvg)
            elif direction=='bearish' and c1.low>c3.high:
                fvg=FVG('bearish',c3.high,c1.low,
                        (c3.high+c1.low)/2,i+2)
                if mark_fpfvg and not found_first:
                    fvg.is_fpfvg=True; found_first=True
                fvgs.append(fvg)
        return fvgs

    # ── FPFVG ──────────────────────────────────────
    def find_fpfvg(self, candles_since_midnight,
                   direction) -> Optional[FVG]:
        """First valid FVG after midnight open."""
        fvgs=self.find_fvgs(candles_since_midnight,
                             direction,mark_fpfvg=True)
        fps=[f for f in fvgs if f.is_fpfvg]
        return fps[0] if fps else None

    # ── Standard Deviations ────────────────────────
    def calc_sd_zones(self, range_high:float, range_low:float,
                      direction:str) -> Tuple[float,float,float]:
        """Calculate 1.5, 2.5, 4.5 SD zones."""
        rng = range_high - range_low
        if rng == 0: return 0.0,0.0,0.0
        if direction=='bullish':
            return (range_high+rng*1.5,
                    range_high+rng*2.5,
                    range_high+rng*4.5)
        else:
            return (range_low-rng*1.5,
                    range_low-rng*2.5,
                    range_low-rng*4.5)

    def check_at_sd_zone(self, price:float,
                          sd15:float, sd25:float,
                          sd45:float, atr:float) -> str:
        """Check if price is near an SD zone."""
        buf = atr*0.5
        if abs(price-sd45)<=buf: return '4.5'
        if abs(price-sd25)<=buf: return '2.5'
        if abs(price-sd15)<=buf: return '1.5'
        return ''

    # ── Liquidity Pools ────────────────────────────
    def find_liquidity_pools(self,
                              candles) -> Tuple[List[LiquidityPool],
                                                List[LiquidityPool]]:
        buy_pools=[]; sell_pools=[]
        highs=[c.high for c in candles]
        lows =[c.low  for c in candles]
        atr  =self.get_atr(candles)
        buf  =atr*0.3

        # Equal highs = buy-side liquidity
        for i in range(len(highs)):
            count=sum(1 for h in highs if abs(h-highs[i])<=buf)
            if count>=3:
                exists=any(abs(p.price-highs[i])<=buf
                           for p in buy_pools)
                if not exists:
                    buy_pools.append(LiquidityPool(
                        'buy_side',highs[i],count))

        # Equal lows = sell-side liquidity
        for i in range(len(lows)):
            count=sum(1 for l in lows if abs(l-lows[i])<=buf)
            if count>=3:
                exists=any(abs(p.price-lows[i])<=buf
                           for p in sell_pools)
                if not exists:
                    sell_pools.append(LiquidityPool(
                        'sell_side',lows[i],count))

        # Check if swept by recent candles
        recent=candles[-10:]
        for p in buy_pools:
            if any(c.high>=p.price for c in recent):
                p.swept=True
        for p in sell_pools:
            if any(c.low<=p.price for c in recent):
                p.swept=True

        buy_pools.sort(key=lambda x:x.price,reverse=True)
        sell_pools.sort(key=lambda x:x.price)
        return buy_pools[:5],sell_pools[:5]

    # ── PO3 Detection ──────────────────────────────
    def detect_po3_phase(self, candles,
                          direction) -> PO3Phase:
        """Detect current PO3 phase from recent candles."""
        if len(candles)<20: return PO3Phase.UNKNOWN
        recent=candles[-20:]
        atr=self.get_atr(candles)

        # Accumulation: tight range low volatility
        ranges=[c.range_size() for c in recent[-10:]]
        avg_range=sum(ranges)/len(ranges)
        if avg_range < atr*0.7:
            return PO3Phase.ACCUMULATION

        # Check last 5 candles for manipulation
        # (large spike opposite to direction)
        last5=recent[-5:]
        if direction=='bullish':
            big_bear=any(c.is_bearish() and
                         c.body_size()>=atr*1.5
                         for c in last5)
            if big_bear: return PO3Phase.MANIPULATION
        else:
            big_bull=any(c.is_bullish() and
                         c.body_size()>=atr*1.5
                         for c in last5)
            if big_bull: return PO3Phase.MANIPULATION

        # Distribution: strong directional move
        if direction=='bullish':
            bullish_count=sum(1 for c in last5 if c.is_bullish())
            if bullish_count>=3:
                return PO3Phase.DISTRIBUTION
        else:
            bearish_count=sum(1 for c in last5 if c.is_bearish())
            if bearish_count>=3:
                return PO3Phase.DISTRIBUTION

        return PO3Phase.UNKNOWN

    # ── OHLC Model ─────────────────────────────────
    def detect_ohlc_model(self, daily_candles) -> str:
        """Detect bullish (O→L→H→C) or bearish (O→H→L→C) day."""
        if not daily_candles: return ''
        today=daily_candles[-1]
        # Rough heuristic using today's candle body
        if today.is_bullish():
            return 'bullish_olhc'   # Open→Low→High→Close
        elif today.is_bearish():
            return 'bearish_ohlc'   # Open→High→Low→Close
        return ''

    # ── Judas Swing ────────────────────────────────
    def detect_judas_swing(self, candles,
                            direction) -> Tuple[bool,str]:
        """Early fake move before real direction."""
        if len(candles)<10: return False,''
        recent=candles[-10:]
        atr=self.get_atr(candles)
        if direction=='bullish':
            # Judas = early bearish spike then reversal
            early_drop=any(c.is_bearish() and
                           c.body_size()>=atr*1.2
                           for c in recent[:5])
            later_bull=any(c.is_bullish() and
                           c.body_size()>=atr*1.2
                           for c in recent[5:])
            if early_drop and later_bull:
                return True,'bearish_fake'
        else:
            early_pump=any(c.is_bullish() and
                           c.body_size()>=atr*1.2
                           for c in recent[:5])
            later_bear=any(c.is_bearish() and
                           c.body_size()>=atr*1.2
                           for c in recent[5:])
            if early_pump and later_bear:
                return True,'bullish_fake'
        return False,''

    # ── 8:30 Manipulation ──────────────────────────
    def detect_830_manipulation(self, htf_candles,
                                  direction,
                                  open_830:float) -> Tuple[bool,str]:
        """Check if 8:30 manipulation occurred."""
        if open_830==0: return False,''
        atr=self.get_atr(htf_candles)
        recent=htf_candles[-6:]
        if direction=='bullish':
            # Bull day: expect drop below 8:30 open
            swept_below=any(c.low<open_830-atr*0.5
                            for c in recent)
            if swept_below: return True,'drop_below_830'
        else:
            # Bear day: expect spike above 8:30 open
            swept_above=any(c.high>open_830+atr*0.5
                            for c in recent)
            if swept_above: return True,'spike_above_830'
        return False,''

    # ── SMT Divergence ─────────────────────────────
    def detect_smt(self, candles_a, candles_b,
                   direction) -> Tuple[bool,str]:
        """Compare two correlated assets for divergence."""
        if len(candles_a)<10 or len(candles_b)<10:
            return False,''
        a_recent=candles_a[-10:]
        b_recent=candles_b[-10:]
        a_high=max(c.high  for c in a_recent)
        a_low =min(c.low   for c in a_recent)
        b_high=max(c.high  for c in b_recent)
        b_low =min(c.low   for c in b_recent)
        prev_a=candles_a[-20:-10]
        prev_b=candles_b[-20:-10]
        if not prev_a or not prev_b: return False,''
        pa_high=max(c.high for c in prev_a)
        pa_low =min(c.low  for c in prev_a)
        pb_high=max(c.high for c in prev_b)
        pb_low =min(c.low  for c in prev_b)

        if direction=='bearish':
            # A makes new high but B fails → A weakness
            a_new_high = a_high>pa_high
            b_fail_high= b_high<=pb_high
            if a_new_high and b_fail_high:
                return True,'bearish_divergence'
        else:
            # A makes new low but B fails → bullish divergence
            a_new_low = a_low<pa_low
            b_fail_low= b_low>=pb_low
            if a_new_low and b_fail_low:
                return True,'bullish_divergence'
        return False,''

    # ── LTF CHOCH ──────────────────────────────────
    def ltf_choch(self, ltf_candles, direction) -> bool:
        if len(ltf_candles)<20: return False
        cswings=self.classify_structure(
            self.detect_swings(ltf_candles[-50:],lb=2))
        trend=self.get_trend(cswings)
        if direction=='bullish':
            return (trend=='uptrend' or
                    any(s.type in ['HL','HH'] for s in cswings[-6:]))
        return (trend=='downtrend' or
                any(s.type in ['LH','LL'] for s in cswings[-6:]))

    # ── MTF Trend ──────────────────────────────────
    def mtf_trend(self, htf, daily, weekly, direction) -> Tuple[bool,int]:
        def tr(c):
            if len(c)<10: return 'ranging'
            return self.get_trend(
                self.classify_structure(self.detect_swings(c)))
        tfs=[tr(htf),tr(daily),tr(weekly)]
        exp='uptrend' if direction=='bullish' else 'downtrend'
        count=tfs.count(exp)
        return count==3, count

    # ── Get daily open prices ──────────────────────
    def get_key_opens(self, candles_1h) -> Tuple[float,float,float]:
        """Extract midnight, 8:30, 9:30 opens from 1h candles."""
        midnight_open=open_830=open_930=0.0
        for c in reversed(candles_1h):
            try:
                dt=datetime.fromisoformat(
                    str(c.time).replace('Z','+00:00'))
                dt_ny=ICTTimeEngine.utc_to_ny(dt)
                if dt_ny.hour==0 and midnight_open==0:
                    midnight_open=c.open
                if dt_ny.hour==8 and open_830==0:
                    open_830=c.open
                if dt_ny.hour==9 and open_930==0:
                    open_930=c.open
                if all([midnight_open,open_830,open_930]):
                    break
            except Exception:
                continue
        return midnight_open,open_830,open_930

    def get_candles_since_midnight(self,
                                    candles_ltf) -> List[Candle]:
        """Get candles from 12:00 AM NY today."""
        midnight=ICTTimeEngine.get_midnight_open_time()
        result=[]
        for c in candles_ltf:
            try:
                dt=datetime.fromisoformat(
                    str(c.time).replace('Z','+00:00'))
                dt_ny=ICTTimeEngine.utc_to_ny(dt)
                today_midnight=dt_ny.replace(
                    hour=0,minute=0,second=0,microsecond=0)
                if dt_ny >= today_midnight:
                    result.append(c)
            except Exception:
                continue
        return result

    # ══════════════════════════════════════════════
    #  MASTER ANALYSE
    # ══════════════════════════════════════════════
    def analyse(self, symbol, htf, ltf, daily, weekly,
                smt_candles=None, oil_bias='neutral') -> SMCSignal:

        ts  = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        cfg = Config.for_symbol(symbol)
        is_crypto = fetcher.detect_type(symbol)=='crypto'
        kz  = ICTTimeEngine.get_kill_zone()
        ict = ICTContext()
        no  = SMCSignal(SignalDirection.NO_TRADE,symbol,
                        0,0,0,0,0,0,0,0,'ranging','none',
                        ict=ict,session=kz,timestamp=ts)

        if len(htf)<50 or len(ltf)<30:
            no.warnings=['Insufficient candle data']; return no

        cp    = htf[-1].close
        atr   = self.get_atr(htf)
        swings= self.classify_structure(self.detect_swings(htf))
        trend = self.get_trend(swings)

        if trend=='ranging':
            no.warnings=['Market ranging — no clear trend']; return no

        direction='bullish' if trend=='uptrend' else 'bearish'

        # ── High volatility check ────────────────
        if atr>0 and htf[-1].range_size()>atr*2.5:
            no.warnings=['⚠️ Volatility spike — wait']; return no

        # ── OB + FVG ─────────────────────────────
        obs    =self.find_obs(htf,direction)
        fresh_ob=[o for o in obs if o.status=='fresh']
        if not fresh_ob:
            no.warnings=['No valid Order Block found']; return no

        all_fvgs=self.find_fvgs(htf,direction)
        fresh_fg=[f for f in all_fvgs if f.status=='fresh']

        best_ob=None
        for ob in reversed(fresh_ob):
            if (ob.zone_low<=cp<=ob.zone_high or
                    abs(cp-ob.midpoint)/max(ob.midpoint,1e-9)<0.03):
                best_ob=ob; break
        if not best_ob: best_ob=fresh_ob[-1]

        best_fvg=None
        for fvg in reversed(fresh_fg):
            if (fvg.zone_low<=best_ob.zone_high and
                    fvg.zone_high>=best_ob.zone_low):
                best_fvg=fvg; break

        # ── ICT Time Engine ──────────────────────
        macro_active,macro_label=ICTTimeEngine.is_macro_window()
        ict.kill_zone    =kz
        ict.macro_active =macro_active
        ict.macro_label  =macro_label
        ict.is_830_passed=ICTTimeEngine.is_830_passed()
        ict.is_930_passed=ICTTimeEngine.is_930_passed()

        # ── Key open levels ──────────────────────
        mo,o830,o930=self.get_key_opens(htf)
        ict.midnight_open=mo
        ict.open_830     =o830
        ict.open_930     =o930

        ict.above_midnight=(cp>mo) if mo else False
        ict.above_830     =(cp>o830) if o830 else False
        ict.above_930     =(cp>o930) if o930 else False

        above_count=sum([ict.above_midnight,
                         ict.above_830,ict.above_930])
        if above_count>=2:
            ict.premium_discount='deep_premium'
        elif above_count<=1:
            ict.premium_discount='deep_discount'
        else:
            ict.premium_discount='neutral'

        # ── FPFVG ────────────────────────────────
        since_midnight=self.get_candles_since_midnight(ltf)
        if len(since_midnight)>=3:
            ict.fpfvg=self.find_fpfvg(since_midnight,direction)

        # ── SD Zones ─────────────────────────────
        if mo and len(htf)>=5:
            mn_high=max(c.high for c in htf[-48:-40])
            mn_low =min(c.low  for c in htf[-48:-40])
            s15,s25,s45=self.calc_sd_zones(
                mn_high,mn_low,direction)
            ict.sd_15=s15; ict.sd_25=s25; ict.sd_45=s45
            ict.at_sd_zone=self.check_at_sd_zone(
                cp,s15,s25,s45,atr)

        # ── Liquidity Pools ──────────────────────
        ict.buy_pools,ict.sell_pools=\
            self.find_liquidity_pools(htf)

        if direction=='bullish':
            swept=[p for p in ict.sell_pools if p.swept]
            if swept:
                ict.liquidity_swept=True
                ict.swept_side='sell_side'
        else:
            swept=[p for p in ict.buy_pools if p.swept]
            if swept:
                ict.liquidity_swept=True
                ict.swept_side='buy_side'

        # ── PO3 Phase ────────────────────────────
        ict.po3_phase=self.detect_po3_phase(ltf,direction)

        # ── OHLC Model ───────────────────────────
        ict.ohlc_model=self.detect_ohlc_model(daily)

        # ── Expansion vs Manipulation Day ────────
        t_str=self.trend_strength(swings)
        ict.is_expansion_day=(t_str>=2 and
                               ict.po3_phase==PO3Phase.DISTRIBUTION)

        # ── Judas Swing ──────────────────────────
        ict.judas_swing,ict.judas_direction=\
            self.detect_judas_swing(ltf,direction)

        # ── 8:30 Manipulation ────────────────────
        if o830 and ict.is_830_passed:
            ict.manipulation_830,ict.manipulation_830_dir=\
                self.detect_830_manipulation(htf,direction,o830)

        # ── 9:30 Confirmation ────────────────────
        if ict.is_930_passed and ict.manipulation_830:
            last3=htf[-3:]
            if direction=='bullish':
                ict.confirmed_930=any(
                    c.is_bullish() and c.body_size()>=atr*0.8
                    for c in last3)
            else:
                ict.confirmed_930=any(
                    c.is_bearish() and c.body_size()>=atr*0.8
                    for c in last3)

        # ── SMT Divergence ───────────────────────
        if smt_candles and len(smt_candles)>=20:
            ict.smt_divergence,ict.smt_direction=\
                self.detect_smt(htf,smt_candles,direction)
            ict.smt_pair=SMT_PAIRS.get(symbol,'')

        # ── MTF alignment ────────────────────────
        mtf_full,mtf_count=self.mtf_trend(
            htf,daily,weekly,direction)

        # ── LTF CHOCH ────────────────────────────
        ltf_ok=self.ltf_choch(ltf,direction)

        # ── HTF bias ─────────────────────────────
        def cb(c):
            if not c: return 'neutral'
            return ('bullish' if c.close>c.open else
                    'bearish' if c.close<c.open else 'neutral')
        wb=cb(weekly[-1]) if weekly else 'neutral'
        db=cb(daily[-1])  if daily  else 'neutral'
        bias_dir=('bullish' if [wb,db].count('bullish')>=2 else
                  'bearish' if [wb,db].count('bearish')>=2 else 'neutral')

        # ── Oil check ────────────────────────────
        warns=[]
        if cfg.get('check_oil') and oil_bias!='neutral':
            oil_imp='bearish' if oil_bias=='bullish' else 'bullish'
            if oil_imp!=direction:
                warns.append(f'Oil conflict: {oil_bias} oil')

        # ════════════════════════════════════════
        #  SCORING  (16 raw → normalized to 10)
        # ════════════════════════════════════════
        score=0; reasons=[]

        # TIME (4 pts)
        if kz not in ['dead'] or is_crypto:
            score+=1; reasons.append(f'Kill zone: {kz}')
        if macro_active:
            score+=1; reasons.append(f'Macro window: {macro_label}')
        if ict.manipulation_830:
            score+=1; reasons.append(f'8:30 manipulation: '
                                      f'{ict.manipulation_830_dir}')
        if ict.confirmed_930:
            score+=1; reasons.append('9:30 direction confirmed')
        elif ict.is_930_passed and not ict.confirmed_930:
            warns.append('9:30 not yet confirmed')

        # PRICE (4 pts)
        nymo_ok=((direction=='bullish' and not ict.above_midnight) or
                 (direction=='bearish' and ict.above_midnight))
        if nymo_ok and mo:
            score+=1; reasons.append(
                f'Price {"below" if direction=="bullish" else "above"} '
                f'NYMO ({mo:.5f})')
        elif mo:
            warns.append('Price wrong side of NYMO')

        pd_ok=((direction=='bullish' and
                ict.premium_discount=='deep_discount') or
               (direction=='bearish' and
                ict.premium_discount=='deep_premium'))
        if pd_ok:
            score+=1; reasons.append(
                f'{ict.premium_discount.replace("_"," ").title()}')
        else:
            warns.append(f'Not in ideal premium/discount')

        if ict.fpfvg:
            score+=1; reasons.append(
                f'FPFVG @ {ict.fpfvg.zone_low:.5f}–'
                f'{ict.fpfvg.zone_high:.5f}')
        else:
            warns.append('No FPFVG identified today')

        if ict.at_sd_zone:
            score+=1; reasons.append(
                f'At {ict.at_sd_zone} SD zone')

        # NARRATIVE (4 pts)
        if ict.po3_phase==PO3Phase.DISTRIBUTION:
            score+=1; reasons.append('PO3: Distribution phase ✅')
        elif ict.po3_phase==PO3Phase.MANIPULATION:
            score+=1; reasons.append('PO3: Manipulation phase (entry soon)')
        else:
            warns.append(f'PO3: {ict.po3_phase.value}')

        ohlc_ok=((direction=='bullish' and
                  ict.ohlc_model=='bullish_olhc') or
                 (direction=='bearish' and
                  ict.ohlc_model=='bearish_ohlc'))
        if ohlc_ok:
            score+=1; reasons.append(
                f'OHLC model: {"O→L→H→C" if direction=="bullish" else "O→H→L→C"}')
        else:
            warns.append('OHLC model not ideal')

        if ict.liquidity_swept:
            score+=1; reasons.append(
                f'Liquidity swept: {ict.swept_side}')
        else:
            warns.append('Liquidity not yet swept')

        if ict.smt_divergence:
            score+=1; reasons.append(
                f'SMT divergence vs {ict.smt_pair}')
        else:
            warns.append(f'No SMT divergence '
                         f'({SMT_PAIRS.get(symbol,"N/A")})')

        # CONFIRMATION (4 pts)
        if bias_dir==direction:
            score+=1; reasons.append(
                f'HTF bias: W:{wb} D:{db}')
        else:
            warns.append(f'HTF bias mismatch: {bias_dir}')

        if best_fvg:
            score+=1; reasons.append(
                f'OB+FVG @ {best_ob.zone_low:.5f}–{best_ob.zone_high:.5f}')
        else:
            reasons.append(f'Order Block @ {best_ob.zone_low:.5f}–{best_ob.zone_high:.5f}')

        if mtf_full:
            score+=1; reasons.append('All 3 TFs aligned ✅')
        elif mtf_count>=2:
            score+=1; reasons.append(f'{mtf_count}/3 TFs aligned')
        else:
            warns.append('MTF not aligned')

        if ltf_ok:
            score+=1; reasons.append('LTF CHOCH confirmed')
        else:
            warns.append('LTF CHOCH not confirmed')

        # Judas swing bonus
        if ict.judas_swing:
            score+=1; reasons.append('Judas swing detected ✅')

        # Normalize 16 → 10
        raw_max   = 17
        normalized= round((score/raw_max)*10)
        normalized= min(10,normalized)

        # ── Apply thresholds ─────────────────────
        min_score=cfg['min_score']
        if normalized<min_score:
            no.warnings=[f'Score {normalized}/{min_score} — not ready',
                         *warns]
            no.ict=ict; return no

        if not ltf_ok:
            no.warnings=['Waiting LTF CHOCH',*warns]
            no.ict=ict; return no

        # ── Entry / SL / TP ──────────────────────
        if best_fvg:
            el=max(best_ob.zone_low,best_fvg.zone_low)
            eh=min(best_ob.zone_high,best_fvg.zone_high)
            if el>=eh: el,eh=best_ob.zone_low,best_ob.zone_high
            block_type='OB + FVG'
        else:
            el,eh=best_ob.zone_low,best_ob.zone_high
            block_type='Order Block'

        entry=(el+eh)/2
        buf  =entry*cfg['sl_buffer_pct']
        sl   =el-buf if direction=='bullish' else eh+buf
        risk =abs(entry-sl)
        if risk==0:
            no.warnings=['Invalid SL']; return no

        m=1 if direction=='bullish' else -1

        # Use SD zones for targets if available
        if ict.sd_15 and ict.sd_25 and ict.sd_45:
            if direction=='bullish':
                t1=ict.sd_15 if ict.sd_15>entry else entry+risk*1.5
                t2=ict.sd_25 if ict.sd_25>entry else entry+risk*2.5
                t3=ict.sd_45 if ict.sd_45>entry else entry+risk*3.5
            else:
                t1=ict.sd_15 if ict.sd_15<entry else entry-risk*1.5
                t2=ict.sd_25 if ict.sd_25<entry else entry-risk*2.5
                t3=ict.sd_45 if ict.sd_45<entry else entry-risk*3.5
        else:
            # Fallback to liquidity pools
            buy_p =[p.price for p in ict.buy_pools  if p.price>entry]
            sell_p=[p.price for p in ict.sell_pools if p.price<entry]
            tgts  =(sorted(buy_p) if direction=='bullish'
                    else sorted(sell_p,reverse=True))
            t1=tgts[0] if len(tgts)>0 else entry+risk*1.5*m
            t2=tgts[1] if len(tgts)>1 else entry+risk*2.5*m
            t3=tgts[2] if len(tgts)>2 else entry+risk*3.5*m

        rr=round(abs(t2-entry)/risk,2)
        if rr<cfg['min_rr']:
            no.warnings=[f'RR {rr} < min {cfg["min_rr"]}']; return no

        ict.narrative_score=normalized
        ict.reasons=reasons
        ict.warnings=warns

        return SMCSignal(
            direction=(SignalDirection.LONG if direction=='bullish'
                       else SignalDirection.SHORT),
            symbol=symbol,
            entry_low=round(el,6),entry_high=round(eh,6),
            stop_loss=round(sl,6),
            target_1=round(t1,6),target_2=round(t2,6),target_3=round(t3,6),
            rr_ratio=rr,confluence_score=normalized,
            trend=trend,block_type=block_type,
            ict=ict,session=kz,
            reasons=reasons,warnings=warns,timestamp=ts
        )


# ═══════════════════════════════════════════════════
#  POSITION SIZE CALCULATOR
# ═══════════════════════════════════════════════════
def calc_position_size(account_balance:float,
                        risk_pct:float,
                        entry:float,
                        sl:float,
                        symbol:str) -> Dict:
    risk_amount = account_balance*(risk_pct/100)
    sl_pips     = abs(entry-sl)
    if sl_pips==0: return {}
    is_crypto=fetcher.detect_type(symbol)=='crypto'
    if is_crypto:
        # For crypto: size in base currency
        lot_size = risk_amount/sl_pips
    else:
        # Forex/metals: approx lot size
        pip_value = 10  # per standard lot (approx)
        sl_pips_adjusted = sl_pips*10000 if sl_pips<1 else sl_pips
        lot_size  = risk_amount/(sl_pips_adjusted*pip_value)
        lot_size  = round(lot_size,2)
    return {
        'risk_amount' : round(risk_amount,2),
        'sl_distance' : round(sl_pips,6),
        'lot_size'    : round(lot_size,4),
        'risk_pct'    : risk_pct,
    }


# ═══════════════════════════════════════════════════
#  FORMAT SIGNAL
# ═══════════════════════════════════════════════════
PO3_EMOJI = {
    PO3Phase.ACCUMULATION : '⏸ Accumulation',
    PO3Phase.MANIPULATION : '🎭 Manipulation',
    PO3Phase.DISTRIBUTION : '🚀 Distribution',
    PO3Phase.UNKNOWN      : '❓ Unknown',
}

def format_signal(sig:SMCSignal, symbol:str,
                  is_alert:bool=False,
                  is_crypto:bool=False,
                  account_balance:float=0) -> str:

    cfg   = Config.for_symbol(symbol)
    badge = '🚨 <b>AUTO ALERT</b>\n' if is_alert else ''
    ict   = sig.ict
    kz    = ICTTimeEngine.session_label()
    mkt   = '🔵 Crypto (24/7)' if is_crypto else kz

    if not sig.is_valid():
        warns=('\n'.join(f'  ⚠️ {w}' for w in sig.warnings)
               or '  No setup found')
        return (
            f'{badge}'
            f'🔍 <b>{symbol.upper()}</b>\n'
            f'━━━━━━━━━━━━━━━━━━━━━━\n'
            f'🕐 {mkt}\n'
            f'━━━━━━━━━━━━━━━━━━━━━━\n'
            f'⏳ <b>No Trade Setup</b>\n\n'
            f'{warns}\n\n'
            f'⏰ {sig.timestamp}'
        )

    em    = '🟢' if sig.direction==SignalDirection.LONG else '🔴'
    stars = ('⭐⭐⭐' if sig.confluence_score>=8 else
             '⭐⭐'  if sig.confluence_score>=6 else '⭐')
    rsns  = '\n'.join(f'  ✅ {r}' for r in sig.reasons)
    warns = '\n'.join(f'  ⚠️ {w}' for w in sig.warnings)
    vdir  = 'UPTREND'   if sig.direction==SignalDirection.LONG else 'DOWNTREND'
    sw    = ('LOW sweep + CHOCH UP'
             if sig.direction==SignalDirection.LONG
             else 'HIGH sweep + CHOCH DOWN')
    loc   = 'DISCOUNT'  if sig.direction==SignalDirection.LONG else 'PREMIUM'

    # ICT context block
    macro_line=(f'⚡ <b>Macro:</b> {ict.macro_label} Active\n'
                if ict.macro_active else '')
    po3_line  = f'📖 <b>PO3:</b>   {PO3_EMOJI.get(ict.po3_phase,"")}\n'
    ohlc_map  = {'bullish_olhc':'O→L→H→C 📈',
                 'bearish_ohlc':'O→H→L→C 📉'}
    ohlc_line = (f'📊 <b>OHLC:</b>  {ohlc_map.get(ict.ohlc_model,"")}\n'
                 if ict.ohlc_model else '')

    # Premium/Discount block
    pd_emoji = ('🔴 Deep Premium' if ict.premium_discount=='deep_premium'
                else '🟢 Deep Discount' if ict.premium_discount=='deep_discount'
                else '⚪ Neutral')
    opens_block=''
    if ict.midnight_open:
        tick = lambda b: '✅' if b else '❌'
        opens_block=(
            f'━━━━━━━━━━━━━━━━━━━━━━\n'
            f'{pd_emoji}\n'
            f'  12AM: {ict.midnight_open:.5f} '
            f'{tick(ict.above_midnight)}\n'
        )
        if ict.open_830:
            opens_block+=(f'  8:30: {ict.open_830:.5f} '
                          f'{tick(ict.above_830)}\n')
        if ict.open_930:
            opens_block+=(f'  9:30: {ict.open_930:.5f} '
                          f'{tick(ict.above_930)}\n')

    # FPFVG line
    fpfvg_line=''
    if ict.fpfvg:
        fpfvg_line=(f'🎯 <b>FPFVG:</b> '
                    f'{ict.fpfvg.zone_low:.5f}–'
                    f'{ict.fpfvg.zone_high:.5f}\n')

    # SD zones line
    sd_line=''
    if ict.sd_15:
        sd_line=(f'📐 <b>SD Zones:</b> '
                 f'1.5={ict.sd_15:.5f} | '
                 f'2.5={ict.sd_25:.5f} | '
                 f'4.5={ict.sd_45:.5f}\n')

    # SMT line
    smt_line=''
    if ict.smt_divergence:
        smt_line=(f'🔀 <b>SMT:</b> {ict.smt_pair} diverged '
                  f'({ict.smt_direction})\n')

    # Position size
    pos_line=''
    if account_balance>0:
        ps=calc_position_size(account_balance,1.0,
                               sig.entry_high,sig.stop_loss,symbol)
        if ps:
            pos_line=(f'━━━━━━━━━━━━━━━━━━━━━━\n'
                      f'💰 <b>Position Size (1% risk):</b>\n'
                      f'  Risk: ${ps["risk_amount"]}\n'
                      f'  Lots: {ps["lot_size"]}\n')

    # Judas line
    judas_line=''
    if ict.judas_swing:
        judas_line='🎭 <b>Judas Swing:</b> Detected ✅\n'

    return (
        f'{badge}'
        f'{em} <b>{sig.direction.value} — {symbol.upper()}</b>\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'🕐 <b>Session:</b> {mkt}\n'
        f'{macro_line}'
        f'{po3_line}'
        f'{ohlc_line}'
        f'{judas_line}'
        f'📦 <b>Setup:</b>   {sig.block_type}\n'
        f'📈 <b>Trend:</b>   {sig.trend.upper()}\n'
        f'{opens_block}'
        f'{fpfvg_line}'
        f'{sd_line}'
        f'{smt_line}'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'📍 <b>Entry Zone:</b>\n'
        f'     {sig.entry_low} – {sig.entry_high}\n\n'
        f'🛑 <b>Stop Loss:</b>   {sig.stop_loss}\n\n'
        f'🎯 <b>T1:</b> {sig.target_1}  → close 50% + move SL to entry\n'
        f'🎯 <b>T2:</b> {sig.target_2}  → close 25% + move SL to T1\n'
        f'🎯 <b>T3:</b> {sig.target_3}  → close 25% let run\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'📊 <b>RR:</b>    1:{sig.rr_ratio}  '
        f'(min {cfg["min_rr"]})\n'
        f'⭐ <b>Score:</b>  {sig.confluence_score}/10  {stars}\n'
        f'{pos_line}'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'<b>Confluences:</b>\n{rsns}\n'
        + (f'\n<b>Warnings:</b>\n{warns}\n' if warns else '') +
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'📋 <b>VERIFY ON TRADINGVIEW</b>\n'
        f'  1H : {vdir} structure\n'
        f'  1H : OB/FVG @ {sig.entry_low}–{sig.entry_high}\n'
        f'  1H : Price in {loc} zone\n'
        f'  5M : {sw}\n'
        f'  5M : Rejection at FPFVG visible\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'⏰ {sig.timestamp}'
    )


# ═══════════════════════════════════════════════════
#  INSTANCES
# ═══════════════════════════════════════════════════
fetcher = PublicDataFetcher()
engine  = ICTAnalysisEngine()


def scan_one_pair(symbol:str) -> Optional[SMCSignal]:
    try:
        cfg       = Config.for_symbol(symbol)
        smt_symbol= SMT_PAIRS.get(symbol)
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
            f_htf  =ex.submit(fetcher.fetch,symbol,Config.HTF,200)
            f_ltf  =ex.submit(fetcher.fetch,symbol,Config.LTF,150)
            f_daily=ex.submit(fetcher.fetch,symbol,'1d',30)
            f_week =ex.submit(fetcher.fetch,symbol,'1w',20)
            f_smt  =(ex.submit(fetcher.fetch,smt_symbol,Config.HTF,100)
                     if smt_symbol else None)
            f_oil  =(ex.submit(fetcher.get_oil_bias)
                     if cfg.get('check_oil') else None)
            htf   =f_htf.result(); ltf    =f_ltf.result()
            daily =f_daily.result(); weekly=f_week.result()
            smt_c =f_smt.result() if f_smt else None
            oil   =f_oil.result() if f_oil else 'neutral'
        if not htf: return None
        return engine.analyse(symbol,htf,ltf,daily,weekly,
                               smt_c,oil)
    except Exception as e:
        log.error(f'scan_one_pair {symbol}: {e}'); return None


# ═══════════════════════════════════════════════════
#  AUTO SCANNER
# ═══════════════════════════════════════════════════
async def auto_scanner(app):
    await asyncio.sleep(60)
    while True:
        if not ScannerState.auto_alerts_on:
            await asyncio.sleep(60); continue
        if not ScannerState.daily_loss_ok():
            log.info('Daily loss limit reached — alerts paused')
            await asyncio.sleep(30*60); continue
        if ICTTimeEngine.is_news_time():
            log.info('News time — scan skipped')
            await asyncio.sleep(10*60); continue

        kz    = ICTTimeEngine.get_kill_zone()
        pairs = (CRYPTO_PAIRS if kz=='dead' else ALL_SCAN_PAIRS)
        log.info(f'Auto scan [{kz}] — {len(pairs)} pairs')
        found=0

        for symbol in pairs:
            try:
                if not ScannerState.can_alert(symbol): continue
                is_crypto=fetcher.detect_type(symbol)=='crypto'
                if not is_crypto and kz=='dead': continue

                sig=scan_one_pair(symbol)
                if sig and sig.is_alert_worthy():
                    ScannerState.mark_alerted(symbol)
                    msg=format_signal(sig,symbol,
                                      is_alert=True,
                                      is_crypto=is_crypto)
                    await app.bot.send_message(
                        chat_id=Config.TELEGRAM_CHAT_ID,
                        text=msg,parse_mode='HTML')
                    found+=1
                    log.info(f'ALERT: {symbol} '
                             f'Score:{sig.confluence_score} '
                             f'RR:{sig.rr_ratio}')
                    await asyncio.sleep(2)
            except Exception as e:
                log.error(f'Scanner {symbol}: {e}')
            await asyncio.sleep(1)

        log.info(f'Scan done — {found} alerts')
        await asyncio.sleep(Config.SCAN_INTERVAL_MINS*60)


# ═══════════════════════════════════════════════════
#  HANDLERS
# ═══════════════════════════════════════════════════
HELP_TEXT = """
🤖 <b>ICT Signal Bot v4.0</b>
━━━━━━━━━━━━━━━━━━━━━━

<b>ICT Concepts Active:</b>
  🕐 Midnight Open (NYMO)
  🎯 FPFVG Detection
  📐 SD Zones (1.5 / 2.5 / 4.5)
  ⚡ Macro Windows
  🔴 8:30 & 9:30 Manipulation
  📖 PO3 Phase Detection
  📊 OHLC Day Model
  🔀 SMT Divergence
  💧 Liquidity Pool Mapping
  🎭 Judas Swing Detection

<b>Commands:</b>
  /on       — auto alerts ON
  /off      — auto alerts OFF
  /scan     — manual scan NOW
  /pairs    — all pairs
  /status   — bot health
  /size     — position calculator
  /help     — this message

<b>Manual analysis:</b>
  Just send any symbol
  e.g.  XAUUSD  BTCUSDT  EURUSD

<b>Kill Zones (NY Time):</b>
  🟢 02:00–05:00  London Open
  🟢 08:30–11:00  NY Open ⭐
  🟡 10:00–12:00  London Close
  🔴 Other hours  Avoid forex
  🔵 Crypto       24/7 always

<b>Score System:</b>
  8–10 = Strong ⭐⭐⭐ (auto alert)
  6–7  = Valid  ⭐⭐
  0–5  = Skip   ❌
"""

async def start_handler(u,c):
    await u.message.reply_text(HELP_TEXT,parse_mode='HTML')

async def help_handler(u,c):
    await u.message.reply_text(HELP_TEXT,parse_mode='HTML')

async def on_handler(u,c):
    ScannerState.auto_alerts_on=True
    await u.message.reply_text(
        f'✅ <b>Auto Alerts ON</b>\n\n'
        f'🔵 Crypto : {len(CRYPTO_PAIRS)} pairs 24/7\n'
        f'📈 Forex  : {len(FOREX_PAIRS_SCAN)} pairs London/NY\n'
        f'Score   : {Config.ALERT_MIN_SCORE}+ | '
        f'RR {Config.ALERT_MIN_RR}+\n'
        f'Interval: every {Config.SCAN_INTERVAL_MINS}min',
        parse_mode='HTML')

async def off_handler(u,c):
    ScannerState.auto_alerts_on=False
    await u.message.reply_text(
        '🔕 <b>Auto Alerts OFF</b>\n\n'
        'Send any symbol for manual analysis.',
        parse_mode='HTML')

async def status_handler(u,c):
    kz   =ICTTimeEngine.get_kill_zone()
    label=ICTTimeEngine.session_label()
    macro_on,macro_lbl=ICTTimeEngine.is_macro_window()
    ny_now=ICTTimeEngine.now_ny().strftime('%H:%M')
    news ='⚠️ YES' if ICTTimeEngine.is_news_time() else '✅ Clear'
    state='✅ ON'  if ScannerState.auto_alerts_on  else '🔕 OFF'
    loss_ok='✅ OK' if ScannerState.daily_loss_ok() else '🛑 STOPPED'
    await u.message.reply_text(
        f'📊 <b>ICT Bot v4.0 Status</b>\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'🤖 Alerts      : {state}\n'
        f'💰 Daily P&L   : {loss_ok}\n'
        f'🕐 NY Time     : {ny_now}\n'
        f'📍 Kill Zone   : {label}\n'
        f'⚡ Macro Window: {"✅ "+macro_lbl if macro_on else "❌"}\n'
        f'📰 News Time   : {news}\n'
        f'8:30 passed    : {"✅" if ICTTimeEngine.is_830_passed() else "❌"}\n'
        f'9:30 passed    : {"✅" if ICTTimeEngine.is_930_passed() else "❌"}\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'🔵 Crypto      : {len(CRYPTO_PAIRS)} pairs\n'
        f'📈 Forex       : {len(FOREX_PAIRS_SCAN)} pairs\n'
        f'👁 Total       : {len(ALL_SCAN_PAIRS)}\n'
        f'⏱ Interval    : {Config.SCAN_INTERVAL_MINS}min\n'
        f'🎯 Alert Score : {Config.ALERT_MIN_SCORE}+\n'
        f'📊 Alert RR    : {Config.ALERT_MIN_RR}+\n'
        f'⏳ Cooldown    : {Config.ALERT_COOLDOWN_HOURS}h\n'
        f'📬 Alerted     : {len(ScannerState.last_alerted)}\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'⏰ {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}',
        parse_mode='HTML')

async def pairs_handler(u,c):
    def fmt(lst,cols=4):
        rows=[]
        for i in range(0,len(lst),cols):
            rows.append('  '+'  '.join(lst[i:i+cols]))
        return '\n'.join(rows)
    forex_=[p for p in FOREX_PAIRS_SCAN
            if p not in ['XAUUSD','XAGUSD','USOIL','UKOIL']]
    await u.message.reply_text(
        f'👁 <b>Scanning {len(ALL_SCAN_PAIRS)} Pairs</b>\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n\n'
        f'🔵 <b>Crypto ({len(CRYPTO_PAIRS)}) 24/7:</b>\n'
        f'{fmt(CRYPTO_PAIRS)}\n\n'
        f'📈 <b>Forex ({len(forex_)}) London/NY:</b>\n'
        f'{fmt(forex_)}\n\n'
        f'🥇 <b>Metals:</b>\n  XAUUSD  XAGUSD\n\n'
        f'🛢 <b>Oil:</b>\n  USOIL  UKOIL\n\n'
        f'<b>SMT Pairs:</b>\n'
        + '\n'.join(f'  {k} ↔ {v}'
                    for k,v in list(SMT_PAIRS.items())[:6]),
        parse_mode='HTML')

async def size_handler(u:Update, c:ContextTypes.DEFAULT_TYPE):
    await u.message.reply_text(
        '💰 <b>Position Size Calculator</b>\n\n'
        'Send in format:\n'
        '<code>/size BALANCE SYMBOL ENTRY SL</code>\n\n'
        'Example:\n'
        '<code>/size 10000 XAUUSD 3285 3298</code>',
        parse_mode='HTML')

async def size_calc_handler(u:Update, c:ContextTypes.DEFAULT_TYPE):
    try:
        parts=u.message.text.split()
        if len(parts)<5:
            await u.message.reply_text(
                '❌ Format: /size BALANCE SYMBOL ENTRY SL\n'
                'Example: /size 10000 XAUUSD 3285 3298')
            return
        balance=float(parts[1])
        symbol =fetcher.normalize(parts[2])
        entry  =float(parts[3])
        sl     =float(parts[4])
        ps=calc_position_size(balance,1.0,entry,sl,symbol)
        if not ps:
            await u.message.reply_text('❌ Invalid values')
            return
        risk1=ps['risk_amount']
        risk2=round(balance*0.5/100,2)
        await u.message.reply_text(
            f'💰 <b>Position Size — {symbol}</b>\n'
            f'━━━━━━━━━━━━━━━━━━━━━━\n'
            f'Account  : ${balance:,.2f}\n'
            f'Entry    : {entry}\n'
            f'Stop Loss: {sl}\n'
            f'SL Dist  : {ps["sl_distance"]}\n'
            f'━━━━━━━━━━━━━━━━━━━━━━\n'
            f'<b>1% Risk:</b>\n'
            f'  Risk amt : ${risk1}\n'
            f'  Lot size : {ps["lot_size"]}\n\n'
            f'<b>0.5% Risk (conservative):</b>\n'
            f'  Risk amt : ${risk2}\n'
            f'━━━━━━━━━━━━━━━━━━━━━━\n'
            f'⚠️ Max 3 trades per day\n'
            f'⚠️ Stop at 2% daily loss',
            parse_mode='HTML')
    except Exception as e:
        await u.message.reply_text(f'❌ Error: {e}')

async def scan_handler(u,c):
    kz   =ICTTimeEngine.get_kill_zone()
    label=ICTTimeEngine.session_label()
    pairs=(CRYPTO_PAIRS if kz=='dead' else ALL_SCAN_PAIRS)
    note ='crypto only — dead session' if kz=='dead' else 'full scan'
    await u.message.reply_text(
        f'🔍 <b>Manual Scan ({note})</b>\n'
        f'Scanning {len(pairs)} pairs...\n'
        f'Session: {label}\n\n'
        f'⏳ Takes 3-5 minutes...',
        parse_mode='HTML')
    found=[]
    for symbol in pairs:
        try:
            is_crypto=fetcher.detect_type(symbol)=='crypto'
            sig=scan_one_pair(symbol)
            if sig and sig.is_alert_worthy():
                found.append((sig,is_crypto))
        except Exception as e:
            log.error(f'Scan {symbol}: {e}')
    if not found:
        await u.message.reply_text(
            f'🔍 <b>No Setups Found</b>\n\n'
            f'Scanned : {len(pairs)} pairs\n'
            f'Need    : Score {Config.ALERT_MIN_SCORE}+ | '
            f'RR {Config.ALERT_MIN_RR}+\n\n'
            f'💡 Best time: London/NY Kill Zones\n'
            f'💡 Best setups: Macro windows (xx:50–xx:10)\n'
            f'⏰ {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}',
            parse_mode='HTML')
        return
    await u.message.reply_text(
        f'✅ <b>{len(found)} ICT Setup(s) Found!</b>',
        parse_mode='HTML')
    for sig,is_crypto in found:
        msg=format_signal(sig,sig.symbol,
                          is_alert=False,is_crypto=is_crypto)
        await u.message.reply_text(msg,parse_mode='HTML')
        await asyncio.sleep(1)

async def symbol_handler(u:Update, c:ContextTypes.DEFAULT_TYPE):
    raw      =u.message.text.strip()
    symbol   =fetcher.normalize(raw)
    is_crypto=fetcher.detect_type(symbol)=='crypto'
    loading  =await u.message.reply_text(
        f'🔍 Analysing <b>{symbol}</b> with ICT framework...\n'
        f'⏳ Please wait...',
        parse_mode='HTML')
    try:
        start=time.time()
        cfg  =Config.for_symbol(symbol)
        smt_s=SMT_PAIRS.get(symbol)
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
            f_htf  =ex.submit(fetcher.fetch,symbol,Config.HTF,200)
            f_ltf  =ex.submit(fetcher.fetch,symbol,Config.LTF,150)
            f_daily=ex.submit(fetcher.fetch,symbol,'1d',30)
            f_week =ex.submit(fetcher.fetch,symbol,'1w',20)
            f_smt  =(ex.submit(fetcher.fetch,smt_s,Config.HTF,100)
                     if smt_s else None)
            f_oil  =(ex.submit(fetcher.get_oil_bias)
                     if cfg.get('check_oil') else None)
            htf   =f_htf.result(); ltf   =f_ltf.result()
            daily =f_daily.result(); weekly=f_week.result()
            smt_c =f_smt.result() if f_smt else None
            oil   =f_oil.result() if f_oil else 'neutral'
        if not htf:
            await loading.edit_text(
                f'❌ No data for: {symbol}\nCheck symbol.',
                parse_mode='HTML'); return
        sig    =engine.analyse(symbol,htf,ltf,daily,weekly,smt_c,oil)
        elapsed=round(time.time()-start,1)
        msg    =format_signal(sig,symbol,
                              is_alert=False,is_crypto=is_crypto)
        msg   +=f'\n⚡ <i>Completed in {elapsed}s</i>'
        await loading.edit_text(msg,parse_mode='HTML')
        log.info(f'{symbol}: {sig.direction.value} '
                 f'Score:{sig.confluence_score} RR:{sig.rr_ratio}')
    except Exception as e:
        log.error(f'{symbol}: {e}')
        await loading.edit_text(
            f'❌ Error: {symbol}\n{str(e)[:200]}',
            parse_mode='HTML')


# ═══════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════
def main():
    token=Config.TELEGRAM_BOT_TOKEN
    if not token:
        log.error('TELEGRAM_BOT_TOKEN not set'); return

    log.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    log.info('ICT Signal Bot v4.0')
    log.info('Features: NYMO | FPFVG | SD | Macros |')
    log.info('          PO3 | OHLC | SMT | Liquidity')
    log.info(f'Pairs  : {len(ALL_SCAN_PAIRS)} total')
    log.info(f'Score  : manual {Config.MIN_CONFLUENCE_SCORE}+ | '
             f'alert {Config.ALERT_MIN_SCORE}+')
    log.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')

    app=ApplicationBuilder().token(token).build()
    app.add_handler(CommandHandler('start' ,start_handler))
    app.add_handler(CommandHandler('help'  ,help_handler))
    app.add_handler(CommandHandler('on'    ,on_handler))
    app.add_handler(CommandHandler('off'   ,off_handler))
    app.add_handler(CommandHandler('status',status_handler))
    app.add_handler(CommandHandler('pairs' ,pairs_handler))
    app.add_handler(CommandHandler('scan'  ,scan_handler))
    app.add_handler(CommandHandler('size'  ,size_handler))
    app.add_handler(MessageHandler(
        filters.TEXT & filters.Regex(r'^/size\s+\S'),
        size_calc_handler))
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND,symbol_handler))

    async def post_init(application):
        if Config.TELEGRAM_CHAT_ID:
            asyncio.create_task(auto_scanner(application))
            log.info('Auto scanner started')
        else:
            log.warning('No CHAT_ID — auto scanner disabled')

    app.post_init=post_init
    log.info('Bot running.')
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__=='__main__':
    main()
