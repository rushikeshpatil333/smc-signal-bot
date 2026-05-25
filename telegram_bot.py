"""
SMC + ICT + MMC Signal Bot — v5.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SMC  : Order Blocks, FVG, BOS, CHOCH,
       Premium/Discount, Liquidity Sweeps
ICT  : NYMO, FPFVG, SD Zones, Macros,
       8:30 & 9:30, PO3, OHLC, SMT,
       Judas Swing, Kill Zones
MMC  : Structure Repetition, CCP,
       Fakeout Theory, 99% Zone Filter,
       Candle Nature, Volume, Wick Lines
NV   : Needed Volume, IFC Candle,
       Parallel Channels, Body Analysis,
       S/R Interchange, NV Quality
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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
log = logging.getLogger('ICT_MMC_Bot_v5')
NY_TZ = ZoneInfo('America/New_York')


# ═══════════════════════════════════════════════
#  PAIRS
# ═══════════════════════════════════════════════
CRYPTO_PAIRS = [
    'BTCUSDT','ETHUSDT','SOLUSDT','BNBUSDT',
    'XRPUSDT','ADAUSDT','DOGEUSDT','DOTUSDT',
    'AVAXUSDT','LINKUSDT','LTCUSDT',
]
FOREX_PAIRS_SCAN = [
    'EURUSD','GBPUSD','USDJPY','AUDUSD','USDCAD',
    'USDCHF','NZDUSD','EURJPY','EURGBP','EURAUD',
    'EURCAD','EURCHF','GBPJPY','GBPAUD','GBPCAD',
    'GBPCHF','GBPNZD','AUDJPY','AUDCAD','AUDCHF',
    'AUDNZD','NZDJPY','NZDCAD','NZDCHF','CADJPY',
    'CADCHF','CHFJPY','XAUUSD','XAGUSD',
    'USOIL','UKOIL',
]
ALL_SCAN_PAIRS = CRYPTO_PAIRS + FOREX_PAIRS_SCAN

SMT_PAIRS = {
    'XAUUSD':'XAGUSD', 'XAGUSD':'XAUUSD',
    'EURUSD':'GBPUSD', 'GBPUSD':'EURUSD',
    'AUDUSD':'NZDUSD', 'NZDUSD':'AUDUSD',
    'BTCUSDT':'ETHUSDT','ETHUSDT':'BTCUSDT',
    'USDCAD':'USDCHF',  'USDCHF':'USDCAD',
}


# ═══════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════
class Config:
    TELEGRAM_BOT_TOKEN   = os.getenv('TELEGRAM_BOT_TOKEN','')
    TELEGRAM_CHAT_ID     = os.getenv('TELEGRAM_CHAT_ID','')
    HTF                  = '1h'
    LTF                  = '5m'
    CANDLE_LIMIT         = 200
    SWING_LOOKBACK       = 5
    MIN_CONFLUENCE_SCORE = 6
    MIN_RR_RATIO         = 1.8
    SL_BUFFER_PCT        = 0.002
    DISPLACEMENT_MULT    = 1.0
    ALERT_MIN_SCORE      = 8
    ALERT_MIN_RR         = 2.0
    ALERT_COOLDOWN_HOURS = 4
    SCAN_INTERVAL_MINS   = 30

    SYMBOL_SETTINGS: Dict = {
        'XAUUSD':{'min_score':6,'min_rr':1.8,
                  'sl_buffer_pct':0.005},
        'GBPUSD':{'min_score':6,'min_rr':1.8,
                  'sl_buffer_pct':0.002},
        'USDCAD':{'min_score':6,'min_rr':1.8,
                  'sl_buffer_pct':0.002,'check_oil':True},
    }

    @classmethod
    def for_symbol(cls, symbol:str) -> Dict:
        defaults = {
            'min_score'    : cls.MIN_CONFLUENCE_SCORE,
            'min_rr'       : cls.MIN_RR_RATIO,
            'sl_buffer_pct': cls.SL_BUFFER_PCT,
            'check_oil'    : False,
        }
        return {**defaults, **cls.SYMBOL_SETTINGS.get(symbol,{})}


# ═══════════════════════════════════════════════
#  STATE
# ═══════════════════════════════════════════════
class ScannerState:
    auto_alerts_on   = True
    last_alerted     : Dict[str,datetime] = {}
    daily_pnl_pct    : float = 0.0
    daily_reset_date : str   = ''

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
            cls.daily_pnl_pct   = 0.0
            cls.daily_reset_date = today

    @classmethod
    def daily_loss_ok(cls) -> bool:
        cls.check_daily_reset()
        return cls.daily_pnl_pct > -3.0


# ═══════════════════════════════════════════════
#  DATA CLASSES
# ═══════════════════════════════════════════════
class SignalDirection(Enum):
    LONG     = 'LONG'
    SHORT    = 'SHORT'
    NO_TRADE = 'NO TRADE'

class PO3Phase(Enum):
    ACCUMULATION = 'Accumulation'
    MANIPULATION = 'Manipulation'
    DISTRIBUTION = 'Distribution'
    UNKNOWN      = 'Unknown'

class NVQuality(Enum):
    PREMIUM  = 'PREMIUM'
    STANDARD = 'STANDARD'
    WEAK     = 'WEAK'
    NONE     = 'NONE'

@dataclass
class Candle:
    time:str; open:float; high:float
    low:float; close:float; volume:float=0.0

    def body_high(self)  : return max(self.open,self.close)
    def body_low(self)   : return min(self.open,self.close)
    def is_bullish(self) : return self.close>self.open
    def is_bearish(self) : return self.close<self.open
    def body_size(self)  : return abs(self.close-self.open)
    def range_size(self) : return self.high-self.low
    def wick_upper(self) : return self.high-self.body_high()
    def wick_lower(self) : return self.body_low()-self.low
    def has_strong_body(self, atr:float) -> bool:
        return self.body_size() >= atr*1.5
    def has_rejection_wick(self, atr:float) -> bool:
        return (self.wick_upper()>=atr*0.8 or
                self.wick_lower()>=atr*0.8)

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
    type:str; price:float; touches:int; swept:bool=False

@dataclass
class ParallelChannel:
    upper_slope   : float
    lower_slope   : float
    upper_intercept:float
    lower_intercept:float
    is_straight   : bool
    direction     : str   # 'bullish'/'bearish'/'neutral'
    strength      : float # 0-1 how clean

@dataclass
class NeededVolumeResult:
    valid         : bool
    nv_type       : str    # 'positive'/'negative'/''
    quality       : NVQuality
    zone_low      : float
    zone_high     : float
    expected_price: float
    actual_price  : float
    ifc_confirmed : bool
    ifc_size      : float
    ifc_direction : str
    location_score: int    # 0/1/2
    size_vs_atr   : float
    is_fragmented : bool
    sr_interchange: bool
    reason        : str    # if invalid
    continuation  : bool   # True if continuation, False if reversal

@dataclass
class MMCContext:
    # Candle Nature
    candle_nature_score   : int   = 0
    strong_body_at_zone   : bool  = False
    rejection_wick_at_zone: bool  = False
    volume_confirms       : bool  = False
    fakeout_detected      : bool  = False
    fakeout_probability   : str   = 'low'  # low/medium/high
    zone_passes_99pct     : bool  = False
    structure_repetition  : bool  = False
    insider_sd_zone       : bool  = False

    # Needed Volume
    nv                    : Optional[NeededVolumeResult] = None
    nv_score              : int   = 0

    # Overall MMC score contribution
    mmcScore              : int   = 0
    reasons               : List[str] = field(default_factory=list)
    warnings              : List[str] = field(default_factory=list)

@dataclass
class ICTContext:
    kill_zone            : str   = 'dead'
    macro_active         : bool  = False
    macro_label          : str   = ''
    is_830_passed        : bool  = False
    is_930_passed        : bool  = False
    midnight_open        : float = 0.0
    open_830             : float = 0.0
    open_930             : float = 0.0
    above_midnight       : bool  = False
    above_830            : bool  = False
    above_930            : bool  = False
    premium_discount     : str   = 'neutral'
    fpfvg                : Optional[FVG] = None
    sd_15                : float = 0.0
    sd_25                : float = 0.0
    sd_45                : float = 0.0
    at_sd_zone           : str   = ''
    buy_pools            : List[LiquidityPool] = field(default_factory=list)
    sell_pools           : List[LiquidityPool] = field(default_factory=list)
    liquidity_swept      : bool  = False
    swept_side           : str   = ''
    po3_phase            : PO3Phase = PO3Phase.UNKNOWN
    ohlc_model           : str   = ''
    is_expansion_day     : bool  = False
    judas_swing          : bool  = False
    judas_direction      : str   = ''
    manipulation_830     : bool  = False
    manipulation_830_dir : str   = ''
    confirmed_930        : bool  = False
    smt_divergence       : bool  = False
    smt_direction        : str   = ''
    smt_pair             : str   = ''
    narrative_score      : int   = 0
    reasons              : List[str] = field(default_factory=list)
    warnings             : List[str] = field(default_factory=list)

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
    ict              : ICTContext  = field(default_factory=ICTContext)
    mmc              : MMCContext  = field(default_factory=MMCContext)
    session          : str        = ''
    reasons          : List[str]  = field(default_factory=list)
    warnings         : List[str]  = field(default_factory=list)
    timestamp        : str        = ''
    is_nv_premium    : bool       = False

    def is_valid(self) -> bool:
        cfg = Config.for_symbol(self.symbol)
        return (self.direction != SignalDirection.NO_TRADE and
                self.rr_ratio  >= cfg['min_rr'] and
                self.confluence_score >= cfg['min_score'])

    def is_alert_worthy(self) -> bool:
        return (self.is_valid() and
                self.confluence_score >= Config.ALERT_MIN_SCORE and
                self.rr_ratio         >= Config.ALERT_MIN_RR)


# ═══════════════════════════════════════════════
#  ICT TIME ENGINE
# ═══════════════════════════════════════════════
class ICTTimeEngine:
    MACRO_WINDOWS = [
        (2,50),(3,10), (3,50),(4,10),
        (9,50),(10,10),(10,50),(11,10),
    ]
    KILL_ZONES = {
        'london_open' :(2, 5),
        'london'      :(5, 8),
        'pre_ny'      :(7, 8),
        'ny_open'     :(8,11),
        'london_close':(10,12),
    }
    NEWS_TIMES_UTC = [
        (8,30),(9,30),(12,30),(13,30),
        (14,0),(14,30),(18,0),(18,30),
    ]
    NEWS_BUFFER_MINS = 30

    @classmethod
    def now_ny(cls): return datetime.now(NY_TZ)

    @classmethod
    def utc_to_ny(cls, dt): return dt.astimezone(NY_TZ)

    @classmethod
    def is_macro_window(cls) -> Tuple[bool,str]:
        now = cls.now_ny(); h,m = now.hour,now.minute
        curr = h*60+m
        hour_start = h*60
        if curr>=hour_start+50 or curr<=hour_start+10:
            return True, f'{h:02d}:50–{(h+1)%24:02d}:10'
        pairs=[(cls.MACRO_WINDOWS[i],cls.MACRO_WINDOWS[i+1])
               for i in range(0,len(cls.MACRO_WINDOWS),2)]
        for (sh,sm),(eh,em) in pairs:
            if sh*60+sm <= curr <= eh*60+em:
                return True, f'{sh:02d}:{sm:02d}–{eh:02d}:{em:02d}'
        return False,''

    @classmethod
    def get_kill_zone(cls) -> str:
        h = cls.now_ny().hour
        for name,(s,e) in cls.KILL_ZONES.items():
            if s<=h<e: return name
        return 'dead'

    @classmethod
    def is_news_time(cls) -> bool:
        now=datetime.now(timezone.utc); curr=now.hour*60+now.minute
        return any(abs(curr-nh*60-nm)<=cls.NEWS_BUFFER_MINS
                   for nh,nm in cls.NEWS_TIMES_UTC)

    @classmethod
    def get_midnight_open_time(cls):
        n=cls.now_ny()
        return n.replace(hour=0,minute=0,second=0,microsecond=0)

    @classmethod
    def is_830_passed(cls):
        n=cls.now_ny()
        return n>=n.replace(hour=8,minute=30,second=0,microsecond=0)

    @classmethod
    def is_930_passed(cls):
        n=cls.now_ny()
        return n>=n.replace(hour=9,minute=30,second=0,microsecond=0)

    @classmethod
    def session_label(cls) -> str:
        labels={'london_open':'🟢 London Open',
                'london':'🟡 London',
                'pre_ny':'🟠 Pre-NY',
                'ny_open':'🟢 NY Open ⭐',
                'london_close':'🟡 London Close',
                'dead':'🔴 Dead Session'}
        return labels.get(cls.get_kill_zone(),'Unknown')


# ═══════════════════════════════════════════════
#  DATA FETCHER
# ═══════════════════════════════════════════════
class PublicDataFetcher:
    BINANCE_TF   = {'1m':'1m','3m':'3m','5m':'5m','15m':'15m',
                    '30m':'30m','1h':'1h','4h':'4h','1d':'1d',
                    '1w':'1w','1M':'1M'}
    YAHOO_INT    = {'5m':'5m','15m':'15m','30m':'30m','1h':'1h',
                    '4h':'1h','1d':'1d','1w':'1wk','1M':'1mo'}
    YAHOO_RANGE  = {'5m':'7d','15m':'60d','30m':'60d','1h':'2y',
                    '4h':'2y','1d':'5y','1w':'10y','1M':'10y'}
    CG_MAP       = {'BTCUSDT':'bitcoin','ETHUSDT':'ethereum',
                    'SOLUSDT':'solana','BNBUSDT':'binancecoin',
                    'XRPUSDT':'ripple','ADAUSDT':'cardano',
                    'DOGEUSDT':'dogecoin','DOTUSDT':'polkadot',
                    'AVAXUSDT':'avalanche-2','LINKUSDT':'chainlink',
                    'LTCUSDT':'litecoin'}
    CG_DAYS      = {'5m':'1','15m':'1','30m':'2','1h':'7',
                    '4h':'30','1d':'365'}
    YAHOO_SYM    = {'XAUUSD':'GC=F','XAGUSD':'SI=F','USOIL':'CL=F',
                    'UKOIL':'BZ=F','BTCUSDT':'BTC-USD',
                    'ETHUSDT':'ETH-USD','SOLUSDT':'SOL-USD',
                    'BNBUSDT':'BNB-USD','XRPUSDT':'XRP-USD',
                    'ADAUSDT':'ADA-USD','DOGEUSDT':'DOGE-USD',
                    'DOTUSDT':'DOT-USD','AVAXUSDT':'AVAX-USD',
                    'LINKUSDT':'LINK-USD','LTCUSDT':'LTC-USD'}
    ALL_FOREX    = [
        'EURUSD','GBPUSD','USDJPY','AUDUSD','USDCAD','USDCHF',
        'NZDUSD','EURJPY','EURGBP','EURAUD','EURCAD','EURCHF',
        'EURNZD','GBPJPY','GBPAUD','GBPCAD','GBPCHF','GBPNZD',
        'AUDJPY','AUDCAD','AUDCHF','AUDNZD','NZDJPY','NZDCAD',
        'NZDCHF','CADJPY','CADCHF','CHFJPY',
        'XAUUSD','XAGUSD','USOIL','UKOIL',
    ]

    def detect_type(self, symbol:str) -> str:
        s=symbol.upper().replace('/','').replace('-','')
        return ('crypto' if any(s.endswith(e)
                for e in ['USDT','BTC','ETH','BNB','BUSD'])
                else 'forex')

    def normalize(self, symbol:str) -> str:
        return symbol.upper().replace('/','').replace('-','').replace(' ','')

    def fetch(self, symbol:str, tf:str, limit:int=None) -> List[Candle]:
        limit=limit or Config.CANDLE_LIMIT
        s=self.normalize(symbol)
        if self.detect_type(s)=='crypto':
            return (self._binance(s,tf,limit) or
                    self._coingecko(s,tf,limit) or
                    self._yahoo(s,tf,limit))
        return self._yahoo(s,tf,limit)

    def _binance(self,symbol,tf,limit):
        try:
            r=requests.get('https://api.binance.com/api/v3/klines',
                params={'symbol':symbol,
                        'interval':self.BINANCE_TF.get(tf,'1h'),
                        'limit':limit},timeout=10)
            if r.status_code!=200: return []
            return [Candle(
                time=str(datetime.fromtimestamp(row[0]/1000,
                          tz=timezone.utc)),
                open=float(row[1]),high=float(row[2]),
                low=float(row[3]),close=float(row[4]),
                volume=float(row[5])) for row in r.json()]
        except Exception as e:
            log.error(f'Binance:{e}'); return []

    def _coingecko(self,symbol,tf,limit):
        try:
            cid=self.CG_MAP.get(symbol)
            if not cid: return []
            r=requests.get(
                f'https://api.coingecko.com/api/v3/coins/{cid}'
                f'/ohlc?vs_currency=usd'
                f'&days={self.CG_DAYS.get(tf,"7")}',
                headers={'User-Agent':'Mozilla/5.0'},timeout=15)
            if r.status_code!=200: return []
            return [Candle(
                time=str(datetime.fromtimestamp(row[0]/1000,
                          tz=timezone.utc)),
                open=float(row[1]),high=float(row[2]),
                low=float(row[3]),close=float(row[4]),
                volume=0.0) for row in r.json()][-limit:]
        except Exception as e:
            log.error(f'CoinGecko:{e}'); return []

    def _yahoo(self,symbol,tf,limit):
        try:
            ys=self._to_yahoo(symbol)
            iv=self.YAHOO_INT.get(tf,'1d')
            rg=self.YAHOO_RANGE.get(tf,'2y')
            hd={'User-Agent':'Mozilla/5.0','Accept':'application/json'}
            for host in ['query1','query2']:
                try:
                    r=requests.get(
                        f'https://{host}.finance.yahoo.com/v8/'
                        f'finance/chart/{ys}?interval={iv}&range={rg}',
                        headers=hd,timeout=15)
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
                                time=str(datetime.fromtimestamp(
                                    ts,tz=timezone.utc)),
                                open=float(o),high=float(h),
                                low=float(l),close=float(c),
                                volume=float(vols[i] or 0)))
                        except Exception: continue
                    if candles: return candles[-limit:]
                except Exception as e:
                    log.error(f'Yahoo {host}:{e}')
            return []
        except Exception as e:
            log.error(f'Yahoo {symbol}:{e}'); return []

    def _to_yahoo(self,symbol):
        if symbol in self.YAHOO_SYM: return self.YAHOO_SYM[symbol]
        if symbol in self.ALL_FOREX:
            return symbol[:3]+symbol[3:]+'=X'
        return symbol

    def get_oil_bias(self) -> str:
        try:
            c=self._yahoo('USOIL','1d',10)
            if len(c)<5: return 'neutral'
            return 'bullish' if c[-1].close>c[-5].close else 'bearish'
        except Exception: return 'neutral'


# ═══════════════════════════════════════════════
#  MMC ANALYSIS ENGINE
# ═══════════════════════════════════════════════
class MMCEngine:

    # ── Candle Nature ────────────────────────
    def score_candle_nature(self, candles:List[Candle],
                             zone_low:float,
                             zone_high:float,
                             direction:str,
                             atr:float) -> Tuple[int,List[str]]:
        score=0; reasons=[]
        zone_mid=(zone_low+zone_high)/2

        # Find candles touching zone
        zone_candles=[c for c in candles[-20:]
                      if c.low<=zone_high and c.high>=zone_low]
        if not zone_candles: return 0,[]

        last_zone=zone_candles[-1]

        # Strong body at zone
        if last_zone.has_strong_body(atr):
            dir_match=((direction=='bullish' and last_zone.is_bullish()) or
                       (direction=='bearish' and last_zone.is_bearish()))
            if dir_match:
                score+=1
                reasons.append('Strong body at zone (commitment)')

        # Rejection wick at zone
        if last_zone.has_rejection_wick(atr):
            score+=1
            reasons.append('Rejection wick at zone')

        # Volume confirmation
        if len(candles)>=5:
            avg_vol=(sum(c.volume for c in candles[-20:])
                     / max(len(candles[-20:]),1))
            if (avg_vol>0 and
                    last_zone.volume>=avg_vol*1.3):
                score+=1
                reasons.append('Volume confirms zone reaction')
            elif avg_vol>0 and last_zone.volume<avg_vol*0.7:
                reasons.append('⚠️ Low volume at zone')

        return score, reasons

    # ── Fakeout Detection ────────────────────
    def detect_fakeout(self, candles:List[Candle],
                        direction:str,
                        atr:float) -> Tuple[bool,str]:
        if len(candles)<15: return False,'low'
        recent=candles[-10:]

        # Breakout then reversal = fakeout
        if direction=='bullish':
            # Bearish fakeout before bull move
            breakdowns=sum(1 for c in recent[:5]
                           if c.is_bearish() and
                           c.body_size()>=atr*1.0)
            reversals =sum(1 for c in recent[5:]
                           if c.is_bullish() and
                           c.body_size()>=atr*1.0)
            if breakdowns>=1 and reversals>=1:
                return True,'bearish_fakeout_reversed'

        else:  # bearish
            # Bullish fakeout before bear move
            breakouts =sum(1 for c in recent[:5]
                           if c.is_bullish() and
                           c.body_size()>=atr*1.0)
            reversals =sum(1 for c in recent[5:]
                           if c.is_bearish() and
                           c.body_size()>=atr*1.0)
            if breakouts>=1 and reversals>=1:
                return True,'bullish_fakeout_reversed'

        # Fakeout probability
        vol_weak=all(c.body_size()<atr*0.8 for c in recent[-5:])
        prob='high' if vol_weak else 'low'
        return False, prob

    # ── 99% Zone Filter ──────────────────────
    def zone_passes_99pct_filter(self, confluences:int) -> bool:
        """Only zones with 3+ confluences pass the quality gate."""
        return confluences >= 3

    # ── Structure Repetition ─────────────────
    def detect_structure_repetition(self,
                                     candles:List[Candle],
                                     direction:str) -> bool:
        if len(candles)<60: return False
        # Check if similar swing pattern appeared before
        recent_ranges=[]
        for i in range(0,min(40,len(candles)-10),10):
            seg=candles[-(i+10):-i] if i>0 else candles[-10:]
            rng=max(c.high for c in seg)-min(c.low for c in seg)
            recent_ranges.append(rng)
        if len(recent_ranges)<3: return False
        avg=sum(recent_ranges)/len(recent_ranges)
        variance=sum(abs(r-avg) for r in recent_ranges)/len(recent_ranges)
        # Low variance = repetitive structure
        return (variance/avg)<0.4 if avg>0 else False

    # ── Straight Market Check ────────────────
    def is_straight_market(self, candles:List[Candle],
                            lookback:int=30) -> bool:
        if len(candles)<lookback: return False
        seg=candles[-lookback:]
        # Use body highs/lows (MMC rule: body over wick)
        highs=[c.body_high() for c in seg]
        lows =[c.body_low()  for c in seg]
        distances=[h-l for h,l in zip(highs,lows)]
        avg=sum(distances)/len(distances) if distances else 0
        if avg==0: return False
        variance=sum(abs(d-avg) for d in distances)/len(distances)
        straightness=variance/avg
        return straightness<0.35

    # ── Parallel Channel Detection ───────────
    def detect_parallel_channel(self,
                                  candles:List[Candle],
                                  lookback:int=40
                                  ) -> Optional[ParallelChannel]:
        if len(candles)<lookback: return None
        seg=candles[-lookback:]

        # Body-based trendlines (MMC rule)
        bh=[c.body_high() for c in seg]
        bl=[c.body_low()  for c in seg]
        n=len(seg)
        xs=list(range(n))

        def linreg(ys):
            mx=sum(xs)/n; my=sum(ys)/n
            num=sum((xs[i]-mx)*(ys[i]-my) for i in range(n))
            den=sum((xs[i]-mx)**2 for i in range(n))
            slope=num/den if den else 0
            intercept=my-slope*mx
            return slope,intercept

        us,ui=linreg(bh)  # upper trendline
        ls,li=linreg(bl)  # lower trendline

        # Channel direction
        avg_slope=(us+ls)/2
        if avg_slope>0.0001:   direction='bullish'
        elif avg_slope<-0.0001:direction='bearish'
        else:                  direction='neutral'

        # Channel strength: how well price respects boundaries
        upper_touches=sum(1 for i,c in enumerate(seg)
                          if abs(c.body_high()-(us*i+ui))<
                          (max(bh)-min(bh))*0.05)
        lower_touches=sum(1 for i,c in enumerate(seg)
                          if abs(c.body_low()-(ls*i+li))<
                          (max(bl)-min(bl))*0.05)
        strength=min((upper_touches+lower_touches)/10.0,1.0)

        is_straight=self.is_straight_market(candles,lookback)

        return ParallelChannel(
            upper_slope=us,lower_slope=ls,
            upper_intercept=ui,lower_intercept=li,
            is_straight=is_straight,
            direction=direction,strength=strength)

    # ── Needed Volume Detection ───────────────
    def detect_needed_volume(self,
                              candles:List[Candle],
                              direction:str,
                              atr:float,
                              sr_levels:List[float]
                              ) -> NeededVolumeResult:
        null=NeededVolumeResult(
            valid=False,nv_type='',quality=NVQuality.NONE,
            zone_low=0,zone_high=0,expected_price=0,
            actual_price=0,ifc_confirmed=False,ifc_size=0,
            ifc_direction='',location_score=0,size_vs_atr=0,
            is_fragmented=False,sr_interchange=False,
            reason='',continuation=False)

        if len(candles)<30:
            null.reason='Insufficient candles'; return null

        # Step 1: Straight market check
        if not self.is_straight_market(candles):
            null.reason='Bending/curved market — NV invalid'
            return null

        # Step 2: Channel detection
        channel=self.detect_parallel_channel(candles)
        if not channel or channel.strength<0.15:
            null.reason='No clean parallel channel'
            return null

        # Step 3: Find structural failure (NV zone)
        # Expected = where price SHOULD have gone (trendline)
        # Actual   = where price actually reversed
        n=len(candles)
        nv_zone=None

        if direction=='bearish':
            # Look for failed attempt to reach upper trendline
            # (Negative NV)
            for i in range(n-5, max(n-25,5), -1):
                c=candles[i]
                expected_upper=(channel.upper_slope*i
                                +channel.upper_intercept)
                # Body failed to reach upper boundary
                gap=expected_upper-c.body_high()
                if gap>atr*0.5 and c.is_bearish():
                    nv_zone={
                        'low' :c.body_high(),
                        'high':expected_upper,
                        'expected':expected_upper,
                        'actual':c.body_high(),
                        'index':i,
                        'type':'negative'
                    }
                    break
        else:
            # Positive NV: failed to reach lower trendline
            for i in range(n-5, max(n-25,5), -1):
                c=candles[i]
                expected_lower=(channel.lower_slope*i
                                +channel.lower_intercept)
                gap=c.body_low()-expected_lower
                if gap>atr*0.5 and c.is_bullish():
                    nv_zone={
                        'low' :expected_lower,
                        'high':c.body_low(),
                        'expected':expected_lower,
                        'actual':c.body_low(),
                        'index':i,
                        'type':'positive'
                    }
                    break

        if not nv_zone:
            null.reason='No structural failure found'
            return null

        nv_size=abs(nv_zone['high']-nv_zone['low'])
        size_vs_atr=nv_size/atr if atr else 0

        # Step 4: IFC candle detection
        ifc,ifc_size,ifc_dir=self._find_ifc(
            candles, nv_zone, direction, atr)
        if not ifc:
            null.reason='No IFC candle at NV zone'
            null.valid=False; return null

        # Step 5: Fragmented movement check
        is_fragmented=self._check_fragmented(
            candles[-15:], direction, atr)

        # Step 6: Location quality
        loc_score=self._score_location(
            nv_zone, sr_levels, atr)

        # Step 7: S/R interchange check
        sr_inter=self._check_sr_interchange(
            candles, nv_zone, atr)

        # Step 8: Continuation vs reversal
        current_direction=self._get_local_trend(candles[-20:])
        is_continuation=(current_direction==direction)

        # Step 9: Quality grading
        if (size_vs_atr>=3 and loc_score>=2 and
                not is_fragmented and ifc_size>=atr*2):
            quality=NVQuality.PREMIUM
        elif (size_vs_atr>=1.5 and loc_score>=1 and
              not is_fragmented):
            quality=NVQuality.STANDARD
        else:
            quality=NVQuality.WEAK

        return NeededVolumeResult(
            valid=True,
            nv_type=nv_zone['type'],
            quality=quality,
            zone_low=round(nv_zone['low'],6),
            zone_high=round(nv_zone['high'],6),
            expected_price=round(nv_zone['expected'],6),
            actual_price=round(nv_zone['actual'],6),
            ifc_confirmed=True,
            ifc_size=round(ifc_size,6),
            ifc_direction=ifc_dir,
            location_score=loc_score,
            size_vs_atr=round(size_vs_atr,2),
            is_fragmented=is_fragmented,
            sr_interchange=sr_inter,
            reason='Valid NV setup',
            continuation=is_continuation)

    def _find_ifc(self, candles, nv_zone, direction, atr):
        """Find Institutional Funding Candle at NV zone."""
        recent=candles[-25:]
        bodies=[c.body_size() for c in recent]
        if not bodies: return None,0,''
        max_body=max(bodies)
        avg_body=sum(bodies)/len(bodies) if bodies else 0
        if avg_body==0: return None,0,''

        for c in reversed(recent):
            # IFC = largest body in local trend
            if (c.body_size()>=max_body*0.75 and
                    c.body_size()>=avg_body*2.0):
                # Should be at or near NV zone
                near_zone=(c.low<=nv_zone['high']*1.002 and
                           c.high>=nv_zone['low']*0.998)
                if near_zone:
                    ifc_dir='bullish' if c.is_bullish() else 'bearish'
                    return c, c.body_size(), ifc_dir
        return None,0,''

    def _check_fragmented(self, candles, direction, atr) -> bool:
        """Fragmented = many small candles instead of one big IFC."""
        if not candles: return True
        bodies=[c.body_size() for c in candles]
        avg=sum(bodies)/len(bodies) if bodies else 0
        small=sum(1 for b in bodies if b<avg*0.7)
        # Fragmented if most candles are small
        return small>len(candles)*0.7

    def _score_location(self, nv_zone, sr_levels, atr) -> int:
        """Score NV location quality (0/1/2)."""
        if not sr_levels: return 0
        nv_mid=(nv_zone['low']+nv_zone['high'])/2
        for level in sr_levels:
            if abs(nv_mid-level)<=atr*1.5:
                return 2  # Major S/R
        for level in sr_levels:
            if abs(nv_mid-level)<=atr*3.0:
                return 1  # Near S/R
        return 0

    def _check_sr_interchange(self, candles,
                                nv_zone, atr) -> bool:
        """Check if NV is near S/R interchange zone."""
        if len(candles)<50: return False
        nv_mid=(nv_zone['low']+nv_zone['high'])/2
        old=candles[:-20]; recent=candles[-20:]
        old_highs=[c.body_high() for c in old]
        old_lows =[c.body_low()  for c in old]
        # Old resistance now acting as support (bullish interchange)
        for h in old_highs[-20:]:
            if abs(nv_mid-h)<=atr*2:
                return True
        # Old support now acting as resistance (bearish interchange)
        for l in old_lows[-20:]:
            if abs(nv_mid-l)<=atr*2:
                return True
        return False

    def _get_local_trend(self, candles) -> str:
        if len(candles)<5: return 'neutral'
        first_half=candles[:len(candles)//2]
        second_half=candles[len(candles)//2:]
        f_avg=sum(c.close for c in first_half)/len(first_half)
        s_avg=sum(c.close for c in second_half)/len(second_half)
        if s_avg>f_avg*1.001: return 'bullish'
        if s_avg<f_avg*0.999: return 'bearish'
        return 'neutral'

    # ── Insider S&D Zones ────────────────────
    def find_insider_sd_zones(self, candles,
                               direction, atr) -> bool:
        """
        Insider S&D: More precise than standard OB/FVG.
        Looks for origin of strong moves with reaction.
        """
        if len(candles)<30: return False
        for i in range(10, min(40, len(candles)-5)):
            c=candles[-i]
            if direction=='bullish' and c.is_bullish():
                # Strong bullish candle with reaction
                if c.body_size()>=atr*1.8:
                    # Check price returned to this zone
                    zone_h=c.body_high(); zone_l=c.body_low()
                    for rc in candles[-i+1:]:
                        if rc.low<=zone_h and rc.high>=zone_l:
                            return True
            elif direction=='bearish' and c.is_bearish():
                if c.body_size()>=atr*1.8:
                    zone_h=c.body_high(); zone_l=c.body_low()
                    for rc in candles[-i+1:]:
                        if rc.low<=zone_h and rc.high>=zone_l:
                            return True
        return False

    # ── Full MMC Analysis ────────────────────
    def analyse_mmc(self, candles:List[Candle],
                    ltf_candles:List[Candle],
                    direction:str, atr:float,
                    zone_low:float, zone_high:float,
                    sr_levels:List[float]) -> MMCContext:
        ctx=MMCContext(); score=0

        # 1. Candle nature scoring
        cn_score,cn_reasons=self.score_candle_nature(
            candles, zone_low, zone_high, direction, atr)
        ctx.candle_nature_score=cn_score
        if cn_score>=2:
            ctx.strong_body_at_zone=True
            score+=1; ctx.reasons.append(
                'Candle nature confirmed at zone')
        elif cn_score>=1:
            score+=1; ctx.reasons.append(
                'Partial candle confirmation')
        ctx.reasons.extend(cn_reasons)

        # 2. Fakeout detection
        fo,fo_prob=self.detect_fakeout(candles,direction,atr)
        ctx.fakeout_detected=fo
        ctx.fakeout_probability=fo_prob
        if fo:
            score+=1; ctx.reasons.append(
                'Fakeout detected — reversal confirmed ✅')
        elif fo_prob=='high':
            ctx.warnings.append('High fakeout probability ⚠️')

        # 3. 99% zone filter
        # Count active confluences to determine quality
        zone_confluence=cn_score+(1 if fo else 0)
        ctx.zone_passes_99pct=self.zone_passes_99pct_filter(
            zone_confluence)
        if ctx.zone_passes_99pct:
            score+=1; ctx.reasons.append(
                '99% quality filter: PASSED ✅')
        else:
            ctx.warnings.append('99% filter: needs more confluence')

        # 4. Structure repetition
        ctx.structure_repetition=self.detect_structure_repetition(
            candles, direction)
        if ctx.structure_repetition:
            score+=1; ctx.reasons.append(
                'Structure repetition confirmed')

        # 5. Insider S&D zones
        ctx.insider_sd_zone=self.find_insider_sd_zones(
            candles, direction, atr)
        if ctx.insider_sd_zone:
            score+=1; ctx.reasons.append(
                'Insider S&D zone identified')

        # 6. Needed Volume analysis
        nv=self.detect_needed_volume(
            candles, direction, atr, sr_levels)
        ctx.nv=nv
        if nv.valid:
            nv_pts=0
            if nv.quality==NVQuality.PREMIUM:
                nv_pts=3
                ctx.reasons.append(
                    f'NV: PREMIUM ⭐⭐⭐ '
                    f'(size={nv.size_vs_atr:.1f}x ATR)')
            elif nv.quality==NVQuality.STANDARD:
                nv_pts=2
                ctx.reasons.append(
                    f'NV: STANDARD ⭐⭐ '
                    f'(size={nv.size_vs_atr:.1f}x ATR)')
            else:
                nv_pts=1
                ctx.reasons.append(
                    f'NV: WEAK ⭐ — verify carefully')

            if nv.ifc_confirmed:
                nv_pts+=1
                ctx.reasons.append(
                    f'IFC candle confirmed '
                    f'({nv.ifc_direction})')
            if nv.location_score>=2:
                nv_pts+=1
                ctx.reasons.append('NV at major S/R ✅')
            if nv.sr_interchange:
                nv_pts+=1
                ctx.reasons.append('S/R interchange at NV ✅')
            if nv.is_fragmented:
                nv_pts-=1
                ctx.warnings.append(
                    'Fragmented movement at NV ⚠️')

            ctx.nv_score=max(0,nv_pts)
            score+=ctx.nv_score
        else:
            ctx.warnings.append(
                f'NV: {nv.reason}')

        ctx.mmcScore=score
        return ctx


# ═══════════════════════════════════════════════
#  ICT ANALYSIS ENGINE
# ═══════════════════════════════════════════════
class ICTEngine:

    def get_atr(self, candles, period=14) -> float:
        if len(candles)<period+1: return 0.0
        trs=[max(candles[i].high-candles[i].low,
                 abs(candles[i].high-candles[i-1].close),
                 abs(candles[i].low -candles[i-1].close))
             for i in range(1,len(candles))]
        return sum(trs[-period:])/period

    def detect_swings(self, candles, lb=None):
        lb=lb or Config.SWING_LOOKBACK; pts=[]
        for i in range(lb,len(candles)-lb):
            c=candles[i]
            if all(c.high>candles[i-k].high and
                   c.high>candles[i+k].high
                   for k in range(1,lb+1)):
                pts.append(SwingPoint('swing_high',c.high,i))
            if all(c.low<candles[i-k].low and
                   c.low<candles[i+k].low
                   for k in range(1,lb+1)):
                pts.append(SwingPoint('swing_low',c.low,i))
        return sorted(pts,key=lambda x:x.index)

    def classify_structure(self, swings):
        result=[]; ph=pl=None
        for s in swings:
            if s.type in ['swing_high','HH','LH']:
                s.type='HH' if (ph is None or
                    s.price>ph.price) else 'LH'
                ph=s
            else:
                s.type='HL' if (pl is None or
                    s.price>pl.price) else 'LL'
                pl=s
            result.append(s)
        return result

    def get_trend(self, swings) -> str:
        if len(swings)<4: return 'ranging'
        last=[s.type for s in swings[-6:]]
        if last.count('HH')+last.count('HL')>=4:
            return 'uptrend'
        if last.count('LL')+last.count('LH')>=4:
            return 'downtrend'
        return 'ranging'

    def trend_strength(self, swings) -> int:
        if len(swings)<6: return 0
        last=[s.type for s in swings[-8:]]
        mx=max(last.count('HH')+last.count('HL'),
               last.count('LL')+last.count('LH'))
        return 3 if mx>=6 else 2 if mx>=4 else 1 if mx>=2 else 0

    def find_obs(self, candles, direction) -> List[OrderBlock]:
        obs=[]; ar=(sum(c.range_size() for c in candles)
                    /max(len(candles),1))
        thr=ar*Config.DISPLACEMENT_MULT
        for i in range(len(candles)-3):
            c=candles[i]
            if direction=='bullish' and not c.is_bearish(): continue
            if direction=='bearish' and not c.is_bullish(): continue
            disp=any((candles[j].is_bullish()
                      if direction=='bullish'
                      else candles[j].is_bearish()) and
                     candles[j].body_size()>=thr
                     for j in range(i+1,min(i+4,len(candles))))
            if not disp: continue
            obs.append(OrderBlock(
                direction=direction,
                zone_low=c.low,zone_high=c.high,
                midpoint=(c.low+c.high)/2,index=i))
        for ob in obs:
            for c in candles[ob.index+3:]:
                if c.low<=ob.zone_high and c.high>=ob.zone_low:
                    ob.status='tapped'; break
        return obs

    def find_fvgs(self, candles, direction,
                  mark_fp=False) -> List[FVG]:
        fvgs=[]; found=False
        for i in range(len(candles)-2):
            c1,c3=candles[i],candles[i+2]
            if direction=='bullish' and c1.high<c3.low:
                f=FVG('bullish',c1.high,c3.low,
                      (c1.high+c3.low)/2,i+2)
                if mark_fp and not found:
                    f.is_fpfvg=True; found=True
                fvgs.append(f)
            elif direction=='bearish' and c1.low>c3.high:
                f=FVG('bearish',c3.high,c1.low,
                      (c3.high+c1.low)/2,i+2)
                if mark_fp and not found:
                    f.is_fpfvg=True; found=True
                fvgs.append(f)
        return fvgs

    def get_key_opens(self, candles) -> Tuple[float,float,float]:
        mo=o830=o930=0.0
        for c in reversed(candles):
            try:
                dt=datetime.fromisoformat(
                    str(c.time).replace('Z','+00:00'))
                ny=ICTTimeEngine.utc_to_ny(dt)
                if ny.hour==0 and mo==0: mo=c.open
                if ny.hour==8 and o830==0: o830=c.open
                if ny.hour==9 and o930==0: o930=c.open
                if all([mo,o830,o930]): break
            except Exception: continue
        return mo,o830,o930

    def get_candles_since_midnight(self,
                                    ltf) -> List[Candle]:
        result=[]
        for c in ltf:
            try:
                dt=datetime.fromisoformat(
                    str(c.time).replace('Z','+00:00'))
                ny=ICTTimeEngine.utc_to_ny(dt)
                td=ny.replace(hour=0,minute=0,
                               second=0,microsecond=0)
                if ny>=td: result.append(c)
            except Exception: continue
        return result

    def calc_sd_zones(self, rh, rl,
                       direction) -> Tuple[float,float,float]:
        rng=rh-rl
        if rng==0: return 0.0,0.0,0.0
        if direction=='bullish':
            return (rh+rng*1.5, rh+rng*2.5, rh+rng*4.5)
        return (rl-rng*1.5, rl-rng*2.5, rl-rng*4.5)

    def find_liquidity_pools(self,
                              candles) -> Tuple[List[LiquidityPool],
                                                List[LiquidityPool]]:
        bp=[]; sp=[]; atr=self.get_atr(candles); buf=atr*0.3
        highs=[c.high for c in candles]
        lows =[c.low  for c in candles]
        for i,h in enumerate(highs):
            cnt=sum(1 for x in highs if abs(x-h)<=buf)
            if cnt>=3 and not any(abs(p.price-h)<=buf for p in bp):
                bp.append(LiquidityPool('buy_side',h,cnt))
        for i,l in enumerate(lows):
            cnt=sum(1 for x in lows if abs(x-l)<=buf)
            if cnt>=3 and not any(abs(p.price-l)<=buf for p in sp):
                sp.append(LiquidityPool('sell_side',l,cnt))
        recent=candles[-10:]
        for p in bp:
            if any(c.high>=p.price for c in recent): p.swept=True
        for p in sp:
            if any(c.low<=p.price for c in recent): p.swept=True
        bp.sort(key=lambda x:x.price,reverse=True)
        sp.sort(key=lambda x:x.price)
        return bp[:5],sp[:5]

    def detect_po3_phase(self, candles,
                          direction) -> PO3Phase:
        if len(candles)<20: return PO3Phase.UNKNOWN
        recent=candles[-20:]; atr=self.get_atr(candles)
        ranges=[c.range_size() for c in recent[-10:]]
        avg_r=sum(ranges)/len(ranges) if ranges else 0
        if avg_r<atr*0.7: return PO3Phase.ACCUMULATION
        last5=recent[-5:]
        if direction=='bullish':
            if any(c.is_bearish() and c.body_size()>=atr*1.5
                   for c in last5):
                return PO3Phase.MANIPULATION
            if sum(1 for c in last5 if c.is_bullish())>=3:
                return PO3Phase.DISTRIBUTION
        else:
            if any(c.is_bullish() and c.body_size()>=atr*1.5
                   for c in last5):
                return PO3Phase.MANIPULATION
            if sum(1 for c in last5 if c.is_bearish())>=3:
                return PO3Phase.DISTRIBUTION
        return PO3Phase.UNKNOWN

    def detect_ohlc_model(self, daily) -> str:
        if not daily: return ''
        t=daily[-1]
        return ('bullish_olhc' if t.is_bullish()
                else 'bearish_ohlc' if t.is_bearish() else '')

    def detect_judas_swing(self, candles,
                            direction) -> Tuple[bool,str]:
        if len(candles)<10: return False,''
        recent=candles[-10:]; atr=self.get_atr(candles)
        if direction=='bullish':
            ed=any(c.is_bearish() and c.body_size()>=atr*1.2
                   for c in recent[:5])
            lb=any(c.is_bullish() and c.body_size()>=atr*1.2
                   for c in recent[5:])
            if ed and lb: return True,'bearish_fake'
        else:
            ep=any(c.is_bullish() and c.body_size()>=atr*1.2
                   for c in recent[:5])
            lb=any(c.is_bearish() and c.body_size()>=atr*1.2
                   for c in recent[5:])
            if ep and lb: return True,'bullish_fake'
        return False,''

    def detect_830_manip(self, htf, direction,
                          o830) -> Tuple[bool,str]:
        if not o830: return False,''
        atr=self.get_atr(htf); recent=htf[-6:]
        if direction=='bullish':
            if any(c.low<o830-atr*0.5 for c in recent):
                return True,'drop_below_830'
        else:
            if any(c.high>o830+atr*0.5 for c in recent):
                return True,'spike_above_830'
        return False,''

    def detect_smt(self, ca, cb,
                   direction) -> Tuple[bool,str]:
        if len(ca)<20 or len(cb)<20: return False,''
        ar=ca[-10:]; br=cb[-10:]
        ap=ca[-20:-10]; bp_=cb[-20:-10]
        if not ap or not bp_: return False,''
        ah=max(c.high for c in ar)
        al=min(c.low  for c in ar)
        bh=max(c.high for c in br)
        bl=min(c.low  for c in br)
        pah=max(c.high for c in ap)
        pal=min(c.low  for c in ap)
        pbh=max(c.high for c in bp_)
        pbl=min(c.low  for c in bp_)
        if direction=='bearish':
            if ah>pah and bh<=pbh: return True,'bearish_div'
        else:
            if al<pal and bl>=pbl: return True,'bullish_div'
        return False,''

    def ltf_choch(self, ltf, direction) -> bool:
        if len(ltf)<20: return False
        sw=self.classify_structure(
            self.detect_swings(ltf[-50:],lb=2))
        trend=self.get_trend(sw)
        if direction=='bullish':
            return (trend=='uptrend' or
                    any(s.type in ['HL','HH'] for s in sw[-6:]))
        return (trend=='downtrend' or
                any(s.type in ['LH','LL'] for s in sw[-6:]))

    def mtf_trend(self, htf, daily, weekly,
                  direction) -> Tuple[bool,int]:
        def tr(c):
            if not c: return 'ranging'
            return self.get_trend(
                self.classify_structure(self.detect_swings(c)))
        exp='uptrend' if direction=='bullish' else 'downtrend'
        cnt=sum(1 for t in [tr(htf),tr(daily),tr(weekly)]
                if t==exp)
        return cnt==3,cnt

    def get_sr_levels(self, candles) -> List[float]:
        """Extract major support/resistance levels."""
        if len(candles)<20: return []
        levels=[]
        swings=self.detect_swings(candles,lb=5)
        for s in swings:
            if not any(abs(s.price-l)<self.get_atr(candles)
                       for l in levels):
                levels.append(s.price)
        return sorted(levels)


# ═══════════════════════════════════════════════
#  MASTER ANALYSIS ENGINE
# ═══════════════════════════════════════════════
class MasterEngine:

    def __init__(self):
        self.ict=ICTEngine()
        self.mmc=MMCEngine()

    def analyse(self, symbol, htf, ltf, daily, weekly,
                smt_candles=None,
                oil_bias='neutral') -> SMCSignal:

        ts  =datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        cfg =Config.for_symbol(symbol)
        is_c=fetcher.detect_type(symbol)=='crypto'
        kz  =ICTTimeEngine.get_kill_zone()

        no=SMCSignal(SignalDirection.NO_TRADE,symbol,
                     0,0,0,0,0,0,0,0,'ranging','none',
                     ICTContext(),MMCContext(),
                     session=kz,timestamp=ts)

        if len(htf)<50 or len(ltf)<30:
            no.warnings=['Insufficient candle data']; return no

        cp  =htf[-1].close
        atr =self.ict.get_atr(htf)
        sw  =self.ict.classify_structure(
            self.ict.detect_swings(htf))
        trend=self.ict.get_trend(sw)

        if trend=='ranging':
            no.warnings=['Market ranging']; return no

        direction='bullish' if trend=='uptrend' else 'bearish'

        # Volatility spike check
        if atr>0 and htf[-1].range_size()>atr*2.5:
            no.warnings=['Volatility spike — wait']; return no

        # OB + FVG
        obs   =self.ict.find_obs(htf,direction)
        fresh =[o for o in obs if o.status=='fresh']
        if not fresh:
            no.warnings=['No fresh Order Block']; return no

        all_fvg =self.ict.find_fvgs(htf,direction)
        fresh_fg=[f for f in all_fvg if f.status=='fresh']

        best_ob=None
        for ob in reversed(fresh):
            if (ob.zone_low<=cp<=ob.zone_high or
                    abs(cp-ob.midpoint)/max(ob.midpoint,1e-9)<0.03):
                best_ob=ob; break
        if not best_ob: best_ob=fresh[-1]

        best_fvg=None
        for fvg in reversed(fresh_fg):
            if (fvg.zone_low<=best_ob.zone_high and
                    fvg.zone_high>=best_ob.zone_low):
                best_fvg=fvg; break

        # ── ICT Context ─────────────────────
        ict=ICTContext()
        ma,ml=ICTTimeEngine.is_macro_window()
        ict.kill_zone=kz; ict.macro_active=ma; ict.macro_label=ml
        ict.is_830_passed=ICTTimeEngine.is_830_passed()
        ict.is_930_passed=ICTTimeEngine.is_930_passed()

        mo,o830,o930=self.ict.get_key_opens(htf)
        ict.midnight_open=mo; ict.open_830=o830; ict.open_930=o930
        ict.above_midnight=(cp>mo) if mo else False
        ict.above_830=(cp>o830) if o830 else False
        ict.above_930=(cp>o930) if o930 else False
        ac=sum([ict.above_midnight,ict.above_830,ict.above_930])
        ict.premium_discount=('deep_premium' if ac>=2
                              else 'deep_discount' if ac<=1
                              else 'neutral')

        # FPFVG
        since_mn=self.ict.get_candles_since_midnight(ltf)
        if len(since_mn)>=3:
            fps=self.ict.find_fvgs(since_mn,direction,mark_fp=True)
            fp =[f for f in fps if f.is_fpfvg]
            ict.fpfvg=fp[0] if fp else None

        # SD Zones
        if mo and len(htf)>=50:
            seg=htf[-50:-40]
            mh=max(c.high for c in seg)
            ml2=min(c.low  for c in seg)
            s15,s25,s45=self.ict.calc_sd_zones(mh,ml2,direction)
            ict.sd_15=s15; ict.sd_25=s25; ict.sd_45=s45
            buf=atr*0.5
            ict.at_sd_zone=(
                '4.5' if abs(cp-s45)<=buf else
                '2.5' if abs(cp-s25)<=buf else
                '1.5' if abs(cp-s15)<=buf else '')

        # Liquidity
        ict.buy_pools,ict.sell_pools=\
            self.ict.find_liquidity_pools(htf)
        if direction=='bullish':
            if any(p.swept for p in ict.sell_pools):
                ict.liquidity_swept=True; ict.swept_side='sell_side'
        else:
            if any(p.swept for p in ict.buy_pools):
                ict.liquidity_swept=True; ict.swept_side='buy_side'

        # PO3 / OHLC / Judas
        ict.po3_phase =self.ict.detect_po3_phase(ltf,direction)
        ict.ohlc_model=self.ict.detect_ohlc_model(daily)
        ict.judas_swing,ict.judas_direction=\
            self.ict.detect_judas_swing(ltf,direction)
        ict.is_expansion_day=(
            self.ict.trend_strength(sw)>=2 and
            ict.po3_phase==PO3Phase.DISTRIBUTION)

        # 8:30 / 9:30
        if o830 and ict.is_830_passed:
            ict.manipulation_830,ict.manipulation_830_dir=\
                self.ict.detect_830_manip(htf,direction,o830)
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

        # SMT
        if smt_candles and len(smt_candles)>=20:
            ict.smt_divergence,ict.smt_direction=\
                self.ict.detect_smt(htf,smt_candles,direction)
            ict.smt_pair=SMT_PAIRS.get(symbol,'')

        # MTF
        mtf_full,mtf_cnt=self.ict.mtf_trend(
            htf,daily,weekly,direction)

        # LTF CHOCH
        ltf_ok=self.ict.ltf_choch(ltf,direction)

        # HTF bias
        def cb(c):
            if not c: return 'neutral'
            return ('bullish' if c.close>c.open else
                    'bearish' if c.close<c.open else 'neutral')
        wb=cb(weekly[-1]) if weekly else 'neutral'
        db=cb(daily[-1])  if daily  else 'neutral'
        bias=('bullish' if [wb,db].count('bullish')>=2 else
              'bearish' if [wb,db].count('bearish')>=2 else 'neutral')

        # S/R levels for MMC
        sr_levels=self.ict.get_sr_levels(htf)

        # ── MMC Context ─────────────────────
        zone_l=best_ob.zone_low; zone_h=best_ob.zone_high
        mmc_ctx=self.mmc.analyse_mmc(
            htf, ltf, direction, atr,
            zone_l, zone_h, sr_levels)

        # ── SCORING ─────────────────────────
        # Raw max = 20 → normalize to 10
        score=0; reasons=[]; warns=[]

        # TIME (4 pts)
        if kz!='dead' or is_c:
            score+=1; reasons.append(f'Kill zone: {kz}')
        if ma:
            score+=1; reasons.append(f'Macro: {ml}')
        if ict.manipulation_830:
            score+=1; reasons.append(
                f'8:30 manip: {ict.manipulation_830_dir}')
        if ict.confirmed_930:
            score+=1; reasons.append('9:30 confirmed')

        # ICT PRICE (4 pts)
        nymo_ok=((direction=='bullish' and not ict.above_midnight) or
                 (direction=='bearish' and ict.above_midnight))
        if nymo_ok and mo:
            score+=1; reasons.append(
                f'NYMO: price '
                f'{"below" if direction=="bullish" else "above"} '
                f'{mo:.5f}')
        elif mo:
            warns.append('Wrong side of NYMO')

        pd_ok=((direction=='bullish' and
                ict.premium_discount=='deep_discount') or
               (direction=='bearish' and
                ict.premium_discount=='deep_premium'))
        if pd_ok:
            score+=1; reasons.append(
                ict.premium_discount.replace('_',' ').title())
        else:
            warns.append('Not ideal premium/discount')

        if ict.fpfvg:
            score+=1; reasons.append(
                f'FPFVG @ {ict.fpfvg.zone_low:.5f}–'
                f'{ict.fpfvg.zone_high:.5f}')
        else:
            warns.append('No FPFVG today')

        if ict.at_sd_zone:
            score+=1; reasons.append(
                f'At {ict.at_sd_zone} SD zone')

        # NARRATIVE (4 pts)
        if ict.po3_phase==PO3Phase.DISTRIBUTION:
            score+=1; reasons.append('PO3: Distribution ✅')
        elif ict.po3_phase==PO3Phase.MANIPULATION:
            score+=1; reasons.append('PO3: Manipulation (entry soon)')
        else:
            warns.append(f'PO3: {ict.po3_phase.value}')

        ohlc_ok=((direction=='bullish' and
                  ict.ohlc_model=='bullish_olhc') or
                 (direction=='bearish' and
                  ict.ohlc_model=='bearish_ohlc'))
        if ohlc_ok:
            score+=1; reasons.append(
                f'OHLC: {"O→L→H→C" if direction=="bullish" else "O→H→L→C"}')
        else:
            warns.append('OHLC model mismatch')

        if ict.liquidity_swept:
            score+=1; reasons.append(
                f'Liquidity swept: {ict.swept_side}')
        else:
            warns.append('Liquidity not swept yet')

        if ict.smt_divergence:
            score+=1; reasons.append(
                f'SMT vs {ict.smt_pair}')

        # SMC CONFIRMATION (4 pts)
        if bias==direction:
            score+=1; reasons.append(f'HTF: W:{wb} D:{db}')
        else:
            warns.append(f'HTF mismatch: {bias}')

        if best_fvg:
            score+=1; reasons.append(
                f'OB+FVG @ {best_ob.zone_low:.5f}–'
                f'{best_ob.zone_high:.5f}')
        else:
            reasons.append(
                f'OB @ {best_ob.zone_low:.5f}–{best_ob.zone_high:.5f}')

        if mtf_full:
            score+=1; reasons.append('All 3 TFs aligned ✅')
        elif mtf_cnt>=2:
            score+=1; reasons.append(f'{mtf_cnt}/3 TFs aligned')
        else:
            warns.append('MTF not aligned')

        if ltf_ok:
            score+=1; reasons.append('LTF CHOCH ✅')
        else:
            warns.append('Waiting LTF CHOCH')

        # MMC QUALITY (bonus — up to 4 pts)
        mmc_contribution=min(mmc_ctx.mmcScore,4)
        score+=mmc_contribution
        reasons.extend(mmc_ctx.reasons)
        warns.extend(mmc_ctx.warnings)

        # Judas bonus
        if ict.judas_swing:
            score+=1; reasons.append('Judas swing ✅')

        # Oil check
        if cfg.get('check_oil') and oil_bias!='neutral':
            oil_imp='bearish' if oil_bias=='bullish' else 'bullish'
            if oil_imp!=direction:
                warns.append(f'Oil conflict: {oil_bias}')

        # Normalize to 10
        raw_max=22
        normalized=round((score/raw_max)*10)
        normalized=min(10,normalized)

        if normalized<cfg['min_score']:
            no.warnings=[f'Score {normalized} < {cfg["min_score"]}',
                         *warns]
            no.ict=ict; no.mmc=mmc_ctx; return no
        if not ltf_ok:
            no.warnings=['Waiting LTF CHOCH',*warns]
            no.ict=ict; no.mmc=mmc_ctx; return no

        # ── Entry / SL / TP ─────────────────
        if best_fvg:
            el=max(best_ob.zone_low,best_fvg.zone_low)
            eh=min(best_ob.zone_high,best_fvg.zone_high)
            if el>=eh: el,eh=best_ob.zone_low,best_ob.zone_high
            block_type='OB + FVG'
        else:
            el,eh=best_ob.zone_low,best_ob.zone_high
            block_type='Order Block'

        entry=(el+eh)/2
        buf=entry*cfg['sl_buffer_pct']
        sl=(el-buf if direction=='bullish' else eh+buf)
        risk=abs(entry-sl)
        if risk==0:
            no.warnings=['Invalid SL']; return no

        m=1 if direction=='bullish' else -1

        # SD zones for targets (preferred)
        if ict.sd_15 and ict.sd_25 and ict.sd_45:
            if direction=='bullish':
                t1=(ict.sd_15 if ict.sd_15>entry
                    else entry+risk*1.5)
                t2=(ict.sd_25 if ict.sd_25>entry
                    else entry+risk*2.5)
                t3=(ict.sd_45 if ict.sd_45>entry
                    else entry+risk*3.5)
            else:
                t1=(ict.sd_15 if ict.sd_15<entry
                    else entry-risk*1.5)
                t2=(ict.sd_25 if ict.sd_25<entry
                    else entry-risk*2.5)
                t3=(ict.sd_45 if ict.sd_45<entry
                    else entry-risk*3.5)
        else:
            # Fallback: liquidity pools
            bp=[p.price for p in ict.buy_pools if p.price>entry]
            sp=[p.price for p in ict.sell_pools if p.price<entry]
            tgts=(sorted(bp) if direction=='bullish'
                  else sorted(sp,reverse=True))
            t1=tgts[0] if len(tgts)>0 else entry+risk*1.5*m
            t2=tgts[1] if len(tgts)>1 else entry+risk*2.5*m
            t3=tgts[2] if len(tgts)>2 else entry+risk*3.5*m

        rr=round(abs(t2-entry)/risk,2)
        if rr<cfg['min_rr']:
            no.warnings=[f'RR {rr} < {cfg["min_rr"]}']; return no

        ict.narrative_score=normalized
        ict.reasons=reasons; ict.warnings=warns

        is_nv_premium=(mmc_ctx.nv is not None and
                       mmc_ctx.nv.valid and
                       mmc_ctx.nv.quality==NVQuality.PREMIUM)

        return SMCSignal(
            direction=(SignalDirection.LONG
                       if direction=='bullish'
                       else SignalDirection.SHORT),
            symbol=symbol,
            entry_low=round(el,6),entry_high=round(eh,6),
            stop_loss=round(sl,6),
            target_1=round(t1,6),target_2=round(t2,6),
            target_3=round(t3,6),
            rr_ratio=rr,confluence_score=normalized,
            trend=trend,block_type=block_type,
            ict=ict,mmc=mmc_ctx,
            session=kz,
            reasons=reasons,warnings=warns,
            timestamp=ts,is_nv_premium=is_nv_premium)


# ═══════════════════════════════════════════════
#  POSITION SIZE CALCULATOR
# ═══════════════════════════════════════════════
def calc_position_size(balance:float, risk_pct:float,
                        entry:float, sl:float,
                        symbol:str) -> Dict:
    risk_amt=balance*(risk_pct/100)
    sl_dist=abs(entry-sl)
    if sl_dist==0: return {}
    if fetcher.detect_type(symbol)=='crypto':
        lot=risk_amt/sl_dist
    else:
        sl_adj=sl_dist*10000 if sl_dist<1 else sl_dist
        lot=risk_amt/(sl_adj*10)
        lot=round(lot,2)
    return {'risk_amount':round(risk_amt,2),
            'sl_distance':round(sl_dist,6),
            'lot_size'   :round(lot,4),
            'risk_pct'   :risk_pct}


# ═══════════════════════════════════════════════
#  FORMAT SIGNAL
# ═══════════════════════════════════════════════
PO3_EMOJI={
    PO3Phase.ACCUMULATION:'⏸ Accumulation',
    PO3Phase.MANIPULATION:'🎭 Manipulation',
    PO3Phase.DISTRIBUTION:'🚀 Distribution',
    PO3Phase.UNKNOWN     :'❓ Unknown',
}
NV_QUALITY_EMOJI={
    NVQuality.PREMIUM :'⭐⭐⭐ PREMIUM',
    NVQuality.STANDARD:'⭐⭐ STANDARD',
    NVQuality.WEAK    :'⭐ WEAK',
    NVQuality.NONE    :'',
}

def format_signal(sig:SMCSignal, symbol:str,
                  is_alert:bool=False,
                  is_crypto:bool=False,
                  account_balance:float=0) -> str:

    cfg  =Config.for_symbol(symbol)
    badge=('🚨 <b>AUTO ALERT — ICT+SMC+MMC</b>\n'
           if is_alert else '')
    ict  =sig.ict; mmc=sig.mmc
    kz   =ICTTimeEngine.session_label()
    mkt  ='🔵 Crypto (24/7)' if is_crypto else kz

    if not sig.is_valid():
        warns=('\n'.join(f'  ⚠️ {w}' for w in sig.warnings)
               or '  No setup')
        return (f'{badge}🔍 <b>{symbol}</b>\n'
                f'━━━━━━━━━━━━━━━━━━━━━━\n'
                f'🕐 {mkt}\n'
                f'━━━━━━━━━━━━━━━━━━━━━━\n'
                f'⏳ <b>No Trade Setup</b>\n\n'
                f'{warns}\n\n⏰ {sig.timestamp}')

    em   =('🟢' if sig.direction==SignalDirection.LONG
           else '🔴')
    stars=('⭐⭐⭐' if sig.confluence_score>=8 else
           '⭐⭐'  if sig.confluence_score>=6 else '⭐')
    premium_badge=(' 🏆 NV PREMIUM' if sig.is_nv_premium else '')

    rsns='\n'.join(f'  ✅ {r}' for r in sig.reasons)
    warns='\n'.join(f'  ⚠️ {w}' for w in sig.warnings)

    vdir=('UPTREND' if sig.direction==SignalDirection.LONG
          else 'DOWNTREND')
    sw  =('LOW sweep + CHOCH UP'
          if sig.direction==SignalDirection.LONG
          else 'HIGH sweep + CHOCH DOWN')
    loc =('DISCOUNT' if sig.direction==SignalDirection.LONG
          else 'PREMIUM')

    # ICT block
    macro_l=(f'⚡ Macro: {ict.macro_label} Active\n'
             if ict.macro_active else '')
    po3_l  =f'📖 PO3:   {PO3_EMOJI.get(ict.po3_phase,"")}\n'
    ohlc_m ={'bullish_olhc':'O→L→H→C 📈',
              'bearish_ohlc':'O→H→L→C 📉'}
    ohlc_l =(f'📊 OHLC:  {ohlc_m.get(ict.ohlc_model,"")}\n'
             if ict.ohlc_model else '')
    judas_l=('🎭 Judas Swing: Detected ✅\n'
             if ict.judas_swing else '')

    # Opens block
    tick=lambda b:'✅' if b else '❌'
    pd_em=('🔴 Deep Premium' if ict.premium_discount=='deep_premium'
           else '🟢 Deep Discount'
           if ict.premium_discount=='deep_discount'
           else '⚪ Neutral')
    opens_b=''
    if ict.midnight_open:
        opens_b=(f'━━━━━━━━━━━━━━━━━━━━━━\n'
                 f'{pd_em}\n'
                 f'  12AM: {ict.midnight_open:.5f} '
                 f'{tick(ict.above_midnight)}\n')
        if ict.open_830:
            opens_b+=(f'  8:30: {ict.open_830:.5f} '
                      f'{tick(ict.above_830)}\n')
        if ict.open_930:
            opens_b+=(f'  9:30: {ict.open_930:.5f} '
                      f'{tick(ict.above_930)}\n')

    # FPFVG
    fp_l=''
    if ict.fpfvg:
        fp_l=(f'🎯 FPFVG: {ict.fpfvg.zone_low:.5f}–'
              f'{ict.fpfvg.zone_high:.5f}\n')

    # SD zones
    sd_l=''
    if ict.sd_15:
        sd_l=(f'📐 SD: 1.5={ict.sd_15:.5f} | '
              f'2.5={ict.sd_25:.5f} | '
              f'4.5={ict.sd_45:.5f}\n')

    # SMT
    smt_l=''
    if ict.smt_divergence:
        smt_l=(f'🔀 SMT: {ict.smt_pair} '
               f'({ict.smt_direction})\n')

    # MMC block
    mmc_b=''
    if mmc and mmc.nv and mmc.nv.valid:
        nv=mmc.nv
        nv_type_label=('🟢 Positive NV (Bullish)'
                       if nv.nv_type=='positive'
                       else '🔴 Negative NV (Bearish)')
        cont_label=('Continuation' if nv.continuation
                    else 'Reversal')
        mmc_b=(
            f'━━━━━━━━━━━━━━━━━━━━━━\n'
            f'📐 <b>MMC NEEDED VOLUME</b>\n'
            f'  Type    : {nv_type_label}\n'
            f'  Quality : {NV_QUALITY_EMOJI.get(nv.quality,"")}\n'
            f'  Signal  : {cont_label}\n'
            f'  Zone    : {nv.zone_low:.5f}–{nv.zone_high:.5f}\n'
            f'  Size    : {nv.size_vs_atr:.1f}x ATR\n'
            f'  IFC     : {"✅ Confirmed" if nv.ifc_confirmed else "❌ Missing"}\n'
            f'  Location: {"⭐⭐ Major S/R" if nv.location_score>=2 else "⭐ Near S/R" if nv.location_score>=1 else "⚪ Random"}\n'
            f'  S/R Int : {"✅" if nv.sr_interchange else "❌"}\n'
            f'  Fragment: {"⚠️ Yes" if nv.is_fragmented else "✅ No"}\n'
        )
    elif mmc:
        nv_status=mmc.nv.reason if mmc.nv else 'Not detected'
        mmc_b=(f'━━━━━━━━━━━━━━━━━━━━━━\n'
               f'📐 <b>MMC NV:</b> ❌ {nv_status}\n')

    # MMC quality indicators
    mmcq_b=''
    if mmc:
        fo=('✅ Fakeout confirmed'
            if mmc.fakeout_detected
            else f'Low probability ({mmc.fakeout_probability})')
        mmcq_b=(
            f'━━━━━━━━━━━━━━━━━━━━━━\n'
            f'🧠 <b>MMC Quality</b>\n'
            f'  Candle Nature: {mmc.candle_nature_score}/3\n'
            f'  Fakeout: {fo}\n'
            f'  99% Filter: {"✅ PASS" if mmc.zone_passes_99pct else "❌ FAIL"}\n'
            f'  Structure Rep: {"✅" if mmc.structure_repetition else "❌"}\n'
            f'  Insider S&D: {"✅" if mmc.insider_sd_zone else "❌"}\n'
        )

    # Position size
    ps_b=''
    if account_balance>0:
        ps=calc_position_size(account_balance,1.0,
                               sig.entry_high,sig.stop_loss,symbol)
        if ps:
            ps_b=(f'━━━━━━━━━━━━━━━━━━━━━━\n'
                  f'💰 <b>Position (1% risk)</b>\n'
                  f'  Risk : ${ps["risk_amount"]}\n'
                  f'  Lots : {ps["lot_size"]}\n')

    return (
        f'{badge}'
        f'{em} <b>{sig.direction.value} — {symbol}'
        f'{premium_badge}</b>\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'🕐 {mkt}\n'
        f'{macro_l}'
        f'{po3_l}'
        f'{ohlc_l}'
        f'{judas_l}'
        f'📦 Setup : {sig.block_type}\n'
        f'📈 Trend : {sig.trend.upper()}\n'
        f'{opens_b}'
        f'{fp_l}'
        f'{sd_l}'
        f'{smt_l}'
        f'{mmc_b}'
        f'{mmcq_b}'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'📍 <b>Entry:</b> {sig.entry_low} – {sig.entry_high}\n'
        f'🛑 <b>SL:</b>    {sig.stop_loss}\n\n'
        f'🎯 <b>T1:</b> {sig.target_1}  (1.5 SD)\n'
        f'   └ Close 50% + Move SL to entry\n'
        f'🎯 <b>T2:</b> {sig.target_2}  (2.5 SD)\n'
        f'   └ Close 25% + Move SL to T1\n'
        f'🎯 <b>T3:</b> {sig.target_3}  (4.5 SD)\n'
        f'   └ Close 25% — let run\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'📊 RR: 1:{sig.rr_ratio}  '
        f'(min {cfg["min_rr"]})\n'
        f'⭐ Score: {sig.confluence_score}/10 {stars}\n'
        f'{ps_b}'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'<b>Confluences:</b>\n{rsns}\n'
        + (f'\n<b>Warnings:</b>\n{warns}\n' if warns else '') +
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'📋 <b>VERIFY ON TRADINGVIEW:</b>\n'
        f'  1H: {vdir} structure\n'
        f'  1H: OB/FVG @ entry zone\n'
        f'  1H: Price in {loc} zone\n'
        f'  5M: {sw}\n'
        f'  5M: IFC candle at NV zone\n'
        f'  5M: Body-to-body rejection\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'⏰ {sig.timestamp}'
    )


# ═══════════════════════════════════════════════
#  INSTANCES
# ═══════════════════════════════════════════════
fetcher=PublicDataFetcher()
engine =MasterEngine()


def scan_one_pair(symbol:str) -> Optional[SMCSignal]:
    try:
        cfg    =Config.for_symbol(symbol)
        smt_sym=SMT_PAIRS.get(symbol)
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=6) as ex:
            fh=ex.submit(fetcher.fetch,symbol,Config.HTF,200)
            fl=ex.submit(fetcher.fetch,symbol,Config.LTF,150)
            fd=ex.submit(fetcher.fetch,symbol,'1d',30)
            fw=ex.submit(fetcher.fetch,symbol,'1w',20)
            fs=(ex.submit(fetcher.fetch,smt_sym,Config.HTF,100)
                if smt_sym else None)
            fo=(ex.submit(fetcher.get_oil_bias)
                if cfg.get('check_oil') else None)
            htf   =fh.result(); ltf   =fl.result()
            daily =fd.result(); weekly=fw.result()
            smt_c =(fs.result() if fs else None)
            oil   =(fo.result() if fo else 'neutral')
        if not htf: return None
        return engine.analyse(symbol,htf,ltf,daily,
                               weekly,smt_c,oil)
    except Exception as e:
        log.error(f'scan_one_pair {symbol}: {e}'); return None


# ═══════════════════════════════════════════════
#  AUTO SCANNER
# ═══════════════════════════════════════════════
async def auto_scanner(app):
    await asyncio.sleep(60)
    while True:
        if not ScannerState.auto_alerts_on:
            await asyncio.sleep(60); continue
        if not ScannerState.daily_loss_ok():
            log.info('Daily limit reached — paused')
            await asyncio.sleep(30*60); continue
        if ICTTimeEngine.is_news_time():
            log.info('News time — skipping')
            await asyncio.sleep(10*60); continue

        kz   =ICTTimeEngine.get_kill_zone()
        pairs=(CRYPTO_PAIRS if kz=='dead'
               else ALL_SCAN_PAIRS)
        log.info(f'Auto scan [{kz}] — {len(pairs)} pairs')
        found=0

        for symbol in pairs:
            try:
                if not ScannerState.can_alert(symbol): continue
                is_c=fetcher.detect_type(symbol)=='crypto'
                if not is_c and kz=='dead': continue
                sig=scan_one_pair(symbol)
                if sig and sig.is_alert_worthy():
                    ScannerState.mark_alerted(symbol)
                    msg=format_signal(sig,symbol,
                                      is_alert=True,
                                      is_crypto=is_c)
                    await app.bot.send_message(
                        chat_id=Config.TELEGRAM_CHAT_ID,
                        text=msg,parse_mode='HTML')
                    found+=1
                    log.info(f'ALERT {symbol} '
                             f'Score:{sig.confluence_score} '
                             f'RR:{sig.rr_ratio} '
                             f'NV:{sig.is_nv_premium}')
                    await asyncio.sleep(2)
            except Exception as e:
                log.error(f'Scanner {symbol}: {e}')
            await asyncio.sleep(1)

        log.info(f'Scan done — {found} alerts sent')
        await asyncio.sleep(Config.SCAN_INTERVAL_MINS*60)


# ═══════════════════════════════════════════════
#  HELP TEXT
# ═══════════════════════════════════════════════
HELP_TEXT="""
🤖 <b>ICT + SMC + MMC Bot v5.0</b>
━━━━━━━━━━━━━━━━━━━━━━

<b>SMC Concepts:</b>
  📦 Order Blocks + FVG
  📈 BOS + CHOCH
  💧 Liquidity Sweeps
  📊 Premium / Discount

<b>ICT Concepts:</b>
  🕐 Midnight Open (NYMO)
  🎯 FPFVG Detection
  📐 SD Zones 1.5 / 2.5 / 4.5
  ⚡ Macro Windows
  🔴 8:30 + 9:30 Manipulation
  📖 PO3 Phase Detection
  📊 OHLC Day Model
  🔀 SMT Divergence
  🎭 Judas Swing

<b>MMC Concepts:</b>
  🧠 Fakeout Theory
  📏 99% Zone Filter
  🕯 Candle Nature + CCP
  📉 Volume Confirmation
  🔁 Structure Repetition
  🏦 Insider S&D Zones

<b>Needed Volume (NV):</b>
  📐 Parallel Channel Detection
  📉 Structural Failure Zones
  🏦 IFC Candle Confirmation
  ✅ Body-over-Wick Analysis
  📍 S/R + S/R Interchange
  ⚡ NV Quality Grading

<b>Commands:</b>
  /on     — Auto alerts ON
  /off    — Auto alerts OFF
  /scan   — Manual full scan
  /pairs  — Show all pairs
  /status — Bot health
  /size   — Position calculator
  /help   — This message

<b>Just send a symbol:</b>
  XAUUSD  BTCUSDT  EURUSD  GBPJPY

<b>Score System:</b>
  8–10 = ⭐⭐⭐ Auto alert
  6–7  = ⭐⭐  Valid
  0–5  = ❌   No trade
  NV PREMIUM = 🏆 Best setup
"""


# ═══════════════════════════════════════════════
#  HANDLERS
# ═══════════════════════════════════════════════
async def start_handler(u,c):
    await u.message.reply_text(HELP_TEXT,parse_mode='HTML')

async def help_handler(u,c):
    await u.message.reply_text(HELP_TEXT,parse_mode='HTML')

async def on_handler(u,c):
    ScannerState.auto_alerts_on=True
    await u.message.reply_text(
        f'✅ <b>Auto Alerts ON</b>\n\n'
        f'Crypto : {len(CRYPTO_PAIRS)} pairs\n'
        f'Forex  : {len(FOREX_PAIRS_SCAN)} pairs\n'
        f'Score  : {Config.ALERT_MIN_SCORE}+ | '
        f'RR {Config.ALERT_MIN_RR}+\n'
        f'Engines: SMC + ICT + MMC + NV',
        parse_mode='HTML')

async def off_handler(u,c):
    ScannerState.auto_alerts_on=False
    await u.message.reply_text(
        '🔕 <b>Auto Alerts OFF</b>',parse_mode='HTML')

async def status_handler(u,c):
    kz   =ICTTimeEngine.get_kill_zone()
    label=ICTTimeEngine.session_label()
    ma,ml=ICTTimeEngine.is_macro_window()
    ny_t =ICTTimeEngine.now_ny().strftime('%H:%M')
    news ='⚠️ YES' if ICTTimeEngine.is_news_time() else '✅ Clear'
    state='✅ ON'  if ScannerState.auto_alerts_on  else '🔕 OFF'
    loss ='✅ OK'  if ScannerState.daily_loss_ok() else '🛑 STOPPED'
    await u.message.reply_text(
        f'📊 <b>Bot v5.0 Status</b>\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'Alerts     : {state}\n'
        f'Daily P&L  : {loss}\n'
        f'NY Time    : {ny_t}\n'
        f'Kill Zone  : {label}\n'
        f'Macro      : {"✅ "+ml if ma else "❌"}\n'
        f'News       : {news}\n'
        f'8:30       : {"✅" if ICTTimeEngine.is_830_passed() else "❌"}\n'
        f'9:30       : {"✅" if ICTTimeEngine.is_930_passed() else "❌"}\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'Crypto     : {len(CRYPTO_PAIRS)} pairs\n'
        f'Forex      : {len(FOREX_PAIRS_SCAN)} pairs\n'
        f'Total      : {len(ALL_SCAN_PAIRS)}\n'
        f'Interval   : {Config.SCAN_INTERVAL_MINS}min\n'
        f'Alert Score: {Config.ALERT_MIN_SCORE}+\n'
        f'Alert RR   : {Config.ALERT_MIN_RR}+\n'
        f'Alerted    : {len(ScannerState.last_alerted)}\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'Engines: SMC + ICT + MMC + NV\n'
        f'⏰ {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}',
        parse_mode='HTML')

async def pairs_handler(u,c):
    def fmt(lst,cols=4):
        rows=[]
        for i in range(0,len(lst),cols):
            rows.append('  '+'  '.join(lst[i:i+cols]))
        return '\n'.join(rows)
    smt_list='\n'.join(f'  {k} ↔ {v}'
                       for k,v in list(SMT_PAIRS.items())[:6])
    forex_=[p for p in FOREX_PAIRS_SCAN
            if p not in ['XAUUSD','XAGUSD','USOIL','UKOIL']]
    await u.message.reply_text(
        f'👁 <b>Scanning {len(ALL_SCAN_PAIRS)} Pairs</b>\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n\n'
        f'🔵 <b>Crypto ({len(CRYPTO_PAIRS)}):</b>\n'
        f'{fmt(CRYPTO_PAIRS)}\n\n'
        f'📈 <b>Forex ({len(forex_)}):</b>\n'
        f'{fmt(forex_)}\n\n'
        f'🥇 <b>Metals:</b>\n  XAUUSD  XAGUSD\n\n'
        f'🛢 <b>Oil:</b>\n  USOIL  UKOIL\n\n'
        f'<b>SMT Pairs:</b>\n{smt_list}',
        parse_mode='HTML')

async def scan_handler(u,c):
    kz   =ICTTimeEngine.get_kill_zone()
    label=ICTTimeEngine.session_label()
    pairs=(CRYPTO_PAIRS if kz=='dead' else ALL_SCAN_PAIRS)
    note =('crypto only' if kz=='dead' else 'full scan')
    await u.message.reply_text(
        f'🔍 <b>Manual Scan ({note})</b>\n'
        f'{len(pairs)} pairs — {label}\n'
        f'Engines: SMC + ICT + MMC + NV\n'
        f'⏳ 3-5 minutes...',
        parse_mode='HTML')
    found=[]
    for symbol in pairs:
        try:
            is_c=fetcher.detect_type(symbol)=='crypto'
            sig =scan_one_pair(symbol)
            if sig and sig.is_alert_worthy():
                found.append((sig,is_c))
        except Exception as e:
            log.error(f'Scan {symbol}: {e}')
    if not found:
        await u.message.reply_text(
            f'🔍 <b>No Setups Found</b>\n\n'
            f'Scanned : {len(pairs)} pairs\n'
            f'Need    : Score {Config.ALERT_MIN_SCORE}+ '
            f'RR {Config.ALERT_MIN_RR}+\n\n'
            f'💡 Best: London/NY kill zones\n'
            f'💡 Best: Macro windows xx:50–xx:10\n'
            f'💡 NV Premium setups = rare but powerful\n'
            f'⏰ {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}',
            parse_mode='HTML')
        return
    nv_count=sum(1 for s,_ in found if s.is_nv_premium)
    await u.message.reply_text(
        f'✅ <b>{len(found)} Setup(s) Found!</b>\n'
        f'🏆 NV Premium: {nv_count}',
        parse_mode='HTML')
    for sig,is_c in found:
        msg=format_signal(sig,sig.symbol,
                          is_alert=False,is_crypto=is_c)
        await u.message.reply_text(msg,parse_mode='HTML')
        await asyncio.sleep(1)

async def size_handler(u,c):
    await u.message.reply_text(
        '💰 <b>Position Calculator</b>\n\n'
        'Format:\n'
        '<code>/size BALANCE SYMBOL ENTRY SL</code>\n\n'
        'Example:\n'
        '<code>/size 10000 XAUUSD 3285 3298</code>',
        parse_mode='HTML')

async def size_calc_handler(u,c):
    try:
        parts=u.message.text.split()
        if len(parts)<5:
            await u.message.reply_text(
                '❌ Format: /size BALANCE SYMBOL ENTRY SL')
            return
        bal=float(parts[1]); sym=fetcher.normalize(parts[2])
        ent=float(parts[3]); sl =float(parts[4])
        ps=calc_position_size(bal,1.0,ent,sl,sym)
        if not ps:
            await u.message.reply_text('❌ Invalid'); return
        await u.message.reply_text(
            f'💰 <b>{sym} Position</b>\n'
            f'━━━━━━━━━━━━━━━━━━━━━━\n'
            f'Account : ${bal:,.2f}\n'
            f'Entry   : {ent}\n'
            f'SL      : {sl}\n'
            f'SL Dist : {ps["sl_distance"]}\n'
            f'━━━━━━━━━━━━━━━━━━━━━━\n'
            f'<b>1% Risk:</b>\n'
            f'  Amount: ${ps["risk_amount"]}\n'
            f'  Lots  : {ps["lot_size"]}\n\n'
            f'<b>0.5% Risk:</b>\n'
            f'  Amount: ${round(bal*0.005,2)}\n'
            f'━━━━━━━━━━━━━━━━━━━━━━\n'
            f'⚠️ Max 3 trades/day\n'
            f'⚠️ Stop at 2% daily loss',
            parse_mode='HTML')
    except Exception as e:
        await u.message.reply_text(f'❌ {e}')

async def symbol_handler(u:Update,
                          c:ContextTypes.DEFAULT_TYPE):
    raw   =u.message.text.strip()
    symbol=fetcher.normalize(raw)
    is_c  =fetcher.detect_type(symbol)=='crypto'
    loading=await u.message.reply_text(
        f'🔍 Analysing <b>{symbol}</b>\n'
        f'Engines: SMC + ICT + MMC + Needed Volume\n'
        f'⏳ Please wait...',
        parse_mode='HTML')
    try:
        start=time.time()
        cfg  =Config.for_symbol(symbol)
        smt_s=SMT_PAIRS.get(symbol)
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=6) as ex:
            fh=ex.submit(fetcher.fetch,symbol,Config.HTF,200)
            fl=ex.submit(fetcher.fetch,symbol,Config.LTF,150)
            fd=ex.submit(fetcher.fetch,symbol,'1d',30)
            fw=ex.submit(fetcher.fetch,symbol,'1w',20)
            fs=(ex.submit(fetcher.fetch,smt_s,Config.HTF,100)
                if smt_s else None)
            fo=(ex.submit(fetcher.get_oil_bias)
                if cfg.get('check_oil') else None)
            htf   =fh.result(); ltf   =fl.result()
            daily =fd.result(); weekly=fw.result()
            smt_c =(fs.result() if fs else None)
            oil   =(fo.result() if fo else 'neutral')
        if not htf:
            await loading.edit_text(
                f'❌ No data: {symbol}',
                parse_mode='HTML'); return
        sig    =engine.analyse(symbol,htf,ltf,daily,
                                weekly,smt_c,oil)
        elapsed=round(time.time()-start,1)
        msg    =format_signal(sig,symbol,
                              is_alert=False,is_crypto=is_c)
        msg   +=f'\n⚡ <i>{elapsed}s</i>'
        await loading.edit_text(msg,parse_mode='HTML')
        log.info(f'{symbol}: {sig.direction.value} '
                 f'Score:{sig.confluence_score} '
                 f'RR:{sig.rr_ratio} '
                 f'NV:{sig.is_nv_premium}')
    except Exception as e:
        log.error(f'{symbol}: {e}')
        await loading.edit_text(
            f'❌ Error: {str(e)[:200]}',parse_mode='HTML')


# ═══════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════
def main():
    token=Config.TELEGRAM_BOT_TOKEN
    if not token:
        log.error('TELEGRAM_BOT_TOKEN not set'); return

    log.info('━'*45)
    log.info('ICT + SMC + MMC Signal Bot v5.0')
    log.info('SMC: OB FVG BOS CHOCH Liquidity')
    log.info('ICT: NYMO FPFVG SD Macros PO3 SMT')
    log.info('MMC: Fakeout 99% CCP Structure')
    log.info('NV : Channel IFC Body S/R Quality')
    log.info(f'Pairs: {len(ALL_SCAN_PAIRS)} total')
    log.info(f'Score: manual {Config.MIN_CONFLUENCE_SCORE}+ '
             f'alert {Config.ALERT_MIN_SCORE}+')
    log.info('━'*45)

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
            log.info('Auto scanner started ✅')
        else:
            log.warning('No CHAT_ID — scanner disabled')

    app.post_init=post_init
    log.info('Bot v5.0 running 🚀')
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__=='__main__':
    main()
