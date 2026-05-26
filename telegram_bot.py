"""
ICT + SMC + MMC + Needed Volume Signal Bot — v5.0 FIXED
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FIXES in this version:
  - Alert score threshold: 8 → 6
  - Min confluence score:  6 → 4
  - Min RR ratio:        1.8 → 1.5
  - LTF CHOCH: hard block removed (soft score only)
  - Scans ALL pairs always (not just crypto in dead session)
  - Trend detection: needs 3 swings not 4
  - Added /debug command
  - Many internal thresholds relaxed
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import os, time, asyncio, logging, requests, concurrent.futures
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
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s | %(levelname)s | %(message)s')
log = logging.getLogger('ICT_MMC_v5')
NY_TZ = ZoneInfo('America/New_York')

# ═══════════════════════════════════════════════════════
#  PAIRS
# ═══════════════════════════════════════════════════════
CRYPTO_PAIRS = [
    'BTCUSDT','ETHUSDT','SOLUSDT','BNBUSDT','XRPUSDT',
    'ADAUSDT','DOGEUSDT','DOTUSDT','AVAXUSDT','LINKUSDT','LTCUSDT',
]
FOREX_PAIRS_SCAN = [
    'EURUSD','GBPUSD','USDJPY','AUDUSD','USDCAD','USDCHF','NZDUSD',
    'EURJPY','EURGBP','EURAUD','EURCAD','EURCHF','GBPJPY','GBPAUD',
    'GBPCAD','GBPCHF','GBPNZD','AUDJPY','AUDCAD','AUDCHF','AUDNZD',
    'NZDJPY','NZDCAD','NZDCHF','CADJPY','CADCHF','CHFJPY',
    'XAUUSD','XAGUSD','USOIL','UKOIL',
]
ALL_SCAN_PAIRS = CRYPTO_PAIRS + FOREX_PAIRS_SCAN

SMT_PAIRS = {
    'XAUUSD':'XAGUSD','XAGUSD':'XAUUSD',
    'EURUSD':'GBPUSD','GBPUSD':'EURUSD',
    'AUDUSD':'NZDUSD','NZDUSD':'AUDUSD',
    'BTCUSDT':'ETHUSDT','ETHUSDT':'BTCUSDT',
    'USDCAD':'USDCHF','USDCHF':'USDCAD',
}

# ═══════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════
class Config:
    TELEGRAM_BOT_TOKEN    = os.getenv('TELEGRAM_BOT_TOKEN', '')
    TELEGRAM_CHAT_ID      = os.getenv('TELEGRAM_CHAT_ID', '')
    HTF                   = '1h'
    LTF                   = '5m'
    CANDLE_LIMIT          = 200
    SWING_LOOKBACK        = 5
    MIN_CONFLUENCE_SCORE  = 4      # was 6
    MIN_RR_RATIO          = 1.5    # was 1.8
    SL_BUFFER_PCT         = 0.002
    DISPLACEMENT_MULT     = 0.8    # was 1.0
    ALERT_MIN_SCORE       = 6      # was 8
    ALERT_MIN_RR          = 1.8    # was 2.0
    ALERT_COOLDOWN_HOURS  = 4
    SCAN_INTERVAL_MINS    = 30
    SCAN_ALL_ALWAYS       = True   # always scan all pairs

    SYMBOL_SETTINGS: Dict = {
        'XAUUSD': {'min_score':4,'min_rr':1.5,'sl_buffer_pct':0.005},
        'GBPUSD': {'min_score':4,'min_rr':1.5,'sl_buffer_pct':0.002},
        'USDCAD': {'min_score':4,'min_rr':1.5,'sl_buffer_pct':0.002,'check_oil':True},
    }

    @classmethod
    def for_symbol(cls, s: str) -> Dict:
        d = {'min_score':cls.MIN_CONFLUENCE_SCORE,
             'min_rr':cls.MIN_RR_RATIO,
             'sl_buffer_pct':cls.SL_BUFFER_PCT,
             'check_oil':False}
        return {**d, **cls.SYMBOL_SETTINGS.get(s, {})}

# ═══════════════════════════════════════════════════════
#  STATE
# ═══════════════════════════════════════════════════════
class ScannerState:
    auto_alerts_on    = True
    last_alerted: Dict[str, datetime] = {}
    daily_pnl_pct     = 0.0
    daily_reset_date  = ''

    @classmethod
    def can_alert(cls, sym: str) -> bool:
        if sym not in cls.last_alerted: return True
        return datetime.now(timezone.utc) - cls.last_alerted[sym] >= timedelta(hours=Config.ALERT_COOLDOWN_HOURS)

    @classmethod
    def mark_alerted(cls, sym: str):
        cls.last_alerted[sym] = datetime.now(timezone.utc)

    @classmethod
    def check_daily_reset(cls):
        today = datetime.now(NY_TZ).strftime('%Y-%m-%d')
        if cls.daily_reset_date != today:
            cls.daily_pnl_pct = 0.0
            cls.daily_reset_date = today

    @classmethod
    def daily_loss_ok(cls) -> bool:
        cls.check_daily_reset()
        return cls.daily_pnl_pct > -3.0

# ═══════════════════════════════════════════════════════
#  ENUMS & DATACLASSES
# ═══════════════════════════════════════════════════════
class SignalDirection(Enum):
    LONG = 'LONG'; SHORT = 'SHORT'; NO_TRADE = 'NO TRADE'

class PO3Phase(Enum):
    ACCUMULATION = 'Accumulation'; MANIPULATION = 'Manipulation'
    DISTRIBUTION = 'Distribution'; UNKNOWN = 'Unknown'

class NVQuality(Enum):
    PREMIUM = 'PREMIUM'; STANDARD = 'STANDARD'
    WEAK = 'WEAK'; NONE = 'NONE'

@dataclass
class Candle:
    time: str; open: float; high: float; low: float; close: float; volume: float = 0.0
    def body_high(self):   return max(self.open, self.close)
    def body_low(self):    return min(self.open, self.close)
    def is_bullish(self):  return self.close > self.open
    def is_bearish(self):  return self.close < self.open
    def body_size(self):   return abs(self.close - self.open)
    def range_size(self):  return self.high - self.low
    def wick_upper(self):  return self.high - self.body_high()
    def wick_lower(self):  return self.body_low() - self.low
    def has_strong_body(self, atr: float): return self.body_size() >= atr * 1.2
    def has_rejection_wick(self, atr: float):
        return self.wick_upper() >= atr * 0.6 or self.wick_lower() >= atr * 0.6

@dataclass
class SwingPoint:
    type: str; price: float; index: int; broken: bool = False

@dataclass
class OrderBlock:
    direction: str; zone_low: float; zone_high: float
    midpoint: float; index: int; status: str = 'fresh'

@dataclass
class FVG:
    direction: str; zone_low: float; zone_high: float
    midpoint: float; index: int; status: str = 'fresh'; is_fpfvg: bool = False

@dataclass
class LiquidityPool:
    type: str; price: float; touches: int; swept: bool = False

@dataclass
class NeededVolumeResult:
    valid: bool; nv_type: str; quality: NVQuality
    zone_low: float; zone_high: float
    expected_price: float; actual_price: float
    ifc_confirmed: bool; ifc_size: float; ifc_direction: str
    location_score: int; size_vs_atr: float
    is_fragmented: bool; sr_interchange: bool
    reason: str; continuation: bool

@dataclass
class MMCContext:
    candle_nature_score: int = 0
    strong_body_at_zone: bool = False
    rejection_wick_at_zone: bool = False
    volume_confirms: bool = False
    fakeout_detected: bool = False
    fakeout_probability: str = 'low'
    zone_passes_99pct: bool = False
    structure_repetition: bool = False
    insider_sd_zone: bool = False
    nv: Optional[NeededVolumeResult] = None
    nv_score: int = 0
    mmcScore: int = 0
    reasons: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

@dataclass
class ICTContext:
    kill_zone: str = 'dead'; macro_active: bool = False; macro_label: str = ''
    is_830_passed: bool = False; is_930_passed: bool = False
    midnight_open: float = 0.0; open_830: float = 0.0; open_930: float = 0.0
    above_midnight: bool = False; above_830: bool = False; above_930: bool = False
    premium_discount: str = 'neutral'
    fpfvg: Optional[FVG] = None
    sd_15: float = 0.0; sd_25: float = 0.0; sd_45: float = 0.0; at_sd_zone: str = ''
    buy_pools: List[LiquidityPool] = field(default_factory=list)
    sell_pools: List[LiquidityPool] = field(default_factory=list)
    liquidity_swept: bool = False; swept_side: str = ''
    po3_phase: PO3Phase = PO3Phase.UNKNOWN; ohlc_model: str = ''
    is_expansion_day: bool = False
    judas_swing: bool = False; judas_direction: str = ''
    manipulation_830: bool = False; manipulation_830_dir: str = ''
    confirmed_930: bool = False
    smt_divergence: bool = False; smt_direction: str = ''; smt_pair: str = ''
    narrative_score: int = 0
    reasons: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

@dataclass
class SMCSignal:
    direction: SignalDirection; symbol: str
    entry_low: float; entry_high: float; stop_loss: float
    target_1: float; target_2: float; target_3: float
    rr_ratio: float; confluence_score: int
    trend: str; block_type: str
    ict: ICTContext = field(default_factory=ICTContext)
    mmc: MMCContext = field(default_factory=MMCContext)
    session: str = ''
    reasons: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    timestamp: str = ''; is_nv_premium: bool = False

    def is_valid(self) -> bool:
        cfg = Config.for_symbol(self.symbol)
        return (self.direction != SignalDirection.NO_TRADE
                and self.rr_ratio >= cfg['min_rr']
                and self.confluence_score >= cfg['min_score'])

    def is_alert_worthy(self) -> bool:
        return (self.is_valid()
                and self.confluence_score >= Config.ALERT_MIN_SCORE
                and self.rr_ratio >= Config.ALERT_MIN_RR)

# ═══════════════════════════════════════════════════════
#  ICT TIME ENGINE
# ═══════════════════════════════════════════════════════
class ICTTimeEngine:
    MACRO_WINDOWS = [(2,50),(3,10),(3,50),(4,10),(9,50),(10,10),(10,50),(11,10)]
    KILL_ZONES = {'london_open':(2,5),'london':(5,8),'pre_ny':(7,8),
                  'ny_open':(8,11),'london_close':(10,12)}
    NEWS_TIMES_UTC = [(8,30),(9,30),(12,30),(13,30),(14,0),(14,30),(18,0),(18,30)]
    NEWS_BUFFER_MINS = 30

    @classmethod
    def now_ny(cls): return datetime.now(NY_TZ)

    @classmethod
    def utc_to_ny(cls, dt): return dt.astimezone(NY_TZ)

    @classmethod
    def is_macro_window(cls) -> Tuple[bool, str]:
        now = cls.now_ny(); h, m = now.hour, now.minute; curr = h*60+m
        if curr >= h*60+50 or curr <= h*60+10:
            return True, f'{h:02d}:50–{(h+1)%24:02d}:10'
        pairs = [(cls.MACRO_WINDOWS[i], cls.MACRO_WINDOWS[i+1])
                 for i in range(0, len(cls.MACRO_WINDOWS), 2)]
        for (sh,sm),(eh,em) in pairs:
            if sh*60+sm <= curr <= eh*60+em:
                return True, f'{sh:02d}:{sm:02d}–{eh:02d}:{em:02d}'
        return False, ''

    @classmethod
    def get_kill_zone(cls) -> str:
        h = cls.now_ny().hour
        for n,(s,e) in cls.KILL_ZONES.items():
            if s <= h < e: return n
        return 'dead'

    @classmethod
    def is_news_time(cls) -> bool:
        now = datetime.now(timezone.utc); curr = now.hour*60+now.minute
        return any(abs(curr-nh*60-nm) <= cls.NEWS_BUFFER_MINS for nh,nm in cls.NEWS_TIMES_UTC)

    @classmethod
    def is_830_passed(cls):
        n = cls.now_ny(); return n >= n.replace(hour=8,minute=30,second=0,microsecond=0)

    @classmethod
    def is_930_passed(cls):
        n = cls.now_ny(); return n >= n.replace(hour=9,minute=30,second=0,microsecond=0)

    @classmethod
    def session_label(cls) -> str:
        labels = {'london_open':'🟢 London Open','london':'🟡 London',
                  'pre_ny':'🟠 Pre-NY','ny_open':'🟢 NY Open ⭐',
                  'london_close':'🟡 London Close','dead':'🔴 Dead Session'}
        return labels.get(cls.get_kill_zone(), 'Unknown')

# ═══════════════════════════════════════════════════════
#  DATA FETCHER
# ═══════════════════════════════════════════════════════
class PublicDataFetcher:
    BINANCE_TF   = {'1m':'1m','3m':'3m','5m':'5m','15m':'15m','30m':'30m',
                    '1h':'1h','4h':'4h','1d':'1d','1w':'1w','1M':'1M'}
    YAHOO_INT    = {'5m':'5m','15m':'15m','30m':'30m','1h':'1h',
                    '4h':'1h','1d':'1d','1w':'1wk','1M':'1mo'}
    YAHOO_RANGE  = {'5m':'7d','15m':'60d','30m':'60d','1h':'2y',
                    '4h':'2y','1d':'5y','1w':'10y','1M':'10y'}
    CG_MAP       = {'BTCUSDT':'bitcoin','ETHUSDT':'ethereum','SOLUSDT':'solana',
                    'BNBUSDT':'binancecoin','XRPUSDT':'ripple','ADAUSDT':'cardano',
                    'DOGEUSDT':'dogecoin','DOTUSDT':'polkadot','AVAXUSDT':'avalanche-2',
                    'LINKUSDT':'chainlink','LTCUSDT':'litecoin'}
    CG_DAYS      = {'5m':'1','15m':'1','30m':'2','1h':'7','4h':'30','1d':'365'}
    YAHOO_SYM    = {'XAUUSD':'GC=F','XAGUSD':'SI=F','USOIL':'CL=F','UKOIL':'BZ=F',
                    'BTCUSDT':'BTC-USD','ETHUSDT':'ETH-USD','SOLUSDT':'SOL-USD',
                    'BNBUSDT':'BNB-USD','XRPUSDT':'XRP-USD','ADAUSDT':'ADA-USD',
                    'DOGEUSDT':'DOGE-USD','DOTUSDT':'DOT-USD','AVAXUSDT':'AVAX-USD',
                    'LINKUSDT':'LINK-USD','LTCUSDT':'LTC-USD'}
    ALL_FOREX    = ['EURUSD','GBPUSD','USDJPY','AUDUSD','USDCAD','USDCHF','NZDUSD',
                    'EURJPY','EURGBP','EURAUD','EURCAD','EURCHF','EURNZD','GBPJPY',
                    'GBPAUD','GBPCAD','GBPCHF','GBPNZD','AUDJPY','AUDCAD','AUDCHF',
                    'AUDNZD','NZDJPY','NZDCAD','NZDCHF','CADJPY','CADCHF','CHFJPY',
                    'XAUUSD','XAGUSD','USOIL','UKOIL']

    def detect_type(self, s: str) -> str:
        s = s.upper().replace('/','').replace('-','')
        return 'crypto' if any(s.endswith(e) for e in ['USDT','BTC','ETH','BNB','BUSD']) else 'forex'

    def normalize(self, s: str) -> str:
        return s.upper().replace('/','').replace('-','').replace(' ','')

    def fetch(self, symbol: str, tf: str, limit: int = None) -> List[Candle]:
        limit = limit or Config.CANDLE_LIMIT
        s = self.normalize(symbol)
        if self.detect_type(s) == 'crypto':
            return self._binance(s,tf,limit) or self._coingecko(s,tf,limit) or self._yahoo(s,tf,limit)
        return self._yahoo(s, tf, limit)

    def _binance(self, symbol, tf, limit):
        try:
            r = requests.get('https://api.binance.com/api/v3/klines',
                params={'symbol':symbol,'interval':self.BINANCE_TF.get(tf,'1h'),'limit':limit},timeout=10)
            if r.status_code != 200: return []
            return [Candle(time=str(datetime.fromtimestamp(row[0]/1000,tz=timezone.utc)),
                           open=float(row[1]),high=float(row[2]),low=float(row[3]),
                           close=float(row[4]),volume=float(row[5])) for row in r.json()]
        except Exception as e:
            log.error(f'Binance:{e}'); return []

    def _coingecko(self, symbol, tf, limit):
        try:
            cid = self.CG_MAP.get(symbol)
            if not cid: return []
            r = requests.get(f'https://api.coingecko.com/api/v3/coins/{cid}/ohlc'
                             f'?vs_currency=usd&days={self.CG_DAYS.get(tf,"7")}',
                             headers={'User-Agent':'Mozilla/5.0'}, timeout=15)
            if r.status_code != 200: return []
            return [Candle(time=str(datetime.fromtimestamp(row[0]/1000,tz=timezone.utc)),
                           open=float(row[1]),high=float(row[2]),low=float(row[3]),
                           close=float(row[4]),volume=0.0) for row in r.json()][-limit:]
        except Exception as e:
            log.error(f'CoinGecko:{e}'); return []

    def _yahoo(self, symbol, tf, limit):
        try:
            ys = self._to_yahoo(symbol)
            iv = self.YAHOO_INT.get(tf,'1d'); rg = self.YAHOO_RANGE.get(tf,'2y')
            hd = {'User-Agent':'Mozilla/5.0','Accept':'application/json'}
            for host in ['query1','query2']:
                try:
                    r = requests.get(f'https://{host}.finance.yahoo.com/v8/finance/chart/{ys}'
                                     f'?interval={iv}&range={rg}', headers=hd, timeout=15)
                    if r.status_code != 200: continue
                    res = r.json().get('chart',{}).get('result',[])
                    if not res: continue
                    res = res[0]; ts_ = res.get('timestamp',[])
                    q = res['indicators']['quote'][0]
                    vols = q.get('volume') or [0]*len(ts_)
                    candles = []
                    for i,ts in enumerate(ts_):
                        try:
                            o,h,l,c = q['open'][i],q['high'][i],q['low'][i],q['close'][i]
                            if None in (o,h,l,c): continue
                            candles.append(Candle(
                                time=str(datetime.fromtimestamp(ts,tz=timezone.utc)),
                                open=float(o),high=float(h),low=float(l),close=float(c),
                                volume=float(vols[i] or 0)))
                        except Exception: continue
                    if candles: return candles[-limit:]
                except Exception as e:
                    log.error(f'Yahoo {host}:{e}')
            return []
        except Exception as e:
            log.error(f'Yahoo {symbol}:{e}'); return []

    def _to_yahoo(self, symbol):
        if symbol in self.YAHOO_SYM: return self.YAHOO_SYM[symbol]
        if symbol in self.ALL_FOREX: return symbol[:3]+symbol[3:]+'=X'
        return symbol

    def get_oil_bias(self) -> str:
        try:
            c = self._yahoo('USOIL','1d',10)
            if len(c) < 5: return 'neutral'
            return 'bullish' if c[-1].close > c[-5].close else 'bearish'
        except Exception: return 'neutral'

# ═══════════════════════════════════════════════════════
#  MMC ENGINE
# ═══════════════════════════════════════════════════════
class MMCEngine:

    def score_candle_nature(self, candles, zl, zh, direction, atr):
        score = 0; reasons = []
        zc = [c for c in candles[-20:] if c.low <= zh and c.high >= zl]
        if not zc: return 0, []
        lz = zc[-1]
        if lz.has_strong_body(atr):
            if (direction=='bullish' and lz.is_bullish()) or (direction=='bearish' and lz.is_bearish()):
                score += 1; reasons.append('Strong body at zone')
        if lz.has_rejection_wick(atr):
            score += 1; reasons.append('Rejection wick at zone')
        if len(candles) >= 5:
            av = sum(c.volume for c in candles[-20:]) / max(len(candles[-20:]),1)
            if av > 0 and lz.volume >= av*1.3:
                score += 1; reasons.append('Volume confirms zone')
            elif av > 0 and lz.volume < av*0.7:
                reasons.append('Low volume at zone')
        return score, reasons

    def detect_fakeout(self, candles, direction, atr):
        if len(candles) < 15: return False, 'low'
        recent = candles[-10:]
        if direction == 'bullish':
            bd = sum(1 for c in recent[:5] if c.is_bearish() and c.body_size()>=atr*0.8)
            rv = sum(1 for c in recent[5:] if c.is_bullish() and c.body_size()>=atr*0.8)
            if bd >= 1 and rv >= 1: return True, 'bearish_fakeout'
        else:
            bo = sum(1 for c in recent[:5] if c.is_bullish() and c.body_size()>=atr*0.8)
            rv = sum(1 for c in recent[5:] if c.is_bearish() and c.body_size()>=atr*0.8)
            if bo >= 1 and rv >= 1: return True, 'bullish_fakeout'
        vw = all(c.body_size() < atr*0.8 for c in recent[-5:])
        return False, ('high' if vw else 'low')

    def zone_passes_99pct_filter(self, confluences: int) -> bool:
        return confluences >= 2   # relaxed from 3

    def detect_structure_repetition(self, candles, direction) -> bool:
        if len(candles) < 40: return False
        ranges = []
        for i in range(0, min(40, len(candles)-10), 10):
            seg = candles[-(i+10):-i] if i > 0 else candles[-10:]
            rng = max(c.high for c in seg) - min(c.low for c in seg)
            ranges.append(rng)
        if len(ranges) < 3: return False
        avg = sum(ranges)/len(ranges)
        var = sum(abs(r-avg) for r in ranges)/len(ranges)
        return (var/avg) < 0.5 if avg > 0 else False

    def is_straight_market(self, candles, lookback=25) -> bool:
        if len(candles) < lookback: return True   # default True = don't block
        seg = candles[-lookback:]
        highs = [c.body_high() for c in seg]; lows = [c.body_low() for c in seg]
        dists = [h-l for h,l in zip(highs, lows)]
        avg = sum(dists)/len(dists) if dists else 0
        if avg == 0: return True
        var = sum(abs(d-avg) for d in dists)/len(dists)
        return (var/avg) < 0.5   # relaxed from 0.35

    def detect_parallel_channel(self, candles, lookback=35):
        if len(candles) < lookback: return None
        seg = candles[-lookback:]
        bh = [c.body_high() for c in seg]; bl = [c.body_low() for c in seg]
        n = len(seg); xs = list(range(n))
        def lr(ys):
            mx=sum(xs)/n; my=sum(ys)/n
            num=sum((xs[i]-mx)*(ys[i]-my) for i in range(n))
            den=sum((xs[i]-mx)**2 for i in range(n))
            sl=num/den if den else 0; return sl, my-sl*mx
        us,ui = lr(bh); ls,li = lr(bl)
        avg_sl = (us+ls)/2
        direction = 'bullish' if avg_sl>0.0001 else 'bearish' if avg_sl<-0.0001 else 'neutral'
        ut = sum(1 for i,c in enumerate(seg)
                 if abs(c.body_high()-(us*i+ui)) < (max(bh)-min(bh)+1e-9)*0.08)
        lt = sum(1 for i,c in enumerate(seg)
                 if abs(c.body_low()-(ls*i+li)) < (max(bl)-min(bl)+1e-9)*0.08)
        strength = min((ut+lt)/8.0, 1.0)
        straight = self.is_straight_market(candles, lookback)

        class PC:
            def __init__(self):
                self.upper_slope=us; self.lower_slope=ls
                self.upper_intercept=ui; self.lower_intercept=li
                self.is_straight=straight; self.direction=direction; self.strength=strength
        return PC()

    def detect_needed_volume(self, candles, direction, atr, sr_levels) -> NeededVolumeResult:
        null = NeededVolumeResult(False,'',NVQuality.NONE,0,0,0,0,False,0,'',0,0,False,False,'',False)
        if len(candles) < 25:
            null.reason='Insufficient candles'; return null
        if not self.is_straight_market(candles):
            null.reason='Bending market'; return null
        ch = self.detect_parallel_channel(candles)
        if not ch or ch.strength < 0.10:   # relaxed from 0.15
            null.reason='No clean channel'; return null
        n = len(candles); nv_zone = None
        if direction == 'bearish':
            for i in range(n-3, max(n-20,5), -1):
                c = candles[i]; exp = ch.upper_slope*i + ch.upper_intercept
                gap = exp - c.body_high()
                if gap > atr*0.3 and c.is_bearish():   # relaxed from 0.5
                    nv_zone={'low':c.body_high(),'high':exp,'expected':exp,
                             'actual':c.body_high(),'index':i,'type':'negative'}; break
        else:
            for i in range(n-3, max(n-20,5), -1):
                c = candles[i]; exp = ch.lower_slope*i + ch.lower_intercept
                gap = c.body_low() - exp
                if gap > atr*0.3 and c.is_bullish():   # relaxed from 0.5
                    nv_zone={'low':exp,'high':c.body_low(),'expected':exp,
                             'actual':c.body_low(),'index':i,'type':'positive'}; break
        if not nv_zone:
            null.reason='No structural failure found'; return null
        nv_size = abs(nv_zone['high']-nv_zone['low']); sva = nv_size/atr if atr else 0
        ifc,ifc_sz,ifc_dir = self._find_ifc(candles, nv_zone, atr)
        if not ifc:
            null.reason='No IFC candle'; return null
        is_frag = self._check_fragmented(candles[-12:], atr)
        loc = self._score_location(nv_zone, sr_levels, atr)
        sri = self._check_sr_interchange(candles, nv_zone, atr)
        is_cont = (self._get_local_trend(candles[-15:]) == direction)
        if sva >= 2.5 and loc >= 2 and not is_frag and ifc_sz >= atr*1.5:
            qual = NVQuality.PREMIUM
        elif sva >= 1.0 and not is_frag:   # relaxed from 1.5
            qual = NVQuality.STANDARD
        else:
            qual = NVQuality.WEAK
        return NeededVolumeResult(True,nv_zone['type'],qual,
            round(nv_zone['low'],6),round(nv_zone['high'],6),
            round(nv_zone['expected'],6),round(nv_zone['actual'],6),
            True,round(ifc_sz,6),ifc_dir,loc,round(sva,2),is_frag,sri,'Valid NV',is_cont)

    def _find_ifc(self, candles, nv_zone, atr):
        recent = candles[-20:]
        bodies = [c.body_size() for c in recent]
        if not bodies: return None,0,''
        mb = max(bodies); ab = sum(bodies)/len(bodies) if bodies else 0
        if ab == 0: return None,0,''
        for c in reversed(recent):
            if c.body_size()>=mb*0.7 and c.body_size()>=ab*1.5:   # relaxed from 2.0
                near = c.low<=nv_zone['high']*1.003 and c.high>=nv_zone['low']*0.997
                if near:
                    return c, c.body_size(), ('bullish' if c.is_bullish() else 'bearish')
        return None,0,''

    def _check_fragmented(self, candles, atr) -> bool:
        if not candles: return False   # default False (not True)
        bodies = [c.body_size() for c in candles]
        avg = sum(bodies)/len(bodies) if bodies else 0
        small = sum(1 for b in bodies if b < avg*0.6)
        return small > len(candles)*0.8   # relaxed from 0.7

    def _score_location(self, nv_zone, sr_levels, atr) -> int:
        if not sr_levels: return 0
        mid = (nv_zone['low']+nv_zone['high'])/2
        for l in sr_levels:
            if abs(mid-l) <= atr*2.0: return 2
        for l in sr_levels:
            if abs(mid-l) <= atr*4.0: return 1
        return 0

    def _check_sr_interchange(self, candles, nv_zone, atr) -> bool:
        if len(candles) < 40: return False
        mid = (nv_zone['low']+nv_zone['high'])/2
        for c in candles[:-15][-20:]:
            if abs(mid-c.body_high()) <= atr*2.5: return True
            if abs(mid-c.body_low())  <= atr*2.5: return True
        return False

    def _get_local_trend(self, candles) -> str:
        if len(candles) < 5: return 'neutral'
        fh = candles[:len(candles)//2]; sh = candles[len(candles)//2:]
        fa = sum(c.close for c in fh)/len(fh); sa = sum(c.close for c in sh)/len(sh)
        if sa > fa*1.001: return 'bullish'
        if sa < fa*0.999: return 'bearish'
        return 'neutral'

    def find_insider_sd_zones(self, candles, direction, atr) -> bool:
        if len(candles) < 20: return False
        for i in range(5, min(30, len(candles)-5)):
            c = candles[-i]
            if direction=='bullish' and c.is_bullish() and c.body_size()>=atr*1.5:
                zh=c.body_high(); zl=c.body_low()
                for rc in candles[-i+1:]:
                    if rc.low<=zh and rc.high>=zl: return True
            elif direction=='bearish' and c.is_bearish() and c.body_size()>=atr*1.5:
                zh=c.body_high(); zl=c.body_low()
                for rc in candles[-i+1:]:
                    if rc.low<=zh and rc.high>=zl: return True
        return False

    def analyse_mmc(self, candles, ltf, direction, atr, zl, zh, sr_levels) -> MMCContext:
        ctx = MMCContext(); score = 0
        cn,cnr = self.score_candle_nature(candles, zl, zh, direction, atr)
        ctx.candle_nature_score = cn
        if cn >= 1:
            score += 1; ctx.reasons.append('Candle nature ✅')
        ctx.reasons.extend(cnr)
        fo,fop = self.detect_fakeout(candles, direction, atr)
        ctx.fakeout_detected=fo; ctx.fakeout_probability=fop
        if fo:
            score += 1; ctx.reasons.append('Fakeout confirmed ✅')
        elif fop == 'high':
            ctx.warnings.append('High fakeout probability')
        ctx.zone_passes_99pct = self.zone_passes_99pct_filter(cn+(1 if fo else 0))
        if ctx.zone_passes_99pct:
            score += 1; ctx.reasons.append('99% filter PASSED ✅')
        ctx.structure_repetition = self.detect_structure_repetition(candles, direction)
        if ctx.structure_repetition:
            score += 1; ctx.reasons.append('Structure repetition ✅')
        ctx.insider_sd_zone = self.find_insider_sd_zones(candles, direction, atr)
        if ctx.insider_sd_zone:
            score += 1; ctx.reasons.append('Insider S&D ✅')
        nv = self.detect_needed_volume(candles, direction, atr, sr_levels)
        ctx.nv = nv
        if nv.valid:
            np = 0
            if nv.quality==NVQuality.PREMIUM:  np=3; ctx.reasons.append('NV PREMIUM ⭐⭐⭐')
            elif nv.quality==NVQuality.STANDARD: np=2; ctx.reasons.append('NV STANDARD ⭐⭐')
            else:                               np=1; ctx.reasons.append('NV WEAK ⭐')
            if nv.ifc_confirmed:  np+=1; ctx.reasons.append('IFC confirmed ✅')
            if nv.location_score>=2: np+=1; ctx.reasons.append('NV at major S/R ✅')
            if nv.sr_interchange: np+=1; ctx.reasons.append('S/R interchange ✅')
            if nv.is_fragmented:  np-=1; ctx.warnings.append('Fragmented NV movement')
            ctx.nv_score=max(0,np); score+=ctx.nv_score
        else:
            ctx.warnings.append(f'NV: {nv.reason}')
        ctx.mmcScore = score
        return ctx

# ═══════════════════════════════════════════════════════
#  ICT ENGINE
# ═══════════════════════════════════════════════════════
class ICTEngine:

    def get_atr(self, candles, period=14) -> float:
        if len(candles) < period+1: return 0.0
        trs = [max(candles[i].high-candles[i].low,
                   abs(candles[i].high-candles[i-1].close),
                   abs(candles[i].low -candles[i-1].close))
               for i in range(1,len(candles))]
        return sum(trs[-period:])/period

    def detect_swings(self, candles, lb=None):
        lb = lb or Config.SWING_LOOKBACK; pts = []
        for i in range(lb, len(candles)-lb):
            c = candles[i]
            if all(c.high>candles[i-k].high and c.high>candles[i+k].high for k in range(1,lb+1)):
                pts.append(SwingPoint('swing_high',c.high,i))
            if all(c.low<candles[i-k].low and c.low<candles[i+k].low for k in range(1,lb+1)):
                pts.append(SwingPoint('swing_low',c.low,i))
        return sorted(pts, key=lambda x:x.index)

    def classify_structure(self, swings):
        result=[]; ph=pl=None
        for s in swings:
            if s.type in ['swing_high','HH','LH']:
                s.type='HH' if (ph is None or s.price>ph.price) else 'LH'; ph=s
            else:
                s.type='HL' if (pl is None or s.price>pl.price) else 'LL'; pl=s
            result.append(s)
        return result

    def get_trend(self, swings) -> str:
        if len(swings) < 3: return 'ranging'   # relaxed from 4
        last = [s.type for s in swings[-6:]]
        if last.count('HH')+last.count('HL') >= 3: return 'uptrend'   # relaxed from 4
        if last.count('LL')+last.count('LH') >= 3: return 'downtrend' # relaxed from 4
        return 'ranging'

    def trend_strength(self, swings) -> int:
        if len(swings) < 4: return 0
        last = [s.type for s in swings[-8:]]
        mx = max(last.count('HH')+last.count('HL'), last.count('LL')+last.count('LH'))
        return 3 if mx>=6 else 2 if mx>=4 else 1 if mx>=2 else 0

    def find_obs(self, candles, direction) -> List[OrderBlock]:
        obs=[]; ar=sum(c.range_size() for c in candles)/max(len(candles),1)
        thr=ar*Config.DISPLACEMENT_MULT
        for i in range(len(candles)-3):
            c=candles[i]
            if direction=='bullish' and not c.is_bearish(): continue
            if direction=='bearish' and not c.is_bullish(): continue
            disp=any((candles[j].is_bullish() if direction=='bullish' else candles[j].is_bearish())
                     and candles[j].body_size()>=thr for j in range(i+1,min(i+4,len(candles))))
            if not disp: continue
            obs.append(OrderBlock(direction=direction,zone_low=c.low,zone_high=c.high,
                                  midpoint=(c.low+c.high)/2,index=i))
        for ob in obs:
            for c in candles[ob.index+3:]:
                if c.low<=ob.zone_high and c.high>=ob.zone_low: ob.status='tapped'; break
        return obs

    def find_fvgs(self, candles, direction, mark_fp=False) -> List[FVG]:
        fvgs=[]; found=False
        for i in range(len(candles)-2):
            c1,c3=candles[i],candles[i+2]
            if direction=='bullish' and c1.high<c3.low:
                f=FVG('bullish',c1.high,c3.low,(c1.high+c3.low)/2,i+2)
                if mark_fp and not found: f.is_fpfvg=True; found=True
                fvgs.append(f)
            elif direction=='bearish' and c1.low>c3.high:
                f=FVG('bearish',c3.high,c1.low,(c3.high+c1.low)/2,i+2)
                if mark_fp and not found: f.is_fpfvg=True; found=True
                fvgs.append(f)
        return fvgs

    def get_key_opens(self, candles) -> Tuple[float,float,float]:
        mo=o830=o930=0.0
        for c in reversed(candles):
            try:
                dt=datetime.fromisoformat(str(c.time).replace('Z','+00:00'))
                ny=ICTTimeEngine.utc_to_ny(dt)
                if ny.hour==0 and mo==0:   mo=c.open
                if ny.hour==8 and o830==0: o830=c.open
                if ny.hour==9 and o930==0: o930=c.open
                if all([mo,o830,o930]): break
            except Exception: continue
        return mo, o830, o930

    def get_candles_since_midnight(self, ltf) -> List[Candle]:
        result=[]
        for c in ltf:
            try:
                dt=datetime.fromisoformat(str(c.time).replace('Z','+00:00'))
                ny=ICTTimeEngine.utc_to_ny(dt)
                if ny >= ny.replace(hour=0,minute=0,second=0,microsecond=0):
                    result.append(c)
            except Exception: continue
        return result

    def calc_sd_zones(self, rh, rl, direction) -> Tuple[float,float,float]:
        rng=rh-rl
        if rng==0: return 0.0,0.0,0.0
        if direction=='bullish': return rh+rng*1.5, rh+rng*2.5, rh+rng*4.5
        return rl-rng*1.5, rl-rng*2.5, rl-rng*4.5

    def find_liquidity_pools(self, candles) -> Tuple[List,List]:
        bp=[]; sp=[]; atr=self.get_atr(candles); buf=atr*0.3
        highs=[c.high for c in candles]; lows=[c.low for c in candles]
        for h in highs:
            cnt=sum(1 for x in highs if abs(x-h)<=buf)
            if cnt>=3 and not any(abs(p.price-h)<=buf for p in bp):
                bp.append(LiquidityPool('buy_side',h,cnt))
        for l in lows:
            cnt=sum(1 for x in lows if abs(x-l)<=buf)
            if cnt>=3 and not any(abs(p.price-l)<=buf for p in sp):
                sp.append(LiquidityPool('sell_side',l,cnt))
        recent=candles[-10:]
        for p in bp:
            if any(c.high>=p.price for c in recent): p.swept=True
        for p in sp:
            if any(c.low<=p.price for c in recent): p.swept=True
        bp.sort(key=lambda x:x.price,reverse=True); sp.sort(key=lambda x:x.price)
        return bp[:5], sp[:5]

    def detect_po3_phase(self, candles, direction) -> PO3Phase:
        if len(candles)<15: return PO3Phase.UNKNOWN
        recent=candles[-15:]; atr=self.get_atr(candles)
        ranges=[c.range_size() for c in recent[-8:]]
        avg_r=sum(ranges)/len(ranges) if ranges else 0
        if avg_r<atr*0.7: return PO3Phase.ACCUMULATION
        last5=recent[-5:]
        if direction=='bullish':
            if any(c.is_bearish() and c.body_size()>=atr*1.2 for c in last5): return PO3Phase.MANIPULATION
            if sum(1 for c in last5 if c.is_bullish())>=3: return PO3Phase.DISTRIBUTION
        else:
            if any(c.is_bullish() and c.body_size()>=atr*1.2 for c in last5): return PO3Phase.MANIPULATION
            if sum(1 for c in last5 if c.is_bearish())>=3: return PO3Phase.DISTRIBUTION
        return PO3Phase.UNKNOWN

    def detect_ohlc_model(self, daily) -> str:
        if not daily: return ''
        t=daily[-1]
        return 'bullish_olhc' if t.is_bullish() else 'bearish_ohlc' if t.is_bearish() else ''

    def detect_judas_swing(self, candles, direction) -> Tuple[bool,str]:
        if len(candles)<10: return False,''
        recent=candles[-10:]; atr=self.get_atr(candles)
        if direction=='bullish':
            if (any(c.is_bearish() and c.body_size()>=atr*1.0 for c in recent[:5]) and
                    any(c.is_bullish() and c.body_size()>=atr*1.0 for c in recent[5:])):
                return True,'bearish_fake'
        else:
            if (any(c.is_bullish() and c.body_size()>=atr*1.0 for c in recent[:5]) and
                    any(c.is_bearish() and c.body_size()>=atr*1.0 for c in recent[5:])):
                return True,'bullish_fake'
        return False,''

    def detect_830_manip(self, htf, direction, o830) -> Tuple[bool,str]:
        if not o830: return False,''
        atr=self.get_atr(htf); recent=htf[-6:]
        if direction=='bullish':
            if any(c.low<o830-atr*0.3 for c in recent): return True,'drop_below_830'
        else:
            if any(c.high>o830+atr*0.3 for c in recent): return True,'spike_above_830'
        return False,''

    def detect_smt(self, ca, cb, direction) -> Tuple[bool,str]:
        if len(ca)<20 or len(cb)<20: return False,''
        ar=ca[-10:]; br=cb[-10:]; ap=ca[-20:-10]; bp_=cb[-20:-10]
        if not ap or not bp_: return False,''
        ah=max(c.high for c in ar); al=min(c.low for c in ar)
        bh=max(c.high for c in br)
        pah=max(c.high for c in ap); pal=min(c.low for c in ap)
        pbh=max(c.high for c in bp_); pbl=min(c.low for c in bp_)
        if direction=='bearish':
            if ah>pah and bh<=pbh: return True,'bearish_div'
        else:
            if al<pal and min(c.low for c in br)>=pbl: return True,'bullish_div'
        return False,''

    def ltf_choch(self, ltf, direction) -> bool:
        # FIXED: soft check — insufficient data returns True
        if len(ltf) < 10: return True
        sw = self.classify_structure(self.detect_swings(ltf[-30:], lb=2))
        if len(sw) < 2: return True
        trend = self.get_trend(sw)
        if direction == 'bullish':
            return trend in ['uptrend','ranging']   # relaxed
        return trend in ['downtrend','ranging']      # relaxed

    def mtf_trend(self, htf, daily, weekly, direction) -> Tuple[bool,int]:
        def tr(c):
            if not c: return 'ranging'
            return self.get_trend(self.classify_structure(self.detect_swings(c)))
        exp = 'uptrend' if direction=='bullish' else 'downtrend'
        cnt = sum(1 for t in [tr(htf),tr(daily),tr(weekly)] if t==exp)
        return cnt==3, cnt

    def get_sr_levels(self, candles) -> List[float]:
        if len(candles)<10: return []
        levels=[]; atr=self.get_atr(candles)
        for s in self.detect_swings(candles, lb=5):
            if not any(abs(s.price-l)<atr for l in levels):
                levels.append(s.price)
        return sorted(levels)

# ═══════════════════════════════════════════════════════
#  MASTER ENGINE
# ═══════════════════════════════════════════════════════
class MasterEngine:
    def __init__(self):
        self.ict = ICTEngine(); self.mmc = MMCEngine()

    def analyse(self, symbol, htf, ltf, daily, weekly,
                smt_candles=None, oil_bias='neutral') -> SMCSignal:
        ts  = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        cfg = Config.for_symbol(symbol)
        is_c = fetcher.detect_type(symbol)=='crypto'
        kz   = ICTTimeEngine.get_kill_zone()

        def no_sig(warns):
            s = SMCSignal(SignalDirection.NO_TRADE,symbol,0,0,0,0,0,0,0,0,
                          'ranging','none',ICTContext(),MMCContext(),
                          session=kz,warnings=warns,timestamp=ts)
            return s

        # ── Candle count check (relaxed) ─────────────────────
        if len(htf) < 30:  return no_sig(['Insufficient HTF data'])
        if len(ltf) < 10:  return no_sig(['Insufficient LTF data'])

        cp  = htf[-1].close; atr = self.ict.get_atr(htf)
        sw  = self.ict.classify_structure(self.ict.detect_swings(htf))
        trend = self.ict.get_trend(sw)

        if trend == 'ranging': return no_sig(['Market ranging — no trend'])

        direction = 'bullish' if trend=='uptrend' else 'bearish'

        if atr>0 and htf[-1].range_size() > atr*3.0:   # relaxed from 2.5
            return no_sig(['Extreme volatility spike'])

        # ── OBs ──────────────────────────────────────────────
        obs   = self.ict.find_obs(htf, direction)
        fresh = [o for o in obs if o.status=='fresh']
        if not fresh: fresh = obs   # fallback to tapped OBs
        if not fresh: return no_sig(['No Order Block found'])

        all_fvg  = self.ict.find_fvgs(htf, direction)
        fresh_fg = [f for f in all_fvg if f.status=='fresh']

        best_ob = None
        for ob in reversed(fresh):
            if (ob.zone_low<=cp<=ob.zone_high or
                    abs(cp-ob.midpoint)/max(ob.midpoint,1e-9)<0.05):   # relaxed from 0.03
                best_ob=ob; break
        if not best_ob: best_ob=fresh[-1]

        best_fvg=None
        for fvg in reversed(fresh_fg):
            if fvg.zone_low<=best_ob.zone_high and fvg.zone_high>=best_ob.zone_low:
                best_fvg=fvg; break

        # ── ICT context ──────────────────────────────────────
        ict = ICTContext()
        ma,ml = ICTTimeEngine.is_macro_window()
        ict.kill_zone=kz; ict.macro_active=ma; ict.macro_label=ml
        ict.is_830_passed=ICTTimeEngine.is_830_passed()
        ict.is_930_passed=ICTTimeEngine.is_930_passed()

        mo,o830,o930 = self.ict.get_key_opens(htf)
        ict.midnight_open=mo; ict.open_830=o830; ict.open_930=o930
        ict.above_midnight=(cp>mo) if mo else False
        ict.above_830=(cp>o830)    if o830 else False
        ict.above_930=(cp>o930)    if o930 else False
        ac = sum([ict.above_midnight, ict.above_830, ict.above_930])
        ict.premium_discount = ('deep_premium' if ac>=2 else 'deep_discount' if ac<=1 else 'neutral')

        since_mn = self.ict.get_candles_since_midnight(ltf)
        if len(since_mn)>=3:
            fps=[f for f in self.ict.find_fvgs(since_mn,direction,mark_fp=True) if f.is_fpfvg]
            ict.fpfvg=fps[0] if fps else None

        if mo and len(htf)>=40:
            seg=htf[-40:-30]
            if seg:
                mh=max(c.high for c in seg); ml2=min(c.low for c in seg)
                s15,s25,s45=self.ict.calc_sd_zones(mh,ml2,direction)
                ict.sd_15=s15; ict.sd_25=s25; ict.sd_45=s45
                buf=atr*0.8   # relaxed from 0.5
                ict.at_sd_zone=('4.5' if abs(cp-s45)<=buf else
                                '2.5' if abs(cp-s25)<=buf else
                                '1.5' if abs(cp-s15)<=buf else '')

        ict.buy_pools,ict.sell_pools=self.ict.find_liquidity_pools(htf)
        if direction=='bullish':
            if any(p.swept for p in ict.sell_pools): ict.liquidity_swept=True; ict.swept_side='sell_side'
        else:
            if any(p.swept for p in ict.buy_pools):  ict.liquidity_swept=True; ict.swept_side='buy_side'

        ict.po3_phase=self.ict.detect_po3_phase(ltf,direction)
        ict.ohlc_model=self.ict.detect_ohlc_model(daily)
        ict.judas_swing,ict.judas_direction=self.ict.detect_judas_swing(ltf,direction)
        ict.is_expansion_day=(self.ict.trend_strength(sw)>=2 and ict.po3_phase==PO3Phase.DISTRIBUTION)

        if o830 and ict.is_830_passed:
            ict.manipulation_830,ict.manipulation_830_dir=self.ict.detect_830_manip(htf,direction,o830)
        if ict.is_930_passed and ict.manipulation_830:
            last3=htf[-3:]
            if direction=='bullish':
                ict.confirmed_930=any(c.is_bullish() and c.body_size()>=atr*0.6 for c in last3)
            else:
                ict.confirmed_930=any(c.is_bearish() and c.body_size()>=atr*0.6 for c in last3)

        if smt_candles and len(smt_candles)>=20:
            ict.smt_divergence,ict.smt_direction=self.ict.detect_smt(htf,smt_candles,direction)
            ict.smt_pair=SMT_PAIRS.get(symbol,'')

        mtf_full,mtf_cnt=self.ict.mtf_trend(htf,daily,weekly,direction)
        ltf_ok=self.ict.ltf_choch(ltf,direction)   # SOFT — no hard gate

        def cb(c):
            if not c: return 'neutral'
            return 'bullish' if c.close>c.open else 'bearish' if c.close<c.open else 'neutral'
        wb=cb(weekly[-1]) if weekly else 'neutral'
        db=cb(daily[-1])  if daily  else 'neutral'
        bias=('bullish' if [wb,db].count('bullish')>=2 else
              'bearish' if [wb,db].count('bearish')>=2 else 'neutral')

        sr_levels=self.ict.get_sr_levels(htf)

        # ── MMC ──────────────────────────────────────────────
        mmc_ctx=self.mmc.analyse_mmc(htf,ltf,direction,atr,
                                      best_ob.zone_low,best_ob.zone_high,sr_levels)

        # ════════════════════════════════════════════════════
        #  SCORING  raw_max=22 → normalize to 10
        # ════════════════════════════════════════════════════
        score=0; reasons=[]; warns=[]

        # TIME (up to 4)
        if kz!='dead' or is_c:    score+=1; reasons.append(f'Kill zone: {kz}')
        if ma:                    score+=1; reasons.append(f'Macro: {ml}')
        if ict.manipulation_830:  score+=1; reasons.append(f'8:30 manip: {ict.manipulation_830_dir}')
        if ict.confirmed_930:     score+=1; reasons.append('9:30 confirmed ✅')

        # ICT PRICE (up to 4)
        if mo:
            nymo_ok=((direction=='bullish' and not ict.above_midnight) or
                     (direction=='bearish' and ict.above_midnight))
            if nymo_ok: score+=1; reasons.append('NYMO bias ✅')
            else:       warns.append('Wrong side of NYMO')

        pd_ok=((direction=='bullish' and ict.premium_discount=='deep_discount') or
               (direction=='bearish' and ict.premium_discount=='deep_premium'))
        if pd_ok: score+=1; reasons.append(ict.premium_discount.replace('_',' ').title())

        if ict.fpfvg:     score+=1; reasons.append('FPFVG ✅')
        else:             warns.append('No FPFVG today')
        if ict.at_sd_zone: score+=1; reasons.append(f'{ict.at_sd_zone} SD zone ✅')

        # NARRATIVE (up to 4)
        if ict.po3_phase in [PO3Phase.DISTRIBUTION,PO3Phase.MANIPULATION]:
            score+=1; reasons.append(f'PO3: {ict.po3_phase.value}')
        else:
            warns.append(f'PO3: {ict.po3_phase.value}')

        ohlc_ok=((direction=='bullish' and ict.ohlc_model=='bullish_olhc') or
                 (direction=='bearish' and ict.ohlc_model=='bearish_ohlc'))
        if ohlc_ok: score+=1; reasons.append('OHLC model ✅')
        else:       warns.append('OHLC mismatch')

        if ict.liquidity_swept: score+=1; reasons.append('Liquidity swept ✅')
        else:                   warns.append('Liquidity not swept')
        if ict.smt_divergence:  score+=1; reasons.append(f'SMT vs {ict.smt_pair} ✅')
        else:                   warns.append(f'No SMT ({SMT_PAIRS.get(symbol,"N/A")})')

        # SMC CONFIRMATION (up to 4)
        if bias==direction: score+=1; reasons.append(f'HTF bias ✅ W:{wb} D:{db}')
        else:               warns.append(f'HTF bias mismatch: {bias}')

        if best_fvg:      score+=1; reasons.append('OB+FVG ✅')
        else:             reasons.append(f'OB only @ {best_ob.zone_low:.5f}')

        if mtf_full:      score+=1; reasons.append('3/3 TFs aligned ✅')
        elif mtf_cnt>=2:  score+=1; reasons.append(f'{mtf_cnt}/3 TFs ✅')
        else:             warns.append('MTF not aligned')

        if ltf_ok:        score+=1; reasons.append('LTF CHOCH ✅')
        else:             warns.append('LTF CHOCH weak')

        # MMC bonus (up to 4)
        mmc_add = min(mmc_ctx.mmcScore, 4)
        score+=mmc_add; reasons.extend(mmc_ctx.reasons); warns.extend(mmc_ctx.warnings)

        if ict.judas_swing: score+=1; reasons.append('Judas swing ✅')

        if cfg.get('check_oil') and oil_bias!='neutral':
            oil_imp='bearish' if oil_bias=='bullish' else 'bullish'
            if oil_imp!=direction: warns.append(f'Oil conflict: {oil_bias}')

        # Normalize
        normalized = min(10, round((score/22)*10))

        # ONLY gate: score below minimum  (NO hard ltf/nymo gates)
        if normalized < cfg['min_score']:
            s=no_sig([f'Score {normalized} < min {cfg["min_score"]}', *warns])
            s.ict=ict; s.mmc=mmc_ctx; return s

        # ── Entry / SL / TP ──────────────────────────────────
        if best_fvg:
            el=max(best_ob.zone_low,best_fvg.zone_low)
            eh=min(best_ob.zone_high,best_fvg.zone_high)
            if el>=eh: el,eh=best_ob.zone_low,best_ob.zone_high
            bt='OB + FVG'
        else:
            el,eh=best_ob.zone_low,best_ob.zone_high; bt='Order Block'

        entry=(el+eh)/2; buf2=entry*cfg['sl_buffer_pct']
        sl=(el-buf2 if direction=='bullish' else eh+buf2)
        risk=abs(entry-sl)
        if risk==0 or risk/entry<0.0001:
            return no_sig(['Invalid SL (too tight)'])

        m=1 if direction=='bullish' else -1
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
            bp=[p.price for p in ict.buy_pools  if p.price>entry]
            sp=[p.price for p in ict.sell_pools if p.price<entry]
            tgts=sorted(bp) if direction=='bullish' else sorted(sp,reverse=True)
            t1=tgts[0] if len(tgts)>0 else entry+risk*1.5*m
            t2=tgts[1] if len(tgts)>1 else entry+risk*2.5*m
            t3=tgts[2] if len(tgts)>2 else entry+risk*3.5*m

        rr=round(abs(t2-entry)/risk,2)
        if rr < cfg['min_rr']:
            return no_sig([f'RR {rr} < min {cfg["min_rr"]}'])

        ict.narrative_score=normalized; ict.reasons=reasons; ict.warnings=warns
        is_nvp=(mmc_ctx.nv is not None and mmc_ctx.nv.valid and mmc_ctx.nv.quality==NVQuality.PREMIUM)

        return SMCSignal(
            direction=(SignalDirection.LONG if direction=='bullish' else SignalDirection.SHORT),
            symbol=symbol,entry_low=round(el,6),entry_high=round(eh,6),
            stop_loss=round(sl,6),target_1=round(t1,6),target_2=round(t2,6),target_3=round(t3,6),
            rr_ratio=rr,confluence_score=normalized,trend=trend,block_type=bt,
            ict=ict,mmc=mmc_ctx,session=kz,reasons=reasons,warnings=warns,
            timestamp=ts,is_nv_premium=is_nvp)

# ═══════════════════════════════════════════════════════
#  POSITION SIZE
# ═══════════════════════════════════════════════════════
def calc_position_size(bal, risk_pct, entry, sl, symbol) -> Dict:
    ra=bal*(risk_pct/100); sd=abs(entry-sl)
    if sd==0: return {}
    if fetcher.detect_type(symbol)=='crypto':
        lot=ra/sd
    else:
        sa=sd*10000 if sd<1 else sd; lot=round(ra/(sa*10),2)
    return {'risk_amount':round(ra,2),'sl_distance':round(sd,6),
            'lot_size':round(lot,4),'risk_pct':risk_pct}

# ═══════════════════════════════════════════════════════
#  INSTANCES
# ═══════════════════════════════════════════════════════
fetcher = PublicDataFetcher()
engine  = MasterEngine()

def scan_one_pair(symbol: str) -> Optional[SMCSignal]:
    try:
        cfg=Config.for_symbol(symbol); ss=SMT_PAIRS.get(symbol)
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
            fh=ex.submit(fetcher.fetch,symbol,Config.HTF,200)
            fl=ex.submit(fetcher.fetch,symbol,Config.LTF,150)
            fd=ex.submit(fetcher.fetch,symbol,'1d',30)
            fw=ex.submit(fetcher.fetch,symbol,'1w',20)
            fs=ex.submit(fetcher.fetch,ss,Config.HTF,100) if ss else None
            fo=ex.submit(fetcher.get_oil_bias) if cfg.get('check_oil') else None
            htf=fh.result(); ltf=fl.result(); daily=fd.result(); weekly=fw.result()
            smt=fs.result() if fs else None; oil=fo.result() if fo else 'neutral'
        if not htf: return None
        return engine.analyse(symbol,htf,ltf,daily,weekly,smt,oil)
    except Exception as e:
        log.error(f'scan_one_pair {symbol}: {e}'); return None

# ═══════════════════════════════════════════════════════
#  FORMAT SIGNAL
# ═══════════════════════════════════════════════════════
PO3_EMOJI = {PO3Phase.ACCUMULATION:'⏸ Accumulation',PO3Phase.MANIPULATION:'🎭 Manipulation',
             PO3Phase.DISTRIBUTION:'🚀 Distribution',PO3Phase.UNKNOWN:'❓ Unknown'}
NV_Q_EMOJI = {NVQuality.PREMIUM:'⭐⭐⭐ PREMIUM',NVQuality.STANDARD:'⭐⭐ STANDARD',
              NVQuality.WEAK:'⭐ WEAK',NVQuality.NONE:''}

def format_signal(sig: SMCSignal, symbol: str,
                  is_alert: bool=False, is_crypto: bool=False,
                  account_balance: float=0) -> str:
    cfg=Config.for_symbol(symbol); ict=sig.ict; mmc=sig.mmc
    badge='🚨 <b>AUTO ALERT — ICT+SMC+MMC v5.0</b>\n' if is_alert else ''
    mkt='🔵 Crypto (24/7)' if is_crypto else ICTTimeEngine.session_label()

    if not sig.is_valid():
        warns='\n'.join(f'  ⚠️ {w}' for w in sig.warnings) or '  No setup'
        return (f'{badge}🔍 <b>{symbol}</b>\n━━━━━━━━━━━━━━━━━━━━━━\n'
                f'🕐 {mkt}\n━━━━━━━━━━━━━━━━━━━━━━\n'
                f'⏳ <b>No Trade Setup</b>\n\n{warns}\n\n⏰ {sig.timestamp}')

    em='🟢' if sig.direction==SignalDirection.LONG else '🔴'
    stars='⭐⭐⭐' if sig.confluence_score>=8 else '⭐⭐' if sig.confluence_score>=6 else '⭐'
    nvb=' 🏆 NV PREMIUM' if sig.is_nv_premium else ''
    rsns='\n'.join(f'  ✅ {r}' for r in sig.reasons)
    warns_txt='\n'.join(f'  ⚠️ {w}' for w in sig.warnings)
    vdir='UPTREND' if sig.direction==SignalDirection.LONG else 'DOWNTREND'
    sw='LOW sweep+CHOCH UP' if sig.direction==SignalDirection.LONG else 'HIGH sweep+CHOCH DOWN'
    loc='DISCOUNT' if sig.direction==SignalDirection.LONG else 'PREMIUM'

    ml=f'⚡ Macro: {ict.macro_label}\n' if ict.macro_active else ''
    po3l=f'📖 PO3: {PO3_EMOJI.get(ict.po3_phase,"")}\n'
    om={'bullish_olhc':'O→L→H→C 📈','bearish_ohlc':'O→H→L→C 📉'}
    ol=f'📊 OHLC: {om.get(ict.ohlc_model,"")}\n' if ict.ohlc_model else ''
    jl='🎭 Judas: Detected ✅\n' if ict.judas_swing else ''

    tick=lambda b:'✅' if b else '❌'
    pde=('🔴 Deep Premium' if ict.premium_discount=='deep_premium' else
         '🟢 Deep Discount' if ict.premium_discount=='deep_discount' else '⚪ Neutral')
    ob_blk=''
    if ict.midnight_open:
        ob_blk=(f'━━━━━━━━━━━━━━━━━━━━━━\n{pde}\n'
                f'  12AM:{ict.midnight_open:.5f} {tick(ict.above_midnight)}\n')
        if ict.open_830: ob_blk+=f'  8:30:{ict.open_830:.5f} {tick(ict.above_830)}\n'
        if ict.open_930: ob_blk+=f'  9:30:{ict.open_930:.5f} {tick(ict.above_930)}\n'

    fpl=(f'🎯 FPFVG:{ict.fpfvg.zone_low:.5f}–{ict.fpfvg.zone_high:.5f}\n') if ict.fpfvg else ''
    sdl=(f'📐 SD:1.5={ict.sd_15:.5f}|2.5={ict.sd_25:.5f}|4.5={ict.sd_45:.5f}\n') if ict.sd_15 else ''
    smtl=f'🔀 SMT:{ict.smt_pair}({ict.smt_direction})\n' if ict.smt_divergence else ''

    nv_blk=''
    if mmc and mmc.nv and mmc.nv.valid:
        nv=mmc.nv
        ntl='🟢 Positive NV' if nv.nv_type=='positive' else '🔴 Negative NV'
        cl='Continuation' if nv.continuation else 'Reversal'
        nv_blk=(f'━━━━━━━━━━━━━━━━━━━━━━\n📐 <b>NEEDED VOLUME</b>\n'
                f'  {ntl} | {NV_Q_EMOJI.get(nv.quality,"")}\n'
                f'  Signal: {cl} | Size: {nv.size_vs_atr:.1f}x ATR\n'
                f'  Zone: {nv.zone_low:.5f}–{nv.zone_high:.5f}\n'
                f'  IFC:{"✅" if nv.ifc_confirmed else "❌"} '
                f'S/R:{"⭐⭐" if nv.location_score>=2 else "⭐" if nv.location_score>=1 else "⚪"} '
                f'Frag:{"⚠️" if nv.is_fragmented else "✅"}\n')
    elif mmc and mmc.nv:
        nv_blk=f'━━━━━━━━━━━━━━━━━━━━━━\n📐 NV: ❌ {mmc.nv.reason}\n'

    mmcq=''
    if mmc:
        fo_txt='✅ Confirmed' if mmc.fakeout_detected else f'({mmc.fakeout_probability})'
        mmcq=(f'━━━━━━━━━━━━━━━━━━━━━━\n🧠 <b>MMC</b> '
              f'Candle:{mmc.candle_nature_score}/3 '
              f'Fakeout:{fo_txt} '
              f'99%:{"✅" if mmc.zone_passes_99pct else "❌"} '
              f'InS&D:{"✅" if mmc.insider_sd_zone else "❌"}\n')

    psb=''
    if account_balance>0:
        ps=calc_position_size(account_balance,1.0,sig.entry_high,sig.stop_loss,symbol)
        if ps: psb=(f'━━━━━━━━━━━━━━━━━━━━━━\n'
                    f'💰 Risk(1%):${ps["risk_amount"]} | Lots:{ps["lot_size"]}\n')

    return (
        f'{badge}'
        f'{em} <b>{sig.direction.value} — {symbol}{nvb}</b>\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'🕐 {mkt}\n{ml}{po3l}{ol}{jl}'
        f'📦 {sig.block_type} | 📈 {sig.trend.upper()}\n'
        f'{ob_blk}{fpl}{sdl}{smtl}{nv_blk}{mmcq}'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'📍 Entry : {sig.entry_low} – {sig.entry_high}\n'
        f'🛑 SL    : {sig.stop_loss}\n\n'
        f'🎯 T1: {sig.target_1}\n   └ Close 50% + move SL to entry\n'
        f'🎯 T2: {sig.target_2}\n   └ Close 25% + move SL to T1\n'
        f'🎯 T3: {sig.target_3}\n   └ Close 25% let run\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'📊 RR:1:{sig.rr_ratio}  ⭐ Score:{sig.confluence_score}/10 {stars}\n'
        f'{psb}'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'<b>Confluences:</b>\n{rsns}\n'
        + (f'\n<b>Warnings:</b>\n{warns_txt}\n' if warns_txt else '') +
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'📋 VERIFY: 1H {vdir} | 1H OB/FVG @ entry\n'
        f'          5M {sw} | 5M IFC at NV zone\n'
        f'⏰ {sig.timestamp}'
    )

# ═══════════════════════════════════════════════════════
#  AUTO SCANNER
# ═══════════════════════════════════════════════════════
async def auto_scanner(app):
    await asyncio.sleep(30)   # was 60
    while True:
        if not ScannerState.auto_alerts_on:
            await asyncio.sleep(60); continue
        if not ScannerState.daily_loss_ok():
            log.info('Daily loss limit — paused')
            await asyncio.sleep(30*60); continue
        if ICTTimeEngine.is_news_time():
            log.info('News time — skipping scan')
            await asyncio.sleep(10*60); continue

        kz = ICTTimeEngine.get_kill_zone()
        pairs = ALL_SCAN_PAIRS   # ALWAYS scan all pairs
        log.info(f'Auto scan [{kz}] — {len(pairs)} pairs')
        found = 0

        for symbol in pairs:
            try:
                if not ScannerState.can_alert(symbol): continue
                sig = scan_one_pair(symbol)
                if sig and sig.is_alert_worthy():
                    ScannerState.mark_alerted(symbol)
                    is_c = fetcher.detect_type(symbol)=='crypto'
                    msg  = format_signal(sig, symbol, is_alert=True, is_crypto=is_c)
                    await app.bot.send_message(chat_id=Config.TELEGRAM_CHAT_ID,
                                               text=msg, parse_mode='HTML')
                    found += 1
                    log.info(f'ALERT {symbol} S:{sig.confluence_score} RR:{sig.rr_ratio}')
                    await asyncio.sleep(2)
            except Exception as e:
                log.error(f'Scanner {symbol}: {e}')
            await asyncio.sleep(0.5)

        log.info(f'Scan complete — {found} alerts sent')
        await asyncio.sleep(Config.SCAN_INTERVAL_MINS * 60)

# ═══════════════════════════════════════════════════════
#  HELP TEXT
# ═══════════════════════════════════════════════════════
HELP_TEXT = """
🤖 <b>ICT + SMC + MMC Bot v5.0 FIXED</b>
━━━━━━━━━━━━━━━━━━━━━━

<b>Commands:</b>
  /on      — Auto alerts ON
  /off     — Auto alerts OFF
  /scan    — Manual full scan
  /debug SYMBOL — Why no signal?
  /pairs   — All watched pairs
  /status  — Bot health
  /size    — Position calculator
  /help    — This message

<b>Send any symbol for analysis:</b>
  XAUUSD  BTCUSDT  EURUSD  GBPJPY

<b>Thresholds (FIXED):</b>
  Alert : Score ≥ 6  | RR ≥ 1.8
  Valid : Score ≥ 4  | RR ≥ 1.5
  Scan  : Every 30 min — ALL pairs

<b>Frameworks:</b>
  SMC : OB FVG BOS CHOCH Liquidity
  ICT : NYMO FPFVG SD Macros PO3 SMT
  MMC : Fakeout 99% CCP Structure NV
  NV  : Channel IFC Body S/R Quality

<b>Score breakdown:</b>
  TIME (4)      Kill zone Macro 8:30 9:30
  ICT  (4)      NYMO FPFVG SD Premium
  NARR (4)      PO3 OHLC Liquidity SMT
  SMC  (4)      HTF OB+FVG MTF CHOCH
  MMC  (4 bonus) Candle NV Fakeout S/R
  8–10 ⭐⭐⭐ | 6–7 ⭐⭐ | 4–5 ⭐
"""

# ═══════════════════════════════════════════════════════
#  HANDLERS
# ═══════════════════════════════════════════════════════
async def start_handler(u, c):
    await u.message.reply_text(HELP_TEXT, parse_mode='HTML')

async def help_handler(u, c):
    await u.message.reply_text(HELP_TEXT, parse_mode='HTML')

async def on_handler(u, c):
    ScannerState.auto_alerts_on = True
    await u.message.reply_text(
        f'✅ <b>Auto Alerts ON</b>\n\n'
        f'Pairs   : {len(ALL_SCAN_PAIRS)} (all always)\n'
        f'Score   : ≥{Config.ALERT_MIN_SCORE} | RR ≥{Config.ALERT_MIN_RR}\n'
        f'Interval: every {Config.SCAN_INTERVAL_MINS} min',
        parse_mode='HTML')

async def off_handler(u, c):
    ScannerState.auto_alerts_on = False
    await u.message.reply_text('🔕 <b>Auto Alerts OFF</b>', parse_mode='HTML')

async def status_handler(u, c):
    kz = ICTTimeEngine.get_kill_zone()
    ma,ml = ICTTimeEngine.is_macro_window()
    ny = ICTTimeEngine.now_ny().strftime('%H:%M')
    await u.message.reply_text(
        f'📊 <b>Bot v5.0 FIXED Status</b>\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'Alerts   : {"✅ ON" if ScannerState.auto_alerts_on else "🔕 OFF"}\n'
        f'Daily OK : {"✅" if ScannerState.daily_loss_ok() else "🛑 STOPPED"}\n'
        f'NY Time  : {ny}\n'
        f'Session  : {ICTTimeEngine.session_label()}\n'
        f'Macro    : {"✅ "+ml if ma else "❌"}\n'
        f'News     : {"⚠️ YES" if ICTTimeEngine.is_news_time() else "✅ Clear"}\n'
        f'8:30     : {"✅" if ICTTimeEngine.is_830_passed() else "❌"}\n'
        f'9:30     : {"✅" if ICTTimeEngine.is_930_passed() else "❌"}\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'Pairs    : {len(ALL_SCAN_PAIRS)} (always all)\n'
        f'Alert    : Score≥{Config.ALERT_MIN_SCORE} RR≥{Config.ALERT_MIN_RR}\n'
        f'Valid    : Score≥{Config.MIN_CONFLUENCE_SCORE} RR≥{Config.MIN_RR_RATIO}\n'
        f'Cooldown : {Config.ALERT_COOLDOWN_HOURS}h\n'
        f'Alerted  : {len(ScannerState.last_alerted)}\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'⏰ {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}',
        parse_mode='HTML')

async def debug_handler(u: Update, c: ContextTypes.DEFAULT_TYPE):
    parts = u.message.text.split()
    if len(parts) < 2:
        await u.message.reply_text('Usage: /debug SYMBOL\nExample: /debug XAUUSD'); return
    symbol = fetcher.normalize(parts[1])
    loading = await u.message.reply_text(f'🔬 Debugging {symbol}...', parse_mode='HTML')
    try:
        htf   = fetcher.fetch(symbol, Config.HTF, 200)
        ltf   = fetcher.fetch(symbol, Config.LTF, 150)
        daily = fetcher.fetch(symbol, '1d', 30)

        if not htf:
            await loading.edit_text(f'❌ No data for {symbol}'); return

        ict_e = engine.ict; atr = ict_e.get_atr(htf)
        sw    = ict_e.classify_structure(ict_e.detect_swings(htf))
        trend = ict_e.get_trend(sw)
        dir_  = 'bullish' if trend=='uptrend' else 'bearish'
        obs   = ict_e.find_obs(htf, dir_)
        fvgs  = ict_e.find_fvgs(htf, dir_)
        fresh_ob  = [o for o in obs  if o.status=='fresh']
        fresh_fvg = [f for f in fvgs if f.status=='fresh']
        ltf_ok = ict_e.ltf_choch(ltf, dir_)
        mo,o830,o930 = ict_e.get_key_opens(htf)
        cp = htf[-1].close

        sig = scan_one_pair(symbol)

        msg = (f'🔬 <b>DEBUG: {symbol}</b>\n'
               f'━━━━━━━━━━━━━━━━━━━━━━\n'
               f'HTF candles : {len(htf)}\n'
               f'LTF candles : {len(ltf)}\n'
               f'Daily       : {len(daily)}\n'
               f'Price       : {cp:.5f}\n'
               f'ATR         : {atr:.5f}\n'
               f'━━━━━━━━━━━━━━━━━━━━━━\n'
               f'Trend       : {trend}\n'
               f'Swing pts   : {len(sw)}\n'
               f'All OBs     : {len(obs)} | Fresh: {len(fresh_ob)}\n'
               f'Fresh FVGs  : {len(fresh_fvg)}\n'
               f'LTF CHOCH   : {"✅" if ltf_ok else "❌"}\n'
               f'━━━━━━━━━━━━━━━━━━━━━━\n'
               f'Midnight    : {f"{mo:.5f}" if mo else "Not found"}\n'
               f'8:30 Open   : {f"{o830:.5f}" if o830 else "Not found"}\n'
               f'9:30 Open   : {f"{o930:.5f}" if o930 else "Not found"}\n'
               f'━━━━━━━━━━━━━━━━━━━━━━\n')

        if sig:
            msg += (f'Signal result:\n'
                    f'  Direction : {sig.direction.value}\n'
                    f'  Score     : {sig.confluence_score}/10\n'
                    f'  RR        : {sig.rr_ratio}\n'
                    f'  Valid     : {"✅" if sig.is_valid() else "❌"}\n'
                    f'  Alert     : {"✅" if sig.is_alert_worthy() else "❌"}\n')
            blocks = sig.warnings[:6]
            if blocks:
                msg += 'Blockers:\n'
                for w in blocks: msg += f'  ⚠️ {w}\n'
        else:
            msg += 'No signal object returned\n'

        await loading.edit_text(msg, parse_mode='HTML')
    except Exception as e:
        await loading.edit_text(f'❌ Debug error: {e}')

async def scan_handler(u, c):
    await u.message.reply_text(
        f'🔍 Scanning ALL {len(ALL_SCAN_PAIRS)} pairs...\n'
        f'Score≥{Config.ALERT_MIN_SCORE} | RR≥{Config.ALERT_MIN_RR}\n'
        f'⏳ ~3-5 minutes...', parse_mode='HTML')
    found = []
    for symbol in ALL_SCAN_PAIRS:
        try:
            sig = scan_one_pair(symbol)
            if sig and sig.is_alert_worthy():
                found.append((sig, fetcher.detect_type(symbol)=='crypto'))
        except Exception as e:
            log.error(f'Scan {symbol}: {e}')
    if not found:
        await u.message.reply_text(
            f'🔍 <b>No setups found</b>\n\n'
            f'Scanned: {len(ALL_SCAN_PAIRS)} pairs\n'
            f'Need: Score≥{Config.ALERT_MIN_SCORE} RR≥{Config.ALERT_MIN_RR}\n\n'
            f'💡 Use /debug XAUUSD to see why\n'
            f'⏰ {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}',
            parse_mode='HTML')
        return
    nv_cnt = sum(1 for s,_ in found if s.is_nv_premium)
    await u.message.reply_text(
        f'✅ <b>{len(found)} setup(s) found!</b>\n🏆 NV Premium: {nv_cnt}',
        parse_mode='HTML')
    for sig,is_c in found:
        msg = format_signal(sig, sig.symbol, is_alert=False, is_crypto=is_c)
        await u.message.reply_text(msg, parse_mode='HTML')
        await asyncio.sleep(1)

async def pairs_handler(u, c):
    def fmt(lst, cols=4):
        rows=[]
        for i in range(0,len(lst),cols): rows.append('  '+'  '.join(lst[i:i+cols]))
        return '\n'.join(rows)
    smt_txt='\n'.join(f'  {k} ↔ {v}' for k,v in list(SMT_PAIRS.items())[:6])
    await u.message.reply_text(
        f'👁 <b>Scanning {len(ALL_SCAN_PAIRS)} Pairs</b>\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n\n'
        f'🔵 <b>Crypto ({len(CRYPTO_PAIRS)}):</b>\n{fmt(CRYPTO_PAIRS)}\n\n'
        f'📈 <b>Forex+Metals+Oil ({len(FOREX_PAIRS_SCAN)}):</b>\n{fmt(FOREX_PAIRS_SCAN)}\n\n'
        f'<b>SMT pairs:</b>\n{smt_txt}',
        parse_mode='HTML')

async def size_handler(u, c):
    await u.message.reply_text(
        '💰 <b>Position Calculator</b>\n\n'
        'Format: <code>/size BALANCE SYMBOL ENTRY SL</code>\n'
        'Example: <code>/size 10000 XAUUSD 3285 3298</code>',
        parse_mode='HTML')

async def size_calc_handler(u, c):
    try:
        p=u.message.text.split()
        if len(p)<5: await u.message.reply_text('❌ /size BAL SYM ENTRY SL'); return
        bal=float(p[1]); sym=fetcher.normalize(p[2]); ent=float(p[3]); sl=float(p[4])
        ps=calc_position_size(bal,1.0,ent,sl,sym)
        if not ps: await u.message.reply_text('❌ Invalid values'); return
        await u.message.reply_text(
            f'💰 <b>{sym}</b>\n'
            f'Account  : ${bal:,.2f}\n'
            f'Entry    : {ent}\n'
            f'SL       : {sl}\n'
            f'SL dist  : {ps["sl_distance"]}\n'
            f'1% risk  : ${ps["risk_amount"]}\n'
            f'Lot size : {ps["lot_size"]}\n\n'
            f'⚠️ Max 3 trades/day | Stop at 2% daily loss',
            parse_mode='HTML')
    except Exception as e:
        await u.message.reply_text(f'❌ {e}')

async def symbol_handler(u: Update, c: ContextTypes.DEFAULT_TYPE):
    symbol = fetcher.normalize(u.message.text.strip())
    is_c   = fetcher.detect_type(symbol)=='crypto'
    loading = await u.message.reply_text(
        f'🔍 <b>{symbol}</b> — ICT+SMC+MMC+NV\n⏳ Analysing...', parse_mode='HTML')
    try:
        start=time.time(); cfg=Config.for_symbol(symbol); ss=SMT_PAIRS.get(symbol)
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
            fh=ex.submit(fetcher.fetch,symbol,Config.HTF,200)
            fl=ex.submit(fetcher.fetch,symbol,Config.LTF,150)
            fd=ex.submit(fetcher.fetch,symbol,'1d',30)
            fw=ex.submit(fetcher.fetch,symbol,'1w',20)
            fs=ex.submit(fetcher.fetch,ss,Config.HTF,100) if ss else None
            fo=ex.submit(fetcher.get_oil_bias) if cfg.get('check_oil') else None
            htf=fh.result(); ltf=fl.result(); daily=fd.result(); weekly=fw.result()
            smt=fs.result() if fs else None; oil=fo.result() if fo else 'neutral'
        if not htf:
            await loading.edit_text(f'❌ No data for {symbol}'); return
        sig=engine.analyse(symbol,htf,ltf,daily,weekly,smt,oil)
        elapsed=round(time.time()-start,1)
        msg=format_signal(sig,symbol,is_alert=False,is_crypto=is_c)
        msg+=f'\n⚡ <i>{elapsed}s</i>'
        await loading.edit_text(msg, parse_mode='HTML')
        log.info(f'{symbol}: {sig.direction.value} S:{sig.confluence_score} RR:{sig.rr_ratio}')
    except Exception as e:
        log.error(f'{symbol}:{e}')
        await loading.edit_text(f'❌ Error: {str(e)[:200]}', parse_mode='HTML')

# ═══════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════
def main():
    token = Config.TELEGRAM_BOT_TOKEN
    if not token:
        log.error('TELEGRAM_BOT_TOKEN not set'); return

    log.info('━'*50)
    log.info('ICT + SMC + MMC Bot v5.0 FIXED')
    log.info(f'Alert: Score≥{Config.ALERT_MIN_SCORE} RR≥{Config.ALERT_MIN_RR}')
    log.info(f'Valid : Score≥{Config.MIN_CONFLUENCE_SCORE} RR≥{Config.MIN_RR_RATIO}')
    log.info(f'Pairs : {len(ALL_SCAN_PAIRS)} (always all)')
    log.info('━'*50)

    app = ApplicationBuilder().token(token).build()
    app.add_handler(CommandHandler('start'  , start_handler))
    app.add_handler(CommandHandler('help'   , help_handler))
    app.add_handler(CommandHandler('on'     , on_handler))
    app.add_handler(CommandHandler('off'    , off_handler))
    app.add_handler(CommandHandler('status' , status_handler))
    app.add_handler(CommandHandler('debug'  , debug_handler))
    app.add_handler(CommandHandler('scan'   , scan_handler))
    app.add_handler(CommandHandler('pairs'  , pairs_handler))
    app.add_handler(CommandHandler('size'   , size_handler))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(r'^/size\s+\S'), size_calc_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, symbol_handler))

    async def post_init(application):
        if Config.TELEGRAM_CHAT_ID:
            asyncio.create_task(auto_scanner(application))
            log.info('Auto scanner started ✅')
        else:
            log.warning('No CHAT_ID — auto scanner disabled')

    app.post_init = post_init
    log.info('Bot running 🚀')
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
