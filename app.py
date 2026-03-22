"""
SevenB - Economic Intelligence Platform
"""
import pandas as pd
import numpy as np
import json, os, time, threading, sqlite3, secrets
from datetime import datetime, timedelta
from functools import wraps
from flask import Flask, render_template, jsonify, request, redirect, url_for, flash, g
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', secrets.token_hex(32))

# --- AUTH SETUP ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'
login_manager.login_message_category = 'info'

DB_PATH = os.path.join(os.path.dirname(__file__), 'sevenb.db')

def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db

@app.teardown_appcontext
def close_db(e=None):
    db = g.pop('db', None)
    if db: db.close()

def init_db():
    db = sqlite3.connect(DB_PATH)
    db.execute('''CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        email TEXT,
        password_hash TEXT NOT NULL,
        is_admin INTEGER DEFAULT 0,
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        last_login TEXT
    )''')
    db.execute('''CREATE TABLE IF NOT EXISTS login_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        username TEXT,
        ip TEXT,
        timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
        action TEXT
    )''')
    # Create default admin if not exists
    existing = db.execute('SELECT id FROM users WHERE username=?', ('admin',)).fetchone()
    if not existing:
        db.execute('INSERT INTO users (username, email, password_hash, is_admin) VALUES (?,?,?,?)',
                   ('admin', 'admin@sevenb.io', generate_password_hash('sevenb2024'), 1))
    db.commit()
    db.close()

class User(UserMixin):
    def __init__(self, id, username, email, is_admin, is_active):
        self.id = id
        self.username = username
        self.email = email
        self.is_admin = is_admin
        self._is_active = is_active
    @property
    def is_active(self):
        return bool(self._is_active)

@login_manager.user_loader
def load_user(user_id):
    db = get_db()
    row = db.execute('SELECT * FROM users WHERE id=?', (user_id,)).fetchone()
    if row:
        return User(row['id'], row['username'], row['email'], row['is_admin'], row['is_active'])
    return None

def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin:
            flash('Admin access required', 'error')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated

def log_action(user_id, username, action):
    try:
        db = get_db()
        ip = request.remote_addr or 'unknown'
        db.execute('INSERT INTO login_logs (user_id, username, ip, action) VALUES (?,?,?,?)',
                   (user_id, username, ip, action))
        db.commit()
    except:
        pass

# --- DATA ENGINE ---
DATA_CACHE = {}
MARKET_CACHE = {}
BIAS_CACHE = {}
BACKTEST_CACHE = {}
LAST_REFRESH = None
REFRESH_INTERVAL = 3600

FRED_SERIES = {
    'GDPC1': {'name': 'Real GDP', 'unit': 'Billions $', 'cat': 'GDP', 'freq': 'Q'},
    'PCECC96': {'name': 'Real PCE', 'unit': 'Billions $', 'cat': 'GDP', 'freq': 'Q'},
    'GCEC1': {'name': 'Gov Consumption', 'unit': 'Billions $', 'cat': 'GDP', 'freq': 'Q'},
    'GPDIC1': {'name': 'Gross Private Investment', 'unit': 'Billions $', 'cat': 'GDP', 'freq': 'Q'},
    'PERMIT': {'name': 'Building Permits', 'unit': 'Thousands', 'cat': 'Housing', 'freq': 'M'},
    'HOUST': {'name': 'Housing Starts', 'unit': 'Thousands', 'cat': 'Housing', 'freq': 'M'},
    'HSN1F': {'name': 'New Home Sales', 'unit': 'Thousands', 'cat': 'Housing', 'freq': 'M'},
    'WALCL': {'name': 'Fed Total Assets', 'unit': 'Millions $', 'cat': 'Central Bank', 'freq': 'W'},
    'FEDFUNDS': {'name': 'Effective Fed Funds Rate', 'unit': '%', 'cat': 'Central Bank', 'freq': 'M'},
    'M2SL': {'name': 'M2 Money Supply', 'unit': 'Billions $', 'cat': 'M2', 'freq': 'M'},
    'GFDEBTN': {'name': 'Federal Debt', 'unit': 'Millions $', 'cat': 'Government', 'freq': 'Q'},
    'DGS10': {'name': '10-Year Treasury Yield', 'unit': '%', 'cat': 'Government', 'freq': 'D'},
    'UMCSENT': {'name': 'Consumer Sentiment (UoM)', 'unit': 'Index', 'cat': 'Consumption', 'freq': 'M'},
    'RSAFS': {'name': 'Retail Sales', 'unit': 'Millions $', 'cat': 'Consumption', 'freq': 'M'},
    'PAYEMS': {'name': 'Nonfarm Payrolls', 'unit': 'Thousands', 'cat': 'Labor', 'freq': 'M'},
    'UNRATE': {'name': 'Unemployment Rate', 'unit': '%', 'cat': 'Labor', 'freq': 'M'},
    'ICSA': {'name': 'Initial Jobless Claims', 'unit': 'Claims', 'cat': 'Labor', 'freq': 'W'},
    'CCSA': {'name': 'Continuing Claims', 'unit': 'Claims', 'cat': 'Labor', 'freq': 'W'},
    'CPIAUCSL': {'name': 'CPI All Urban', 'unit': 'Index', 'cat': 'Inflation', 'freq': 'M'},
    'CPILFESL': {'name': 'Core CPI', 'unit': 'Index', 'cat': 'Inflation', 'freq': 'M'},
    'PPIACO': {'name': 'PPI All Commodities', 'unit': 'Index', 'cat': 'Inflation', 'freq': 'M'},
    'PCEPI': {'name': 'PCE Price Index', 'unit': 'Index', 'cat': 'PCE', 'freq': 'M'},
    'PCEPILFE': {'name': 'Core PCE Price Index', 'unit': 'Index', 'cat': 'PCE', 'freq': 'M'},
    'INDPRO': {'name': 'Industrial Production', 'unit': 'Index', 'cat': 'Industrial Production', 'freq': 'M'},
    'IEABC': {'name': 'Current Account Balance', 'unit': 'Millions $', 'cat': 'Current Account', 'freq': 'Q'},
}

MARKET_SERIES = {
    'SP500': {'name': 'S&P 500', 'unit': 'Index', 'cat': 'Equities', 'freq': 'D'},
    'NASDAQCOM': {'name': 'NASDAQ Composite', 'unit': 'Index', 'cat': 'Equities', 'freq': 'D'},
    'DJIA': {'name': 'Dow Jones Industrial', 'unit': 'Index', 'cat': 'Equities', 'freq': 'D'},
    'VIXCLS': {'name': 'VIX Volatility', 'unit': 'Index', 'cat': 'Volatility', 'freq': 'D'},
    'DGS2': {'name': '2-Year Treasury', 'unit': '%', 'cat': 'Bonds', 'freq': 'D'},
    'DGS10': {'name': '10-Year Treasury', 'unit': '%', 'cat': 'Bonds', 'freq': 'D'},
    'DGS30': {'name': '30-Year Treasury', 'unit': '%', 'cat': 'Bonds', 'freq': 'D'},
    'T10Y2Y': {'name': '10Y-2Y Spread', 'unit': '%', 'cat': 'Bonds', 'freq': 'D'},
    'BAMLH0A0HYM2': {'name': 'HY Credit Spread', 'unit': '%', 'cat': 'Credit', 'freq': 'D'},
    'DTWEXBGS': {'name': 'USD Index (Broad)', 'unit': 'Index', 'cat': 'Forex', 'freq': 'D'},
    'DEXUSEU': {'name': 'EUR/USD', 'unit': 'Rate', 'cat': 'Forex', 'freq': 'D'},
    'DEXJPUS': {'name': 'USD/JPY', 'unit': 'Rate', 'cat': 'Forex', 'freq': 'D'},
    'DEXUSUK': {'name': 'GBP/USD', 'unit': 'Rate', 'cat': 'Forex', 'freq': 'D'},
    'DCOILWTICO': {'name': 'WTI Crude Oil', 'unit': '$/barrel', 'cat': 'Commodities', 'freq': 'D'},
    'DCOILBRENTEU': {'name': 'Brent Crude', 'unit': '$/barrel', 'cat': 'Commodities', 'freq': 'D'},
    'GOLDAMGBD228NLBM': {'name': 'Gold Price', 'unit': '$/oz', 'cat': 'Commodities', 'freq': 'D'},
}

BIAS_RULES = {
    'SP500': {'drivers': [('GDPC1',1,'GDP growth bullish'),('PAYEMS',1,'Job growth bullish'),('UMCSENT',1,'Confidence bullish'),('RSAFS',1,'Retail bullish'),('UNRATE',-1,'Unemployment bearish'),('ICSA',-1,'Claims bearish'),('FEDFUNDS',-0.5,'Rate hikes bearish'),('INDPRO',1,'Industrial bullish'),('CPIAUCSL',-0.5,'Inflation bearish')]},
    'NASDAQCOM': {'drivers': [('GDPC1',1,'GDP bullish'),('FEDFUNDS',-1,'Rates bearish for growth'),('DGS10',-0.8,'Yields bearish'),('UMCSENT',0.8,'Sentiment bullish'),('M2SL',1,'Liquidity bullish'),('INDPRO',0.7,'Industrial bullish')]},
    'DJIA': {'drivers': [('GDPC1',1,'GDP bullish'),('PAYEMS',1,'Jobs bullish'),('RSAFS',1,'Retail bullish'),('UNRATE',-1,'Unemployment bearish'),('INDPRO',1,'Industrial bullish'),('FEDFUNDS',-0.3,'Rates bearish')]},
    'VIXCLS': {'drivers': [('UNRATE',1,'Unemployment=fear'),('ICSA',1,'Claims=fear'),('UMCSENT',-1,'Low confidence=fear'),('FEDFUNDS',0.5,'Rates=uncertainty'),('GDPC1',-1,'Growth reduces fear')]},
    'DGS10': {'drivers': [('CPIAUCSL',1,'Inflation pushes yields'),('PCEPI',1,'PCE pushes yields'),('FEDFUNDS',0.8,'Fed correlated'),('GDPC1',0.5,'Growth pushes yields'),('GFDEBTN',0.3,'Debt=higher yields'),('M2SL',-0.5,'Liquidity suppresses')]},
    'DGS2': {'drivers': [('FEDFUNDS',1,'Tracks Fed'),('CPIAUCSL',0.8,'Inflation'),('PAYEMS',0.5,'Jobs=rates')]},
    'DGS30': {'drivers': [('CPIAUCSL',1,'Inflation'),('GDPC1',0.5,'Growth'),('GFDEBTN',0.5,'Deficit'),('FEDFUNDS',0.5,'Fed')]},
    'T10Y2Y': {'drivers': [('FEDFUNDS',-1,'Hikes flatten'),('GDPC1',0.5,'Growth steepens'),('UNRATE',0.5,'Recession steepens')]},
    'DTWEXBGS': {'drivers': [('FEDFUNDS',1,'Rates strengthen USD'),('DGS10',0.8,'Yields attract capital'),('GDPC1',0.5,'Strong economy'),('CPIAUCSL',-0.5,'Inflation weakens'),('IEABC',-0.5,'Deficit weakens')]},
    'DEXUSEU': {'drivers': [('FEDFUNDS',-1,'US rates=EUR weak'),('GDPC1',-0.5,'Strong US=EUR weak'),('DGS10',-0.5,'Yields=EUR weak')]},
    'DEXJPUS': {'drivers': [('FEDFUNDS',1,'US rates=JPY weak'),('DGS10',1,'Yields=JPY weak'),('GDPC1',0.5,'Strong US')]},
    'DEXUSUK': {'drivers': [('FEDFUNDS',-1,'US rates=GBP weak'),('GDPC1',-0.3,'Strong US')]},
    'DCOILWTICO': {'drivers': [('GDPC1',1,'Growth=demand'),('INDPRO',1,'Industrial=demand'),('PAYEMS',0.5,'Employment'),('RSAFS',0.5,'Retail'),('M2SL',0.5,'Liquidity')]},
    'DCOILBRENTEU': {'drivers': [('GDPC1',1,'Growth'),('INDPRO',1,'Industrial'),('RSAFS',0.5,'Retail'),('M2SL',0.5,'Liquidity')]},
    'GOLDAMGBD228NLBM': {'drivers': [('CPIAUCSL',1,'Inflation hedge'),('FEDFUNDS',-1,'Rates bearish'),('DGS10',-0.8,'Real yields'),('M2SL',1,'Money printing'),('GFDEBTN',0.5,'Fiscal concerns'),('UNRATE',0.5,'Uncertainty')]},
    'BAMLH0A0HYM2': {'drivers': [('UNRATE',1,'Unemployment widens'),('GDPC1',-1,'Growth tightens'),('ICSA',0.8,'Claims widen'),('UMCSENT',-0.5,'Confidence tightens')]},
}

def fetch_fred(series_id):
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    try:
        df = pd.read_csv(url)
        df.columns = ['date', 'value']
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        return df.dropna()
    except:
        return pd.DataFrame(columns=['date', 'value'])

def to_monthly(df, freq):
    if freq in ('W', 'D'):
        df = df.copy()
        df['month'] = df['date'].dt.to_period('M')
        m = df.groupby('month').agg({'value': 'mean', 'date': 'last'}).reset_index()
        m['date'] = m['month'].dt.to_timestamp()
        return m.drop(columns='month')
    return df

def compute_changes(df):
    df = df.sort_values('date').reset_index(drop=True)
    n = len(df)
    r = {'latest': float(df['value'].iloc[-1]), 'latest_date': df['date'].iloc[-1]}
    for label, periods in [('1w', 5), ('1m', 21), ('3m', 63)]:
        if n > periods:
            old, new = float(df['value'].iloc[-periods-1]), float(df['value'].iloc[-1])
            r[f'{label}_chg'] = round(new - old, 4)
            r[f'{label}_pct'] = round((new - old) / abs(old) * 100, 2) if old != 0 else 0
        else:
            r[f'{label}_chg'] = r[f'{label}_pct'] = None
    return r

def compute_monthly_changes(df):
    df = df.sort_values('date').reset_index(drop=True)
    df['mom'] = df['value'].pct_change() * 100
    df['net'] = df['value'].diff()
    last12 = df.tail(13).iloc[1:]
    return {
        'dates': last12['date'].dt.strftime('%Y-%m').tolist(),
        'mom_pct': [round(v, 2) if pd.notna(v) else None for v in last12['mom'].tolist()],
        'net_chg': [round(v, 2) if pd.notna(v) else None for v in last12['net'].tolist()],
        'values': [round(v, 4) if pd.notna(v) else None for v in last12['value'].tolist()],
    }

def compute_yoy(df, periods=12):
    df = df.sort_values('date').reset_index(drop=True)
    df['yoy'] = df['value'].pct_change(periods) * 100
    return df

def get_trend_score(series_id):
    if series_id not in DATA_CACHE: return 0
    df = DATA_CACHE[series_id]['monthly'].sort_values('date')
    if len(df) < 4: return 0
    vals = df['value'].tail(6).values
    vals = vals[~np.isnan(vals)]
    if len(vals) < 3: return 0
    short, lng = np.mean(vals[-3:]), np.mean(vals[-6:]) if len(vals) >= 6 else np.mean(vals)
    if lng == 0: return 0
    pct = (short - lng) / abs(lng) * 100
    return 1 if pct > 0.5 else (-1 if pct < -0.5 else 0)

def run_backtest():
    global BACKTEST_CACHE
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Running 15-year backtest...")
    eco_monthly = {}
    for sid in FRED_SERIES:
        if sid in DATA_CACHE:
            df = DATA_CACHE[sid]['monthly'][['date','value']].copy().sort_values('date').set_index('date')
            eco_monthly[sid] = df[~df.index.duplicated(keep='last')]
    mkt_monthly = {}
    for sid in MARKET_SERIES:
        if sid in MARKET_CACHE:
            df = MARKET_CACHE[sid]['monthly'][['date','value']].copy().sort_values('date').set_index('date')
            mkt_monthly[sid] = df[~df.index.duplicated(keep='last')]

    cutoff = pd.Timestamp.now() - pd.DateOffset(years=15)
    results = {}
    for market_id, rules in BIAS_RULES.items():
        if market_id not in mkt_monthly: continue
        mkt = mkt_monthly[market_id]
        dates_list = mkt.index[mkt.index >= cutoff]
        if len(dates_list) < 12: continue
        signals = []
        for i, dt in enumerate(dates_list):
            if i < 6: continue
            score, max_score = 0, 0
            for eco_id, weight, _ in rules['drivers']:
                max_score += abs(weight)
                if eco_id not in eco_monthly: continue
                eco_before = eco_monthly[eco_id][eco_monthly[eco_id].index <= dt]
                if len(eco_before) < 6: continue
                vals = eco_before['value'].tail(6).values
                vals = vals[~np.isnan(vals)]
                if len(vals) < 3: continue
                short, lng = np.mean(vals[-3:]), np.mean(vals)
                if lng == 0: continue
                pct = (short - lng) / abs(lng) * 100
                trend = 1 if pct > 0.5 else (-1 if pct < -0.5 else 0)
                score += trend * weight
            if max_score == 0: continue
            norm = score / max_score * 100
            price_now = float(mkt.loc[dt, 'value'])
            remaining = mkt.index[mkt.index > dt]
            fwd_1m = (float(mkt.loc[remaining[0], 'value']) - price_now) / abs(price_now) * 100 if len(remaining) >= 1 else None
            fwd_3m = (float(mkt.loc[remaining[2], 'value']) - price_now) / abs(price_now) * 100 if len(remaining) >= 3 else None
            bias = 'BULLISH' if norm > 25 else 'LEAN BULL' if norm > 5 else 'BEARISH' if norm < -25 else 'LEAN BEAR' if norm < -5 else 'NEUTRAL'
            signals.append({'date': dt.strftime('%Y-%m-%d'), 'score': round(norm, 1), 'bias': bias, 'fwd_1m': round(fwd_1m, 2) if fwd_1m is not None else None, 'fwd_3m': round(fwd_3m, 2) if fwd_3m is not None else None, 'price': round(price_now, 2)})
        if not signals: continue
        df_sig = pd.DataFrame(signals)
        stats = {}
        for bl in ['BULLISH','LEAN BULL','NEUTRAL','LEAN BEAR','BEARISH']:
            sub = df_sig[df_sig['bias'] == bl]
            if len(sub) == 0: continue
            f1, f3 = sub['fwd_1m'].dropna(), sub['fwd_3m'].dropna()
            stats[bl] = {'count': int(len(sub)), 'avg_1m': round(float(f1.mean()), 2) if len(f1) else 0, 'avg_3m': round(float(f3.mean()), 2) if len(f3) else 0, 'med_1m': round(float(f1.median()), 2) if len(f1) else 0, 'med_3m': round(float(f3.median()), 2) if len(f3) else 0, 'win_1m': round((f1 > 0).sum() / len(f1) * 100, 1) if len(f1) else 0, 'win_3m': round((f3 > 0).sum() / len(f3) * 100, 1) if len(f3) else 0, 'returns_1m': f1.tolist(), 'returns_3m': f3.tolist()}
        bull_sigs = df_sig[df_sig['score'] > 5]['fwd_1m'].dropna()
        bear_sigs = df_sig[df_sig['score'] < -5]['fwd_1m'].dropna()
        total_d = len(bull_sigs) + len(bear_sigs)
        accuracy = round(((bull_sigs > 0).sum() + (bear_sigs < 0).sum()) / total_d * 100, 1) if total_d > 0 else 0
        eq = [100]
        for s in signals:
            if s['fwd_1m'] is None: continue
            if s['score'] > 5: eq.append(eq[-1] * (1 + s['fwd_1m'] / 100))
            elif s['score'] < -5: eq.append(eq[-1] * (1 - s['fwd_1m'] / 100))
            else: eq.append(eq[-1])
        results[market_id] = {'name': MARKET_SERIES.get(market_id, {}).get('name', market_id), 'cat': MARKET_SERIES.get(market_id, {}).get('cat', ''), 'signals': signals, 'stats': stats, 'accuracy': accuracy, 'total_signals': len(signals), 'equity_curve': [round(e, 2) for e in eq], 'scores': [s['score'] for s in signals], 'fwd_1m_all': [s['fwd_1m'] for s in signals if s['fwd_1m'] is not None], 'fwd_3m_all': [s['fwd_3m'] for s in signals if s['fwd_3m'] is not None], 'dates': [s['date'] for s in signals]}
    BACKTEST_CACHE = results
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Backtest done. {len(results)} markets.")

def compute_bias():
    global BIAS_CACHE
    biases = {}
    for market_id, rules in BIAS_RULES.items():
        score, max_score, details = 0, 0, []
        for eco_id, weight, reason in rules['drivers']:
            trend = get_trend_score(eco_id)
            contribution = trend * weight
            score += contribution
            max_score += abs(weight)
            details.append({'indicator': FRED_SERIES.get(eco_id, {}).get('name', eco_id), 'series_id': eco_id, 'trend': 'Rising' if trend > 0 else 'Falling' if trend < 0 else 'Flat', 'weight': weight, 'contribution': round(contribution, 2), 'reason': reason})
        normalized = round(score / max_score * 100, 1) if max_score > 0 else 0
        bias = 'BULLISH' if normalized > 25 else 'LEAN BULL' if normalized > 5 else 'BEARISH' if normalized < -25 else 'LEAN BEAR' if normalized < -5 else 'NEUTRAL'
        market_changes = compute_changes(MARKET_CACHE[market_id]['raw']) if market_id in MARKET_CACHE else {}
        biases[market_id] = {'name': MARKET_SERIES.get(market_id, {}).get('name', market_id), 'cat': MARKET_SERIES.get(market_id, {}).get('cat', ''), 'score': normalized, 'bias': bias, 'details': details, 'market': market_changes}
    BIAS_CACHE = biases

def refresh_data():
    global DATA_CACHE, MARKET_CACHE, LAST_REFRESH
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Refreshing...")
    for sid, info in FRED_SERIES.items():
        df = fetch_fred(sid)
        if not df.empty:
            df_m = to_monthly(df, info['freq'])
            df_m = compute_yoy(df_m, 4 if info['freq'] == 'Q' else 12)
            DATA_CACHE[sid] = {'raw': df, 'monthly': df_m, 'info': info, 'latest_date': df['date'].max().strftime('%Y-%m-%d'), 'latest_value': float(df['value'].iloc[-1]), 'monthly_changes': compute_monthly_changes(df_m)}
    for sid, info in MARKET_SERIES.items():
        df = fetch_fred(sid)
        if not df.empty:
            df_m = to_monthly(df, info['freq'])
            df_m = compute_yoy(df_m, 12)
            MARKET_CACHE[sid] = {'raw': df, 'monthly': df_m, 'info': info, 'latest_date': df['date'].max().strftime('%Y-%m-%d'), 'latest_value': float(df['value'].iloc[-1]), 'monthly_changes': compute_monthly_changes(df_m)}
    compute_bias()
    run_backtest()
    LAST_REFRESH = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{LAST_REFRESH}] Done. {len(DATA_CACHE)} eco + {len(MARKET_CACHE)} market.")

def auto_refresh_loop():
    while True:
        try: refresh_data()
        except Exception as e: print(f"Refresh error: {e}")
        time.sleep(REFRESH_INTERVAL)

# --- AUTH ROUTES ---
@app.route('/landing')
def landing():
    return render_template('landing.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('index'))
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        db = get_db()
        row = db.execute('SELECT * FROM users WHERE username=?', (username,)).fetchone()
        if row and check_password_hash(row['password_hash'], password):
            if not row['is_active']:
                flash('Account deactivated. Contact admin.', 'error')
                return render_template('login.html')
            user = User(row['id'], row['username'], row['email'], row['is_admin'], row['is_active'])
            login_user(user, remember=True)
            db.execute('UPDATE users SET last_login=? WHERE id=?', (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), row['id']))
            db.commit()
            log_action(row['id'], username, 'login')
            return redirect(request.args.get('next') or url_for('index'))
        flash('Invalid credentials', 'error')
        log_action(None, username, 'failed_login')
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    log_action(current_user.id, current_user.username, 'logout')
    logout_user()
    return redirect(url_for('login'))

# --- ADMIN ROUTES ---
@app.route('/admin')
@login_required
@admin_required
def admin_panel():
    return render_template('admin.html', last_refresh=LAST_REFRESH)

@app.route('/api/admin/users')
@login_required
@admin_required
def api_admin_users():
    db = get_db()
    users = db.execute('SELECT id, username, email, is_admin, is_active, created_at, last_login FROM users ORDER BY created_at DESC').fetchall()
    return jsonify([dict(u) for u in users])

@app.route('/api/admin/logs')
@login_required
@admin_required
def api_admin_logs():
    db = get_db()
    logs = db.execute('SELECT * FROM login_logs ORDER BY timestamp DESC LIMIT 100').fetchall()
    return jsonify([dict(l) for l in logs])

@app.route('/api/admin/user', methods=['POST'])
@login_required
@admin_required
def api_admin_create_user():
    data = request.json
    username = data.get('username', '').strip()
    email = data.get('email', '').strip()
    password = data.get('password', '')
    if not username or not password:
        return jsonify({'error': 'Username and password required'}), 400
    db = get_db()
    try:
        db.execute('INSERT INTO users (username, email, password_hash) VALUES (?,?,?)',
                   (username, email, generate_password_hash(password)))
        db.commit()
        return jsonify({'status': 'created'})
    except sqlite3.IntegrityError:
        return jsonify({'error': 'Username already exists'}), 400

@app.route('/api/admin/user/<int:user_id>', methods=['DELETE'])
@login_required
@admin_required
def api_admin_delete_user(user_id):
    if user_id == current_user.id:
        return jsonify({'error': 'Cannot delete yourself'}), 400
    db = get_db()
    db.execute('DELETE FROM users WHERE id=?', (user_id,))
    db.commit()
    return jsonify({'status': 'deleted'})

@app.route('/api/admin/user/<int:user_id>/toggle', methods=['POST'])
@login_required
@admin_required
def api_admin_toggle_user(user_id):
    if user_id == current_user.id:
        return jsonify({'error': 'Cannot deactivate yourself'}), 400
    db = get_db()
    db.execute('UPDATE users SET is_active = CASE WHEN is_active=1 THEN 0 ELSE 1 END WHERE id=?', (user_id,))
    db.commit()
    return jsonify({'status': 'toggled'})

# --- PAGE ROUTES ---
@app.route('/')
@login_required
def index():
    return render_template('dashboard.html', last_refresh=LAST_REFRESH)

@app.route('/bias')
@login_required
def bias_page():
    return render_template('bias.html', last_refresh=LAST_REFRESH)

@app.route('/backtest')
@login_required
def backtest_page():
    return render_template('backtest.html', last_refresh=LAST_REFRESH)

# --- API ROUTES ---
@app.route('/api/all')
@login_required
def api_all():
    result = {}
    for sid, c in DATA_CACHE.items():
        df = c['monthly']
        result[sid] = {'info': c['info'], 'latest_date': c['latest_date'], 'latest_value': c['latest_value'], 'dates': df['date'].dt.strftime('%Y-%m-%d').tolist(), 'values': [round(v, 4) if pd.notna(v) else None for v in df['value'].tolist()], 'yoy': [round(v, 4) if pd.notna(v) else None for v in df['yoy'].tolist()], 'monthly_changes': c.get('monthly_changes', {})}
    return jsonify({'data': result, 'last_refresh': LAST_REFRESH, 'loading': len(DATA_CACHE) == 0})

@app.route('/api/bias')
@login_required
def api_bias():
    return jsonify({'biases': BIAS_CACHE, 'last_refresh': LAST_REFRESH, 'loading': len(BIAS_CACHE) == 0})

@app.route('/api/markets')
@login_required
def api_markets():
    result = {}
    for sid, c in MARKET_CACHE.items():
        df = c['monthly']
        result[sid] = {'info': c['info'], 'latest_date': c['latest_date'], 'latest_value': c['latest_value'], 'changes': compute_changes(c['raw']), 'dates': df['date'].dt.strftime('%Y-%m-%d').tolist(), 'values': [round(v, 4) if pd.notna(v) else None for v in df['value'].tolist()], 'yoy': [round(v, 4) if pd.notna(v) else None for v in df['yoy'].tolist()], 'monthly_changes': c.get('monthly_changes', {})}
    return jsonify({'data': result, 'last_refresh': LAST_REFRESH, 'loading': len(MARKET_CACHE) == 0})

@app.route('/api/backtest')
@login_required
def api_backtest():
    return jsonify({'data': BACKTEST_CACHE, 'last_refresh': LAST_REFRESH, 'loading': len(BACKTEST_CACHE) == 0})

@app.route('/api/refresh', methods=['POST'])
@login_required
def api_refresh():
    threading.Thread(target=refresh_data, daemon=True).start()
    return jsonify({'status': 'refreshing'})

@app.route('/api/status')
@login_required
def api_status():
    return jsonify({'last_refresh': LAST_REFRESH, 'eco_count': len(DATA_CACHE), 'market_count': len(MARKET_CACHE), 'bias_count': len(BIAS_CACHE)})

init_db()

def start_bg():
    t = threading.Thread(target=auto_refresh_loop, daemon=True)
    t.start()
    print("[SevenB] Background refresh started.")

start_bg()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5050))
    print(f"\n=== SevenB running at http://localhost:{port} ===\n")
    app.run(host='0.0.0.0', port=port, debug=False)
