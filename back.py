import pandas as pd
import numpy as np
import time
import datetime
from collections import deque
from scipy.stats import zscore
from binance.client import Client as BinanceClient
from pybit.unified_trading import HTTP
import telebot
import config
from get_klines import fetch_klines_paged
# === НАСТРОЙКИ ===

symbol = "DOGEUSDT"
interval = 5
bb_period = 40
bb_std = 1
STOP_LOSS_PCT = 0.004
    
def compute_rsi(df, period=450):
    delta = df['close'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(period, min_periods=1).mean()
    avg_loss = loss.rolling(period, min_periods=1).mean()
    rs = avg_gain / avg_loss
    df['RSI'] = 100 - (100 / (1 + rs))
    df['RSI'] = df['RSI'].bfill()
    return df

def compute_csc(df, min_cluster, bull_quant, bear_quant):
    bull_thr = df['CSI'].quantile(bull_quant)
    bear_thr = df['CSI'].quantile(bear_quant)

    df['sentiment'] = np.where(df['CSI'] >= bull_thr, 'bull',
                        np.where(df['CSI'] <= bear_thr, 'bear', 'neutral'))
    df['cluster_id'] = pd.Series(dtype='object')
    curr_type, curr_start, length = None, None, 0

    for i, s in df['sentiment'].items():
        if s == curr_type and s in ['bull', 'bear']:
            length += 1
        else:
            if curr_type in ['bull', 'bear'] and length >= min_cluster:
                df.loc[curr_start:i-1, 'cluster_id'] = f"{curr_type}_{curr_start}"
            if s in ['bull', 'bear']:
                curr_type, curr_start, length = s, i, 1
            else:
                curr_type, length = None, 0

    if curr_type in ['bull', 'bear'] and length >= min_cluster:
        df.loc[curr_start:df.index[-1], 'cluster_id'] = f"{curr_type}_{curr_start}"

    return df

def compute_bollinger(df):
    df['ma'] = df['close'].rolling(bb_period).mean()
    df['std'] = df['close'].rolling(bb_period).std()
    df['upper'] = df['ma'] + bb_std * df['std']
    df['lower'] = df['ma'] - bb_std * df['std']
    return df

def get_csi(df):
    body = (df['close'] - df['open']).abs()
    rng = (df['high'] - df['low']).replace(0, np.nan)
    body_ratio = body / rng
    direction = np.where(df['close'] > df['open'], 1, -1)
    vol_score = df['volume'] / df['volume'].rolling(50).max()
    range_z = zscore(df['high'] - df['low']).clip(-3, 3)

    tr = pd.DataFrame({
        'hl': df['high'] - df['low'],
        'hc': (df['high'] - df['close'].shift(1)).abs(),
        'lc': (df['low'] - df['close'].shift(1)).abs()
    }).max(axis=1)

    atr = tr.rolling(14).mean().bfill()
    df['CSI'] = direction * (0.5 * body_ratio + 0.3 * vol_score + 0.2 * range_z) / atr
    return df
    
def check_signal_row(row, prev_row):
    if np.isnan(row['lower']) or np.isnan(prev_row['CSI']) or np.isnan(row['CSI']):
        return None
    cluster = row['cluster_id']
    if not isinstance(cluster, str):
        return None

    long_cond = (
        row['close'] < row['lower'] and
        row['CSI'] > 0 and row['CSI'] > prev_row['CSI'] and
        cluster.startswith('bull') and row['RSI'] < config.rsi
    )
    short_cond = (
        row['close'] > row['upper'] and
        row['CSI'] < 0 and row['CSI'] < prev_row['CSI'] and
        cluster.startswith('bear') and row['RSI'] > (100 - config.rsi)
    )

    if long_cond:
        return 'buy'
    elif short_cond:
        return 'sell'
    return None
    
if __name__ == '__main__':
    df = fetch_klines_paged(symbol, interval, 8640)
    df = compute_bollinger(df)
    df = get_csi(df)
    df = compute_csc(df, config.min_cluster, config.bull_quant, config.bear_quant)
    df = compute_rsi(df)   

    signals = [None]
    for i in range(1, len(df)):
        signals.append(check_signal_row(df.iloc[i], df.iloc[i - 1]))
    df['signal'] = signals
    df.tail(5)[['timestamp','CSI']].to_csv('dftest.csv', sep=';', index=False, mode='a')
    
    in_position = False
    entry_price = None
    entry_index = None
    position_type = None

    completed_trades = []

    for i in range(1, len(df)):
        row = df.iloc[i]
        signal = row['signal']

        # === Вход в позицию ===
        if not in_position and signal in ['buy', 'sell']:
            in_position = True
            entry_index = i
            entry_price = row['close']
            position_type = 'long' if signal == 'buy' else 'short'
            stop_price = (
                entry_price * (1 - STOP_LOSS_PCT) if position_type == 'long'
                else entry_price * (1 + STOP_LOSS_PCT)
            )

        # === Выход из позиции ===
        elif in_position:
            exit_index = entry_index + 15
            exit_row = df.iloc[i]
            low, high = exit_row['low'], exit_row['high']
            hit_stop = (
                low <= stop_price if position_type == 'long'
                else high >= stop_price
            )

            if hit_stop or i >= exit_index:
                exit_price = stop_price if hit_stop else exit_row['close']
                pnl = (
                    (exit_price - entry_price) / entry_price * 100
                    if position_type == 'long'
                    else (entry_price - exit_price) / entry_price * 100
                )
                completed_trades.append({
                    'entry_time': df.iloc[entry_index]['timestamp'],
                    'exit_time': df.iloc[i]['timestamp'],
                    'position_type': position_type,
                    'entry_price': entry_price,
                    'exit_price': exit_price,
                    'pnl_%': pnl,
                    'reason': 'stop_loss' if hit_stop else 'time_exit'
                })
                in_position = False

    # === Сохраняем сделки ===
    trades_df = pd.DataFrame(completed_trades)
    trades_df.to_csv('trades_complete.csv', sep=';', index=False)

    print("Последние сделки:")
    print(trades_df.tail(5))

    total_pnl = trades_df['pnl_%'].sum()
    print(f"\nОбщий PnL по стратегии: {total_pnl:.2f}%")