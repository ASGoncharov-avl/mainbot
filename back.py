import pandas as pd
import numpy as np
import time
import datetime
from collections import deque
from scipy.stats import zscore
from pybit.unified_trading import HTTP
import telebot
import config
from instruments import *
from get_klines import fetch_klines_paged
# === НАСТРОЙКИ ===
        
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
    df = fetch_klines_paged(config.symbol, config.interval, config.total_bars)
    df = compute_bollinger(df)
    df = get_csi(df)
    df = compute_csc(df)
    df = compute_rsi(df)   

    signals = [None]
    for i in range(1, len(df)):
        signals.append(check_signal_row(df.iloc[i], df.iloc[i - 1]))
    df['signal'] = signals
    # df.to_csv('dftest.csv', sep=';', index=False)
    
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
                entry_price * (1 - config.STOP_LOSS_PCT) if position_type == 'long'
                else entry_price * (1 + config.STOP_LOSS_PCT)
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
                    (exit_price - entry_price*1.002) / entry_price * 100
                    if position_type == 'long'
                    else (entry_price - exit_price*1.002) / entry_price * 100
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