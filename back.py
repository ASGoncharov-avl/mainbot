import pandas as pd
import matplotlib
from func import check_signal_row
import config
from instruments import *
from get_klines import fetch_klines_paged
        
if __name__ == '__main__':
    df = fetch_klines_paged(config.symbol, config.interval, config.total_bars)
    df = df.iloc[:-1]
    df = compute_bollinger(df)
    df = get_csi(df)
    df = compute_csc(df)
    df = compute_rsi(df, 14)   

    signals = [None]
    for i in range(1, len(df)):
        signals.append(check_signal_row(df.iloc[i], df.iloc[i - 1]))
    df['signal'] = signals
    df.tail(150).to_csv('dftest.csv', sep=';', index=False)
    
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
            take_profit = (
                entry_price * (1 + config.tprofit) if position_type =='long'
                else entry_price * (1- config.tprofit)
            )

        # === Выход из позиции ===
        elif in_position:
            # exit_index = entry_index + config.time_ex
            exit_row = df.iloc[i]
            low, high = exit_row['low'], exit_row['high']
            hit_stop = (
                low <= stop_price if position_type == 'long'
                else high >= stop_price
            )
            tp = (
                high >= take_profit if position_type == 'long'
                else low <= take_profit
            )
            exit = (
                signal == 'sell' if position_type == 'long'
                else signal == 'buy'
            )

            if hit_stop or tp or exit:
                if hit_stop : 
                    exit_price = stop_price
                    reason = 'stop_loss'
                else:  
                    exit_price = take_profit
                    reason = 'take_profit'
                # exit_row['close']
                if exit:
                    exit_price = exit_row['close']
                    reason = 'close'
                    
                pnl = (
                    (exit_price * 0.999 - entry_price * 1.001) / entry_price * 100
                    if position_type == 'long'
                    else (entry_price * 0.999 - exit_price * 1.001) / entry_price * 100
                )
                completed_trades.append({
                    'entry_time': df.iloc[entry_index]['timestamp'],
                    'exit_time': df.iloc[i]['timestamp'],
                    'position_type': position_type,
                    'entry_price': entry_price,
                    'exit_price': exit_price,
                    'pnl_%': pnl,
                    'reason': reason,
                })
                in_position = False

    # === Сохраняем сделки ===
    trades_df = pd.DataFrame(completed_trades)
    trades_df.to_csv('trades_complete.csv', sep=';', index=False)

    print("Последние сделки:")
    print(trades_df.tail(5))

    total_pnl = trades_df['pnl_%'].sum()
    print(f"\nОбщий PnL по стратегии: {total_pnl:.2f}%")