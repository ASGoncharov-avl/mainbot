import config
import config3 as config2
from instruments import *
import pandas as pd
import numpy as np
import time
import datetime
from collections import deque
from scipy.stats import zscore
from pybit.unified_trading import HTTP
import telebot
from get_klines import fetch_klines_paged, TRADE_QTY

# === НАСТРОЙКИ ===
EXIT_AFTER_BARS = 3 #15 минут
TELEGRAM_CHAT_ID = config2.TELEGRAM_CHAT_ID #свой chat_id
offset = datetime.timezone(datetime.timedelta(hours=3))


# === API ===
bot = telebot.TeleBot(config2.token) #tg bot
BYBIT_API_KEY = config2.BYBIT_API_KEY
BYBIT_API_SECRET = config2.BYBIT_API_SECRET

bybit = HTTP ( api_key=BYBIT_API_KEY, 
              api_secret=BYBIT_API_SECRET, 
              ) 

entry_history = deque(maxlen=100)
open_positions = []

def get_last_closed_candle():
    df = fetch_klines_paged(total_bars = 3)
    df = df.iloc[:-1]
    
    last_candle = df.tail(1)  # предпоследняя — она закрыта
    now = datetime.datetime.now(datetime.UTC)
    if (now - last_candle.iloc[-1]['timestamp'].to_pydatetime()).total_seconds() >= config.interval * 60:
        return last_candle
    else:
        print("⏳ Свеча ещё не закрыта. Пропускаем.")
        return None
    

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


def place_order(symbol, side, qty, stop_price):
    try:
        bybit.place_order(
            category="linear",
            symbol=symbol,
            side="Buy" if side == "long" else "Sell",
            order_type="Market",
            qty=qty,
            time_in_force="GoodTillCancel",
            stopLoss=round(stop_price, 5)
        )
        bot.send_message(TELEGRAM_CHAT_ID, f"✅ Открыта {side.upper()} позиция на {qty} {config.symbol}")
    except Exception as e:
        print("Ошибка ордера:", e)


def close_position(symbol, position_type, qty):
    try:
        bybit.place_order(
            category="linear",
            symbol=symbol,
            side="Sell" if position_type == "long" else "Buy",
            order_type="Market",
            qty=qty,
            time_in_force="GoodTillCancel"
        )
        bot.send_message(TELEGRAM_CHAT_ID, f"🔻 Закрыта {position_type.upper()} позиция ({qty} {symbol})")
    except Exception as e:
        bot.send_message(TELEGRAM_CHAT_ID, f"❗ Ошибка при закрытии позиции: {e}")

def can_enter_again(signal_type):
    now = datetime.datetime.now(datetime.UTC)
    cooldown = config.interval * 60
    return not any((now - t).total_seconds() < cooldown and s == signal_type for t, s in entry_history)

bot.send_message(TELEGRAM_CHAT_ID, "📈 Бот запущен")
print("Бот запущен")
df = fetch_klines_paged()
df = df.iloc[:-1]
last_checked_minute = None

while True:
    try:
        now = datetime.datetime.now()
        if now.minute % config.interval == 0 and now.second < 10:
            if last_checked_minute == now.minute:
                time.sleep(0.2)
                continue
            last_checked_minute = now.minute
            new_df = get_last_closed_candle()
            if new_df is None:
                continue
            df = pd.concat([df, new_df], ignore_index=True).drop_duplicates('timestamp')
            df = df.tail(config.total_bars)
            
            df = compute_bollinger(df)
            df = compute_rsi(df)
            df = get_csi(df)
            df = compute_csc(df)
            
            df['signal'] = [None] + [check_signal_row(df.iloc[i], df.iloc[i - 1]) for i in range(1, len(df))]
            latest = df.iloc[-1]
            signal = latest['signal']
            cluster_id = latest['cluster_id']
            bot.send_message(TELEGRAM_CHAT_ID, f"{df.iloc[-1]['timestamp']} {signal}")
            if signal in ['buy', 'sell'] and can_enter_again(signal):
                entry_price = latest['close']
                stop_price = entry_price * (1 - config.STOP_LOSS_PCT) if signal == 'buy' else entry_price * (1 + config.STOP_LOSS_PCT)
                position_type = 'long' if signal == 'buy' else 'short'
                entry_time = datetime.datetime.now(datetime.UTC)

                place_order(config.symbol, position_type, TRADE_QTY, stop_price)
                entry_history.append((entry_time, signal))
                open_positions.append({
                    'type': position_type,
                    'entry_price': entry_price,
                    'stop_price': stop_price,
                    'entry_time': entry_time
                })
            positions_to_remove = []
            current_price = latest['close']
            updated_positions = []
            for pos in open_positions[:]:
                entry_time = pos['entry_time']
                print(entry_time)
                elapsed = (datetime.datetime.now(datetime.UTC) - entry_time).total_seconds()
                position_data = bybit.get_positions(category="linear", symbol=config.symbol)["result"]["list"]
                position_size = float(position_data[0]['size']) if position_data else 0

                # Проверка на срабатывание стоп-лосса
                hit_stop = (
                    (pos['type'] == 'long' and current_price <= pos['stop_price']) or
                    (pos['type'] == 'short' and current_price >= pos['stop_price'])
                )
                
                if hit_stop or elapsed >= (EXIT_AFTER_BARS * 5 * 60):
                    # Проверка: позиция ещё существует на бирже
                    if position_size > 0:
                        exit_price = pos['stop_price'] if hit_stop else current_price
                        pnl = (
                            (exit_price * 0.999 - pos['entry_price'] * 1.001) / pos['entry_price'] * 100
                            if pos['type'] == 'long'
                            else (pos['entry_price'] * 0.999 - exit_price * 1.001) / pos['entry_price'] * 100
                        )
                        reason = "стоп-лосс" if hit_stop else "по времени"
                        close_position(config.symbol, pos['type'], TRADE_QTY)
                        bot.send_message(
                            TELEGRAM_CHAT_ID,
                            f"❌ Закрытие позиции: {pos['type'].upper()} по {exit_price:.2f} ({reason})\nPnL: {pnl:.2f}%"
                        )
                    else:
                        # Позиция уже закрыта вручную/стопом вне кода
                        bot.send_message(
                            TELEGRAM_CHAT_ID,
                            f"ℹ️ Позиция {pos['type'].upper()} уже закрыта на бирже. Удаляю из списка."
                        )

                    positions_to_remove.append(pos)

            # Удаление обработанных/закрытых позиций
            for p in positions_to_remove:
                if p in open_positions:
                    open_positions.remove(p)

    except Exception as e:
        bot.send_message(TELEGRAM_CHAT_ID, f"❗ Ошибка: {e}")
        print(e)

        time.sleep(3)
