import config
import config3 as config2
from instruments import *  # noqa: F403
import numpy as np
import datetime
from get_klines import fetch_klines_paged

EXIT_AFTER_BARS = 3 #15 минут
TELEGRAM_CHAT_ID = config2.TELEGRAM_CHAT_ID #свой chat_id

def can_enter_again(signal_type, entry_history):
    now = datetime.datetime.now(datetime.UTC)
    cooldown = config.interval * 60
    return not any((now - t).total_seconds() < cooldown and s == signal_type for t, s in entry_history) 
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

def close_position(symbol, position_type, qty, bybit, bot):
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
        
def place_order(symbol, side, qty, stop_price, bybit, bot):
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

def check_signal_row(row, prev_row):
    if np.isnan(row['lower']) or np.isnan(prev_row['CSI']) or np.isnan(row['CSI']):
        return None
    cluster = prev_row['cluster_id']
    if not isinstance(cluster, str):
        return None

    long_cond = (
        row['close'] < row['lower'] and
        row['CSI'] > 0 and row['CSI'] > prev_row['CSI'] and
        cluster.startswith('bear') and row['RSI'] > (100-config.rsi)
    )
    short_cond = (
        row['close'] > row['upper'] and
        row['CSI'] < 0 and row['CSI'] < prev_row['CSI'] and
        cluster.startswith('bull') and row['RSI'] < config.rsi
    )

    if long_cond:
        return 'buy'
    elif short_cond:
        return 'sell'
    return None