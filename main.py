import config
import pandas as pd
import numpy as np
import time
import datetime
from collections import deque
from scipy.stats import zscore
from binance.client import Client as BinanceClient
from pybit.unified_trading import HTTP
import telebot

# === НАСТРОЙКИ ===
symbol = "DOGEUSDT"
interval = "5m"
bb_period = 40
bb_std = 1
STOP_LOSS_PCT = 0.004
TRADE_QTY = 1200
EXIT_AFTER_BARS = 3 #15 минут
TELEGRAM_CHAT_ID = config.TELEGRAM_CHAT_ID #свой chat_id

# === API ===
bot = telebot.TeleBot(config.token) #tg bot
BYBIT_API_KEY = config.BYBIT_API_KEY
BYBIT_API_SECRET = config.BYBIT_API_SECRET

bybit = HTTP(api_key=BYBIT_API_KEY, api_secret=BYBIT_API_SECRET) #bybit init

entry_history = deque(maxlen=100)
open_positions = []
client = BinanceClient()

def fetch_klines_paged(symbol=symbol, interval=interval,  total_bars=20000, client = None):
    if client is None:
        client = BinanceClient()

    limit = 1000
    data = []
    end_time = None  # самый последний бар (новейшая точка)

    while len(data) < total_bars:
        bars_to_fetch = min(limit, total_bars - len(data))
        try:
            klines = client.futures_klines(
                symbol=symbol,
                interval=interval,
                limit=bars_to_fetch,
                endTime=end_time
            )
        except Exception as e:
            print("Ошибка Binance API:", e)
            break

        if not klines:
            break

        data = klines + data  # prepend! — старые свечи добавляем в начало
        end_time = klines[0][0] - 1  # сдвиг назад по времени
        time.sleep(0.2)

    df = pd.DataFrame(data, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base', 'taker_buy_quote', 'ignore'
    ])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df[['open','high','low','close','volume']] = df[['open','high','low','close','volume']].astype(float)
    df = df.drop_duplicates('timestamp').sort_values('timestamp').reset_index(drop=True)
    return df

def compute_bollinger(df):
    df = fetch_klines_paged(total_bars=10000)
    df['ma'] = df['close'].rolling(bb_period).mean()
    df['std'] = df['close'].rolling(bb_period).std()
    df['upper'] = df['ma'] + bb_std * df['std']
    df['lower'] = df['ma'] - bb_std * df['std']
    return df

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

def get_last_closed_candle():
    df = fetch_klines_paged(total_bars=10000)
    last_candle = df.iloc[-2]  # предпоследняя — она закрыта
    now = datetime.datetime.now(datetime.UTC)
    if (now - last_candle['timestamp'].to_pydatetime()).total_seconds() >= 300:
        return last_candle.to_frame().T
    else:
        print("⏳ Свеча ещё не закрыта. Пропускаем.")
        return None
    
def compute_csc(df):
    sub = df.tail(min(50000, len(df)))
    bull_thr = sub['CSI'].quantile(config.bull_quant)
    bear_thr = sub['CSI'].quantile(config.bear_quant)
    df['sentiment'] = np.where(df['CSI'] >= bull_thr, 'bull', np.where(df['CSI'] <= bear_thr, 'bear', 'neutral'))
    df['cluster_id'] = pd.Series(dtype='object')
    curr_type, curr_start, length = None, None, 0
    for i, s in df['sentiment'].items():
        if s == curr_type and s in ['bull','bear']:
            length += 1
        else:
            if curr_type in ['bull','bear'] and length >= config.min_cluster:
                df.loc[curr_start:i-1, 'cluster_id'] = f"{curr_type}_{curr_start}"
            if s in ['bull','bear']:
                curr_type, curr_start, length = s, i, 1
            else:
                curr_type, length = None, 0
    if curr_type in ['bull','bear'] and length >= config.min_cluster:
        df.loc[curr_start:df.index[-1], 'cluster_id'] = f"{curr_type}_{curr_start}"
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


def place_order(symbol, side, qty, stop_price):
    try:
        bybit.place_order(
            category="linear",
            symbol=symbol,
            side="Buy" if side == "long" else "Sell",
            order_type="Market",
            qty=qty,
            time_in_force="GoodTillCancel",
            stopLoss=round(stop_price, 2)
        )
        bot.send_message(TELEGRAM_CHAT_ID, f"✅ Открыта {side.upper()} позиция на {qty} ETH")
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
    cooldown = 5 * 60
    return not any((now - t).total_seconds() < cooldown and s == signal_type for t, s in entry_history)


bot.send_message(TELEGRAM_CHAT_ID, "📈 Бот запущен")
print("Бот запущен")
df = fetch_klines_paged()
last_checked_minute = None

while True:
    try:
        now = datetime.datetime.now(datetime.UTC)
        if now.minute % 5 == 0 and now.second < 10:
            if last_checked_minute == now.minute:
                time.sleep(1)
                continue
            last_checked_minute = now.minute
            new_df = get_last_closed_candle()
            if new_df is None:
                continue
            df = pd.concat([df, new_df.tail(1)]).drop_duplicates('timestamp').reset_index(drop=True)
            if len(df) > config.total_bars:
                df = df.tail(config.total_bars)
            df = compute_bollinger(df)
            df = get_csi(df)
            df = compute_csc(df)
            df = compute_rsi(df)
            df['signal'] = [None] + [check_signal_row(df.iloc[i], df.iloc[i - 1]) for i in range(1, len(df))]

            latest = df.iloc[-2]
            signal = latest['signal']
            bot.send_message(TELEGRAM_CHAT_ID, f"{now}: {signal}")

            if signal in ['buy', 'sell'] and can_enter_again(signal):
                entry_price = latest['close']
                stop_price = entry_price * (1 - STOP_LOSS_PCT) if signal == 'buy' else entry_price * (1 + STOP_LOSS_PCT)
                position_type = 'long' if signal == 'buy' else 'short'
                entry_time = datetime.datetime.now(datetime.UTC)

                place_order(symbol, position_type, TRADE_QTY, stop_price)
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
                position_data = bybit.get_positions(category="linear", symbol=symbol)["result"]["list"]
                position_size = float(position_data[0]['size']) if position_data else 0

                # Проверка на срабатывание стоп-лосса
                hit_stop = (
                    (pos['type'] == 'long' and current_price <= pos['stop_price']) or
                    (pos['type'] == 'short' and current_price >= pos['stop_price'])
                )

                if hit_stop or elapsed >= (EXIT_AFTER_BARS * 5):
                    # Проверка: позиция ещё существует на бирже
                    if position_size > 0:
                        exit_price = pos['stop_price'] if hit_stop else current_price
                        pnl = (
                            (exit_price - pos['entry_price']) / pos['entry_price'] * 100
                            if pos['type'] == 'long'
                            else (pos['entry_price'] - exit_price) / pos['entry_price'] * 100
                        )
                        reason = "стоп-лосс" if hit_stop else "по времени"
                        close_position(symbol, pos['type'], TRADE_QTY)
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