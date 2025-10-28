from get_klines import fetch_klines_paged
import config
from scipy.stats import zscore
import pandas as pd
import numpy as np
import datetime


def compute_bollinger(df):
    df['ma'] = df['close'].rolling(config.bb_period).mean()
    df['std'] = df['close'].rolling(config.bb_period).std()
    df['upper'] = df['ma'] + config.bb_std * df['std']
    df['lower'] = df['ma'] - config.bb_std * df['std']
    return df

def compute_rsi(df, period=50):
    delta = df['close'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(period, min_periods=1).mean()
    avg_loss = loss.rolling(period, min_periods=1).mean()
    rs = avg_gain / avg_loss
    df['RSI'] = 100 - (100 / (1 + rs))
    df['RSI'] = df['RSI'].bfill()
    return df

def compute_csc(df):
    sub = df.tail(min(config.total_bars, len(df)))
    bull_thr = sub['CSI'].quantile(config.bull_quant)
    bear_thr = sub['CSI'].quantile(config.bear_quant)

    df['sentiment'] = np.where(df['CSI'] >= bull_thr, 'bull', 
                        np.where(df['CSI'] <= bear_thr, 'bear', 'neutral'))
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
    # if curr_type in ['bull','bear'] and length >= config.min_cluster:
    #     df.loc[curr_start:df.index[-1], 'cluster_id'] = f"{curr_type}_{curr_start}"

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

def ema(df, N):
    df[f'ema{N}'] = df['close'].ewm(span=N).mean()
    return df

# df = fetch_klines_paged(total_bars=100)
# df = df.iloc[:-1]

# def get_last_closed_candle():
#     df = fetch_klines_paged(total_bars = 5)
#     last_candle = df.iloc[-2]  # предпоследняя — она закрыта
#     print(last_candle)
#     now = datetime.datetime.now(datetime.UTC)
#     if (now - last_candle['timestamp'].to_pydatetime()).total_seconds() >= config.interval * 60:
#         return last_candle.to_frame()
#     else:
#         print("⏳ Свеча ещё не закрыта. Пропускаем.")
#         return None
    
# new_df = get_last_closed_candle()

# # df = pd.concat([df, new_df.tail(1)], ignore_index=True, join="inner").drop_duplicates('timestamp')
# df.update(new_df)
# print(df)
# print(type(df.iloc[0]['open']))
# df = compute_bollinger(df)
# df = get_csi(df)
# df = compute_csc(df)
# df = compute_rsi(df)

# # print (df)