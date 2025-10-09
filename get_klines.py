import pandas as pd
import time
import config
import config2
from pybit.unified_trading import HTTP

session = HTTP(api_key=config2.BYBIT_API_KEY,
               api_secret = config2.BYBIT_API_SECRET
               )


def fetch_klines_paged(symbol = config.symbol, interval=config.interval,  total_bars=config.total_bars):

    limit = 1000
    data = []
    end_time = None  # самый последний бар (новейшая точка)

    while len(data) < total_bars:
        bars_to_fetch = min(limit, total_bars - len(data))
        try:
            klines = session.get_kline(
                symbol=symbol,
                interval=interval,
                limit=bars_to_fetch,
                endTime=end_time
            )
        except Exception as e:
            print("Ошибка API:", e)
            break

        if not klines:
            break
        llines = klines['result']['list']
        data = llines + data  # prepend! — старые свечи добавляем в начало
        end_time = int(llines[0][0]) - 1000*5*60*1000  # сдвиг назад по времени
        time.sleep(0.2)

    df = pd.DataFrame(data, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'
    ])
    df['timestamp'] = pd.to_datetime(pd.to_numeric(df['timestamp']), unit = 'ms',utc=True)
    df[['open','high','low','close','volume']] = df[['open','high','low','close', 'volume']].astype(float)
    df = df.drop_duplicates('timestamp').sort_values('timestamp').reset_index(drop=True)
    return df

current_price = fetch_klines_paged(config.symbol, config.interval, 1).iloc[0]['open']
balance = float(session.get_wallet_balance(
    accountType="UNIFIED",
    coin="USDT",)['result']['list'][0]["totalAvailableBalance"])

TRADE_QTY = int(balance * 0.85 / current_price * 10)
