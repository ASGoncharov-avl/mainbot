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
place_order("DOGEUSDT", 'buy', 180, 0.187)