import pandas as pd
import pytz

EST = pytz.timezone('US/Eastern')  # 美东时区

class MarketDataProcessor:
    @staticmethod
    def calculate_indicators(data, donchian_period=20, exit_period=10, atr_period=14, trend_period=50):
        if data.index.tzinfo is None:
            data.index = data.index.tz_localize(EST)
        data['highest_high'] = data['high'].rolling(window=donchian_period).max()
        data['lowest_low'] = data['low'].rolling(window=donchian_period).min()
        data['exit_low'] = data['low'].rolling(window=exit_period).min()
        data['exit_high'] = data['high'].rolling(window=exit_period).max()
        data['atr'] = data['high'].rolling(window=atr_period).max() - data['low'].rolling(window=atr_period).min()
        data['ema'] = data['close'].ewm(span=trend_period, adjust=False).mean()
        return data
