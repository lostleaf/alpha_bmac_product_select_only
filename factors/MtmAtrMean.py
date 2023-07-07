import pandas as pd
import talib as ta

from utils.diff import add_diff


def signal(*args):
    # Mtm乘波动率ATR
    df: pd.DataFrame = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    mtm = df['close'].pct_change(n)
    atr = ta.ATR(df['high'], df['low'], df['close'], timeperiod=n)
    atr_norm = atr / df['close'].rolling(window=n, min_periods=1).mean()
    df[factor_name] = mtm.rolling(window=n, min_periods=1).mean() * atr_norm

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df