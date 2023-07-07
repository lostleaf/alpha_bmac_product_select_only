import pandas as pd
from utils.commons import round_to_tick
from decimal import Decimal


def filter_before(df1: pd.DataFrame):
    long_condition1 = df1['ChgPctMax_fl_24'].between(-1e+100, 0.2, inclusive='both')
    df1 = df1.loc[long_condition1]

    df1[f'Volume_fl_24_rank'] = df1.groupby('candle_begin_time')['Volume_fl_24'].rank(method='first',
                                                                                      pct=False,
                                                                                      ascending=False)
    long_condition2 = df1[f'Volume_fl_24_rank'].between(-1e+100, 60, inclusive='both')
    df1 = df1.loc[long_condition2]

    return df1


def filiter_nan_inf(df_factor, factor_filter_cols):
    with pd.option_context('mode.use_inf_as_na', True):  # 过滤 nan 及 inf
        return df_factor[~df_factor[factor_filter_cols].isna().any(axis=1)]


def select_coin(df_factor: pd.DataFrame, factor_name, ascending, long_num, short_num):
    df_factor = df_factor.sort_values(factor_name, ascending=ascending, ignore_index=True)
    df_short = df_factor.head(short_num).copy()
    df_long = df_factor.tail(long_num).copy()

    return df_long, df_short


def assign_position(df_long, df_short, df_exg, capital_usdt, long_num, short_num):

    def pos(capital, close, face_value):
        return round_to_tick(Decimal(capital / close), face_value)

    long_coin_capital = capital_usdt / 2 / long_num
    df_long['position'] = df_long.apply(
        lambda r: pos(long_coin_capital, r['close'], df_exg.at[r['symbol'], 'face_value']), axis=1)
    df_long['pos_val'] = df_long['position'].astype(float) * df_long['close']

    short_coin_capital = capital_usdt / 2 / short_num
    df_short['position'] = df_short.apply(
        lambda r: pos(short_coin_capital, r['close'], df_exg.at[r['symbol'], 'face_value']), axis=1)
    df_short['pos_val'] = df_short['position'].astype(float) * df_short['close']

    return df_long, df_short