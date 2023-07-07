from datetime import timedelta
import importlib
import logging
import time

import pandas as pd

from candle_manager import CandleFeatherManager
from market import MY_DEBUG_LEVEL
from utils.commons import now_time

# factor 计算器
class FactorCalculator:

    def __init__(self, factor, back_hour, d_num):
        self.backhour = int(back_hour)
        self.d_num = d_num

        factor_module_name = f'factors.{factor}'
        module = importlib.import_module(factor_module_name)
        self.signal_func = module.signal

        if d_num == 0:
            self.factor_name = f'{factor}_bh_{back_hour}'
        else:
            self.factor_name = f'{factor}_bh_{back_hour}_diff_{d_num}'

    def calc(self, df: pd.DataFrame):
        return self.signal_func(df, self.backhour, self.d_num, self.factor_name)


# filter 计算器
class FilterCalculator:

    def __init__(self, filter, params):
        self.filter_name = f'{filter}_fl_{params}'
        self.params = params

        filter_module_name = f'filters.{filter}'
        module = importlib.import_module(filter_module_name)
        self.signal_func = module.signal

    def calc(self, df: pd.DataFrame):
        return self.signal_func(df, self.params, self.filter_name)


def create_factor_calc_from_alpha_config(cfg):
    factor, if_reverse, back_hour, d_num, weight = cfg
    return FactorCalculator(factor, back_hour, d_num)


def create_filter_calc_from_alpha_config(cfg):
    filter, params = cfg
    return FilterCalculator(filter, params)


def fetch_swap_candle_data_and_calc_factors_filters(candle_mgr: CandleFeatherManager, symbol_list, run_time, expire_sec,
                                                    factor_calcs, filter_calcs, min_candle_num):
    unready_symbols = set(symbol_list)
    expire_time = run_time + timedelta(seconds=expire_sec)
    symbol_data = dict()

    # 算因子（忙等待）
    while True:
        while len(unready_symbols) > 0:
            readies = {s for s in unready_symbols if candle_mgr.check_ready(s, run_time)}
            if len(readies) == 0:
                break
            for sym in readies:
                df = candle_mgr.read_candle(sym)
                if len(df) < min_candle_num:
                    continue
                df['symbol'] = sym
                for factor_calc in factor_calcs:
                    factor_calc.calc(df)
                for filter_calc in filter_calcs:
                    filter_calc.calc(df)
                symbol_data[sym] = df
            unready_symbols -= readies
            logging.log(MY_DEBUG_LEVEL, 'readys=%d, unready=%d, read=%d', len(readies), len(unready_symbols),
                        len(symbol_data))
        if len(unready_symbols) == 0:
            break
        if now_time() > expire_time:
            break
        time.sleep(0.01)
    current_hour_results = [df.iloc[-1] for df in symbol_data.values()]
    df_factor = pd.DataFrame(current_hour_results).reset_index(drop=True)
    return df_factor
