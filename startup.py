#!/usr/bin/python3
# -*- coding: utf-8 -*-

import logging
import sys
import time
import traceback
from datetime import timedelta
from decimal import Decimal

import pandas as pd
from factor_calc import fetch_swap_candle_data_and_calc_factors_filters

from config import QuantConfig
from position import assign_position, filter_before, filiter_nan_inf, select_coin
from market import (get_fundingrate, load_market)
from save_log import save_factor, save_selected
from utils.commons import (MY_DEBUG_LEVEL, next_run_time, round_to_tick, sleep_until_run_time)

sys.stdout.reconfigure(encoding='utf-8')

# 调试用，实盘修改参数 level=logging.INFO
logging.addLevelName(MY_DEBUG_LEVEL, 'MyDebug')
logging.basicConfig(format='%(asctime)s (%(levelname)s) - %(message)s', level=MY_DEBUG_LEVEL, datefmt='%Y%m%d %H:%M:%S')


def run_loop(Q: QuantConfig):
    run_time = next_run_time('1h')
    if Q.debug:
        run_time -= timedelta(hours=1)

    # sleep 到小时开始
    logging.info(f'Next run time: {run_time}')
    sleep_until_run_time(run_time)

    # 1 加载市场信息
    df_exg = load_market(Q.exg_mgr, run_time, Q.bmac_expire_sec)
    symbol_list = list(df_exg['symbol'])
    df_exg.set_index('symbol', inplace=True)

    logging.info('获取当前周期合约完成')
    logging.log(MY_DEBUG_LEVEL, '\n' + str(df_exg.head(2)))

    # 2 获取当前资金费率
    df_funding = get_fundingrate(Q.exg_mgr, run_time, Q.bmac_expire_sec)
    logging.info('获取资金费数据完成')
    logging.log(MY_DEBUG_LEVEL, '\n' + str(df_funding.tail(3)))

    # 3 算因子
    df_factor = fetch_swap_candle_data_and_calc_factors_filters(Q.candle_mgr, symbol_list, run_time, Q.bmac_expire_sec,
                                                                Q.factor_calcs, Q.filter_calcs, Q.min_candle_num)

    logging.info('计算所有币种K线因子完成')
    df_factor_all = df_factor.copy()

    # 4 过滤
    factor_filter_cols = [c.factor_name for c in Q.factor_calcs] + [c.filter_name for c in Q.filter_calcs]
    df_factor = filiter_nan_inf(df_factor, factor_filter_cols)
    df_factor = filter_before(df_factor)  # 前置过滤

    # 5 选币
    df_long, df_short = select_coin(df_factor, Q.factor_calcs[0].factor_name, Q.factor_cfg[1], Q.long_num, Q.short_num)

    # 6 分配仓位
    df_long, df_short = assign_position(df_long, df_short, df_exg, Q.capital_usdt, Q.long_num, Q.short_num)

    # 7 记录因子和选币结果
    save_selected(Q.workdir, run_time, df_long, df_short)  # 将本轮最新选币存储在 select_his 文件夹下
    save_factor(Q.workdir, run_time, df_factor_all)  # 将本轮计算的最新因子存储在 factor_his 文件夹下

    drop_cols = [
        'open', 'high', 'low', 'close_time', 'volume', 'trade_num', 'taker_buy_base_asset_volume',
        'taker_buy_quote_asset_volume'
    ]
    logging.log(MY_DEBUG_LEVEL, '\n' + str(df_exg.loc[list(df_long['symbol']) + list(df_short['symbol'])]))

    logging.log(MY_DEBUG_LEVEL, 'Short\n' + str(df_short.drop(columns=drop_cols)))
    logging.log(MY_DEBUG_LEVEL, 'Long\n' + str(df_long.drop(columns=drop_cols)))

    if Q.debug:
        exit()


def main(workdir):
    # 初始化配置
    Q = QuantConfig(workdir)

    while True:
        try:
            while True:
                run_loop(Q)
        except Exception as err:
            logging.error('系统出错, 10s之后重新运行, 出错原因: ' + str(err))
            traceback.print_exc()
            time.sleep(10)


if __name__ == '__main__':
    main(sys.argv[1])
