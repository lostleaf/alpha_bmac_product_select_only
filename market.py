#!/usr/bin/python3
# -*- coding: utf-8 -*-

from datetime import timedelta

import pandas as pd

from candle_manager import CandleFeatherManager
from utils.commons import now_time, wait_until_ready

MY_DEBUG_LEVEL = 15  # between DEBUG(10) and INFO(20)


def load_market(exg_mgr: CandleFeatherManager, run_time, bmac_expire_sec):
    # 从 BMAC 读取合约列表
    expire_time = run_time + timedelta(seconds=bmac_expire_sec)
    is_ready = wait_until_ready(exg_mgr, 'exginfo', run_time, expire_time)

    if not is_ready:
        raise RuntimeError(f'exginfo not ready at {now_time()}')

    df_exg: pd.DataFrame = exg_mgr.read_candle('exginfo')
    return df_exg


def get_fundingrate(exg_mgr: CandleFeatherManager, run_time, expire_sec):
    # 从 BMAC 读取资金费
    expire_time = run_time + timedelta(seconds=expire_sec)
    is_ready = wait_until_ready(exg_mgr, 'funding', run_time, expire_time)

    if not is_ready:
        raise RuntimeError(f'Funding rate not ready')

    return exg_mgr.read_candle('funding')
