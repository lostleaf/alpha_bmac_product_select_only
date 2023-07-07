#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import time
from datetime import datetime, timedelta
from decimal import Decimal

import numpy as np
import pandas as pd
import pytz

DEFAULT_TZ = pytz.timezone('hongkong')
MY_DEBUG_LEVEL = 15  # DEBUG(10) < 自定义 logging 等级 < INFO(20)


def next_run_time(time_interval, ahead_seconds=5):
    """
    =====辅助功能函数, 下次运行时间
    根据time_interval，计算下次运行的时间，下一个整点时刻。
    目前只支持分钟和小时。
    :param time_interval: 运行的周期，15m，1h
    :param ahead_seconds: 预留的目标时间和当前时间的间隙
    :return: 下次运行的时间
    案例：
    15m  当前时间为：12:50:51  返回时间为：13:00:00
    15m  当前时间为：12:39:51  返回时间为：12:45:00
    10m  当前时间为：12:38:51  返回时间为：12:40:00
    5m  当前时间为：12:33:51  返回时间为：12:35:00

    5m  当前时间为：12:34:51  返回时间为：12:40:00

    30m  当前时间为：21日的23:33:51  返回时间为：22日的00:00:00

    30m  当前时间为：14:37:51  返回时间为：14:56:00

    1h  当前时间为：14:37:51  返回时间为：15:00:00

    """
    if time_interval.endswith('m') or time_interval.endswith('h'):
        pass
    elif time_interval.endswith('T'):
        time_interval = time_interval.replace('T', 'm')
    elif time_interval.endswith('H'):
        time_interval = time_interval.replace('H', 'h')
    else:
        raise ValueError(f'{time_interval} 格式不符合规范。程序exit')

    ti = pd.to_timedelta(time_interval)
    now = now_time()
    # now = datetime(2019, 5, 9, 23, 50, 30)  # 修改now，可用于测试
    this_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    min_step = timedelta(minutes=1)

    target_time = now.replace(second=0, microsecond=0)

    while True:
        target_time = target_time + min_step
        delta = target_time - this_midnight
        if delta.seconds % ti.seconds == 0 and (target_time - now).seconds >= ahead_seconds:
            # 当符合运行周期，并且目标时间有足够大的余地，默认为60s
            break

    return target_time


# ===依据时间间隔, 自动计算并休眠到指定时间
def sleep_until_run_time(run_time):
    # sleep
    time.sleep(max(0, (run_time - now_time()).total_seconds() - 1))
    while now_time() < run_time:  # 在靠近目标时间时
        time.sleep(0.001)


def now_time():
    return datetime.now(DEFAULT_TZ)


def wait_until_ready(mgr, symbol, run_time, expire_time):
    while not mgr.check_ready(symbol, run_time):
        time.sleep(0.01)
        if now_time() > expire_time:
            return False

    return True


def round_to_tick(value: Decimal, tick: Decimal) -> Decimal:
    """
    Round price/size to tick value.
    """
    rounded = Decimal(int(round(value / tick)) * tick)
    return rounded