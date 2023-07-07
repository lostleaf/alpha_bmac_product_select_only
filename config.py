#!/usr/bin/python3
# -*- coding: utf-8 -*-

import json
import os

import pandas as pd

from candle_manager import CandleFeatherManager
from factor_calc import (create_factor_calc_from_alpha_config, create_filter_calc_from_alpha_config)


class QuantConfig:

    def __init__(self, workdir):
        self.workdir = workdir

        cfg_path = os.path.join(workdir, 'factor_calc.json')
        cfg = json.load(open(cfg_path, 'r'))

        self.interval = cfg['interval']

        self.bmac_dir = cfg['bmac_dir']
        self.bmac_expire_sec = cfg['bmac_expire_sec']

        self.long_num = cfg['long_num']
        self.short_num = cfg['short_num']

        self.min_candle_num = cfg['min_candle_num']

        self.capital_usdt = cfg['capital_usdt']

        self.exg_mgr = CandleFeatherManager(os.path.join(self.bmac_dir, f'exginfo_{self.interval}'))
        self.candle_mgr = CandleFeatherManager(os.path.join(self.bmac_dir, f'usdt_swap_{self.interval}'))

        self.factor_cfg = cfg['factor']
        self.filter_cfgs = cfg['filters']

        self.debug = cfg.get('debug', False)

        self.factor_calcs = [create_factor_calc_from_alpha_config(self.factor_cfg)]
        self.filter_calcs = [create_filter_calc_from_alpha_config(cfg) for cfg in self.filter_cfgs]
