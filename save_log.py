import os

def save_factor(workdir, run_time, df_factor):
    # 记录本周期因子
    output_dir = os.path.join(workdir, 'factor_his')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_path = os.path.join(output_dir, run_time.strftime('coin_%Y%m%d_%H%M%S.csv.zip'))
    df_factor.to_csv(output_path, index=False)


def save_selected(workdir, run_time, df_long, df_short):
    # 记录本周期选币及仓位分配结果
    output_dir = os.path.join(workdir, 'select_his')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    drop_cols = [
        'open', 'high', 'low', 'close_time', 'volume', 'trade_num', 'taker_buy_base_asset_volume',
        'taker_buy_quote_asset_volume'
    ]
    long_path = os.path.join(output_dir, run_time.strftime('%Y%m%d_%H%M%S_long.csv.zip'))
    df_long.drop(columns=drop_cols).to_csv(long_path, index=False)
    short_path = os.path.join(output_dir, run_time.strftime('%Y%m%d_%H%M%S_short.csv.zip'))
    df_short.drop(columns=drop_cols).to_csv(short_path, index=False)