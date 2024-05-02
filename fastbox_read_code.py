# %%
import fastbox as fb
import numpy as np
import pandas as pd
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
# fb.signup()
# %%
bid_list = ['CF', 'MA', 'SH', 'OI', 'SM', 'PF', 'AP', 'RM', 'CY', 'SA', 'FG', 'TA', 'PX', 'SR', 'PK', 'UR']
for bid in bid_list:
    bid_contracts = fb.futures.info(underlyingid = bid)
    contracts = bid_contracts[(bid_contracts.index>=bid+'2301')&(bid_contracts.index<=bid+'2412')].sort_index().index.tolist()
    print(bid,contracts)

    path = f'/Users/page/Downloads/data/{bid}/'
    if not os.path.exists(path):
        os.mkdir(path)

    with ThreadPoolExecutor(max_workers=4) as executor:
        for contract in contracts:
            print(f'start download {contract}')
            df = fb.data.futures.lv1(instrumentid = contract,
            from_date='2023-01-01',
            to_date='2023-12-31', )
            if df is not None:
                    df.to_parquet(path+f'{contract}.parquet')
                    time.sleep(1)
            else:
                print(contract,'no data')

    print(f'----------finish download {bid}-------------')

# %%
