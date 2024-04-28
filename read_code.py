# %%
import fastbox as fb
import numpy as np
import pandas as pd
import os
import time
# fb.signup()
# %%
ap2401 = fb.data.futures.lv2(instrumentid = 'AP2401', 
        from_date='2023-01-01',
        to_date='2023-12-31', )
ap2401
# %%
bid_list = ['AP', 'CF', 'CJ', 'CY', 'ER', 'FG', 'JR', 'LR', 'MA', 'ME', 'OI', 'PF', 'PK', 'PM', 'PX']
for bid in bid_list:
    bid_contracts = fb.futures.info(underlyingid = bid)
    contracts = bid_contracts[(bid_contracts.index>=bid+'2301')&(bid_contracts.index<=bid+'2412')].sort_index().index.tolist()
    print(bid,contracts)
# %%
    for contract in contracts:
        df = fb.data.futures.lv2(instrumentid = contract,
        from_date='2023-01-01',
        to_date='2023-12-31', )
        if df is not None:
                df.to_parquet('data/{}/{}.parquet'.format(bid,contract))
                time.sleep(60)
        else:
             print(contract,'no data')

# %%
