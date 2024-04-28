# %%
import numpy as np
import pandas as pd
pd.set_option('display.max_columns',100)
import os
import datetime
from tqdm import tqdm # type: ignore
from joblib import Parallel, delayed # type: ignore
# %%
class Futures():
    def __init__(self, data_path='C:/users/yuxuan.huang/OneDrive - SAIF/tower data/') -> None:
        self.data_path = data_path
        self.exchanges = [d for d in os.listdir(self.data_path) if os.path.isdir(os.path.join(self.data_path, d))]

    @property
    def info(self):
        return pd.read_csv(self.data_path+'future_info.csv',index_col=0)
    
    @property
    def bid_list(self):
        bid_list = []
        for exchange in self.exchanges:
            bids = os.listdir(self.data_path+'/'+exchange)
            for bid in bids:
                bid_list.append(bid)
        return bid_list

    @property
    def exchange2bid2contract_dict(self):
        exchange2bid2contract_dict = {}

        for exchange in self.exchanges:
            exchange2bid2contract_dict[exchange] = {}
            bids = os.listdir(self.data_path+'/'+exchange)
            for bid in bids:
                exchange2bid2contract_dict[exchange][bid] = {}
                contract_file_name_list = os.listdir(self.data_path+exchange+'/'+bid+'/')
                for contract_file_name in contract_file_name_list:
                    contract_path = self.data_path+exchange+'/'+bid+'/'+contract_file_name
                    contract = contract_file_name[:6]
                    exchange2bid2contract_dict[exchange][bid][contract] = contract_path
        return exchange2bid2contract_dict
    
    def get_daily_vol(self,bid):
        exchange = self.info.loc[bid,'exchange']
        contract_list = list(self.exchange2bid2contract_dict[exchange][bid].keys())
        daily_vol_df = pd.DataFrame()
        for contract in contract_list:
            file_path = self.exchange2bid2contract_dict[exchange][bid][contract]

            df = pd.read_parquet(file_path)

            df = df[(df.index.time >= pd.to_datetime('08:59').time()) & 
                    (df.index.time <= pd.to_datetime('15:00').time())]
            
            daily_vol_ser = df.resample('1D')['vol'].last()
            daily_vol_ser.name = contract[:6]
            daily_vol_ser.index = pd.to_datetime(daily_vol_ser.index).date

            daily_vol_df = pd.concat([daily_vol_df,daily_vol_ser],axis=1)

        daily_vol_df.dropna(axis=0, how='all', inplace=True)
        daily_vol_df.sort_index(inplace=True)
        return daily_vol_df

    def find_main_contract(self,bid):
        daily_vol_df = self.get_daily_vol(bid)

        rolling3mean = daily_vol_df.rolling(3,min_periods=1).mean()
        rolling3mean.fillna(0,inplace=True)
        
        main_contracts = rolling3mean.idxmax(axis=1)
        
        return main_contracts

    def cancel_callback(self,main_contracts):
        history_contracts = dict()
        last_contract = main_contracts.iloc[0]
        for date, contract in main_contracts.items():
            knock = {}
            if contract!=last_contract:
                if contract in history_contracts.keys():
                    knock[contract] = knock.get(contract,0) + 1
                    if knock[contract] >= 3:
                        print(f"{last_contract} call back but eventually replaced by {contract}")
                        last_contract = contract
                    else:
                        print(f"{contract} was replaced at {history_contracts[contract]}, but was called back on {date}")
                        main_contracts[date] = last_contract
                else:
                    history_contracts[last_contract] = date
                    last_contract = contract
        return main_contracts

    def get_main_contract_concat_data(self,bid):
        main_contracts = self.find_main_contract(bid)
        main_contracts = self.cancel_callback(main_contracts)

        exchange = self.info.loc[bid,'exchange']

        contract_list = list(self.exchange2bid2contract_dict[exchange][bid].keys())

        concat_df = pd.DataFrame()

        changed_times = 0

        for contract in contract_list:
            time_being_main = main_contracts.index[main_contracts==contract]
            if not time_being_main.empty:
                time_start, time_end = min(time_being_main), max(time_being_main)

                time_start = pd.Timestamp(datetime.datetime.combine(time_start, datetime.time(8, 59))).tz_localize('Asia/Shanghai')
                
                time_end = pd.Timestamp(datetime.datetime.combine(time_end, datetime.time(15, 00))).tz_localize('Asia/Shanghai')

                df = pd.read_parquet(self.exchange2bid2contract_dict[exchange][bid][contract])

                df = df[(df.index >= time_start) & 
                        (df.index <= time_end)]
                
                df = df[(df.index.time >= pd.to_datetime('08:59').time()) & 
                        (df.index.time <= pd.to_datetime('15:00').time())]
                
                if concat_df.empty:
                    adjust_coefficient = 1
                else:
                    last_adjust_coefficient = concat_df['adjust_coefficient'].iloc[-1]
                    last_main_contract_close = concat_df['last_price'].iloc[-1]
                    current_main_contract_open = df['last_price'].iloc[0]
                    adjust_coefficient = last_adjust_coefficient*current_main_contract_open/last_main_contract_close

                df['changed_times'] = changed_times
                changed_times += 1

                df['adjust_coefficient'] = adjust_coefficient

                concat_df = pd.concat([concat_df,df])

        return concat_df
    
    def get_main_contract_minute_price(self,bid):
        df = self.get_main_contract_concat_data(bid)
        df = self.resample_last(df,'1min')
        ser = df['last_price'] / df['adjust_coefficient'].fillna(1)
        ser.name = bid + '_last_price'
        return ser
    
    def all_bids_minute_bar_in_one_exchange(self,exchange,multi_processor=4):
        bids = self.exchange2bid2contract_dict[exchange].keys()

        res = Parallel(n_jobs=multi_processor)(delayed(self.get_main_contract_minute_price)(bid) for bid in tqdm(bids))

        price_df = pd.concat(res,axis=1,join='outer')

        return price_df
    
    def all_bids_minute_bar(self,multi_processor=4):
        bids = self.bid_list

        res = Parallel(n_jobs=multi_processor)(delayed(self.get_main_contract_minute_price)(bid) for bid in tqdm(bids))

        price_df = pd.concat(res,axis=1,join='outer')

        return price_df

    @classmethod
    def resample_last(cls,df,resample_frequency):
        df['resample_note'] = df.index.floor(resample_frequency)
        groups = df.groupby('resample_note')
        
        res_df = groups.agg('last')
        res_df.index = res_df.index.tz_convert('Asia/Shanghai')

        return res_df

if __name__=='__main__':

    futures = Futures(data_path = 'C:/Users/PageHwang/OneDrive - SAIF/tower data/')

    exchange = 'CZCE'
    bid = 'AP'
    ap_concat_df = futures.get_main_contract_concat_data(exchange,bid)
    ap_minu_bar = futures.resample_last(ap_concat_df,'1min')
    ap_minu_bar
    CZCE_minu_price = futures.all_bid_minute_bar_in_one_exchange('CZCE',multi_processor=1)
    CZCE_minu_price
