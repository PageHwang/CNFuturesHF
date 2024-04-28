# CNFuturesHF

The code for the SAIF-Live course with Tower Research

futures.py 文件实现了Futures类, 实例化时只需输入数据文件的路径.

```python
futures = Futures(data_path = '数据文件夹路径')
```

数据文件夹需要按照交易所-品种-合约的层级进行排列. 数据可以直接从PageHwang的[OneDrive链接]([https://](https://saif-my.sharepoint.cn/:f:/g/personal/yxhuang_23_saif_sjtu_edu_cn/EgqwY1fpj5dKu4UD3wAnpnsBDB66Yrd890ReeP2uwELTmQ?e=QiPvaf)https://)下载.

可以实现:

1. main contract自动拼接(用rolling 3 天的交易量之和为区分指标).
   ```python
   exchange = 'CZCE'
   bid = 'AP'
   ap_concat_df = futures.get_main_contract_concat_data(exchange,bid)
   ```
2. resample到任意频率
   ```python
   ap_minu_bar = futures.resample_last(ap_concat_df,'1min')
   ```
3. 获取一个交易所内所有品种合约的某一频率的价格数据. 行为时间, 列为品种. 支持使用并行, 可以设置multi_processor=你要并行的核心的个数.
   ```
   CZCE_minu_price = futures.all_bid_minute_bar_in_one_exchange('CZCE',multi_processor=1)
   ```
