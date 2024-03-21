import pandas as pd
import requests
import json
import os

pd.set_option('display.max_rows', None)  # 设置显示最大行数为无穷大
pd.set_option('display.max_columns', None)  # 设置显示最大列数为无穷大

def Craw_hugutong(count):
    """
    东方财富沪股通数据
    count  #下载个数  1为更新当天
    :return:
    """

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0"
    }

    json_url = f'https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery112306696264964829428_1705659716265&sortColumns=TRADE_DATE&sortTypes=-1&pageSize={count}&pageNumber=1&reportName=RPT_MUTUAL_DEAL_HISTORY&columns=ALL&source=WEB&client=WEB&filter=(MUTUAL_TYPE%3D%22001%22)'
    print(json_url)

    res = requests.get(json_url, headers=headers)
    result = res.text.split("jQuery112306696264964829428_1705659716265")[1].split("(")[1].split(");")[0]
    result_json = json.loads(result)


    dd= pd.DataFrame(result_json["result"]["data"])

    dd.columns = ["id",'日期','当日资金流入','当日成交净买额','当日余额','历史累计净买额','买入成交额','卖出成交额',"领涨股代码",'领涨股','领涨股涨跌幅','上证指数','上证指数涨跌幅','p']
    del dd['id']

    dd = dd.head(count)

    return dd



data = Craw_hugutong(20)

print(data)


# import pymongo
#
# myclient = pymongo.MongoClient("mongodb://localhost:27017/")
# mydb = myclient["py_db"]
# mycol = mydb["one"]
#
# mycol.insert_many(data)
