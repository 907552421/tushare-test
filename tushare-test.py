import tushare as ts
import numpy as np
import pandas as pd
import threading
import Queue
import time
import datetime

def huge_valume(stock_df,date_str,type,q):
    detail_df = ts.get_tick_data(code=stock_df.name, date=date_str,src = type)

    if type == 'tt' :
        if not detail_df is None and detail_df.size > 0:
            if detail_df.iloc[0]['time'] < '09:30:00':
                if stock_df['outstanding'] < 20 and stock_df['outstanding'] > 10:
                    if detail_df.iloc[0]['volume'] > 6000:
                        q.put(stock_df)
                elif stock_df['outstanding'] <= 10 and stock_df['outstanding'] > 6 :
                    if detail_df.iloc[0]['volume'] > 3000:
                        q.put(stock_df)
                elif stock_df['outstanding'] <= 6 and stock_df['outstanding'] > 4:
                    if detail_df.iloc[0]['volume'] > 2500:
                        q.put(stock_df)
                elif stock_df['outstanding'] <= 4 and stock_df['outstanding'] > 2:
                    if detail_df.iloc[0]['volume'] > 2000:
                        q.put(stock_df)
                elif stock_df['outstanding'] <= 2 and stock_df['outstanding'] > 1:
                    if detail_df.iloc[0]['volume'] > 1500:
                        q.put(stock_df)
                elif stock_df['outstanding'] <= 1 and stock_df['outstanding'] > 0.6:
                    if detail_df.iloc[0]['volume'] > 1000:
                        q.put(stock_df)
                elif stock_df['outstanding'] <= 0.6 :
                    if detail_df.iloc[0]['volume'] > 800:
                        q.put(stock_df)
    elif type == 'sn' :
        if detail_df.iloc[-1]['time'] < '09:30:00':
            if stock_df['outstanding'] < 20 and stock_df['outstanding'] > 10:
                if detail_df.iloc[-1]['volume'] > 6000:
                    q.put(stock_df)
            elif stock_df['outstanding'] <= 10 and stock_df['outstanding'] > 6:
                if detail_df.iloc[-1]['volume'] > 3000:
                    q.put(stock_df)
            elif stock_df['outstanding'] <= 6 and stock_df['outstanding'] > 4:
                if detail_df.iloc[-1]['volume'] > 2500:
                    q.put(stock_df)
            elif stock_df['outstanding'] <= 4 and stock_df['outstanding'] > 2:
                if detail_df.iloc[-1]['volume'] > 2000:
                    q.put(stock_df)
            elif stock_df['outstanding'] <= 2 and stock_df['outstanding'] > 1:
                if detail_df.iloc[-1]['volume'] > 1500:
                    q.put(stock_df)
            elif stock_df['outstanding'] <= 1 and stock_df['outstanding'] > 0.6:
                if detail_df.iloc[-1]['volume'] > 1000:
                    q.put(stock_df)
            elif stock_df['outstanding'] <= 0.6:
                if detail_df.iloc[-1]['volume'] > 800:
                    q.put(stock_df)
    if q.qsize() % 10 == 0:
        print(q.qsize())

def batch_huge_volume(stock_dfs,q,date_str,type):
    for index,stock in stock_dfs.iterrows():
        huge_valume(stock_df=stock,date_str=date_str,type=type,q=q)

def muliti_thread_huge_volume(total_stock,date_str,type):
    q = Queue.Queue()
    threads = []
    thread_count = 20
    for i in range(thread_count):
        index_start = total_stock.index.size / thread_count * i
        index_end = total_stock.index.size / thread_count * (i+1)
        batch_stock = total_stock.iloc[index_start:index_end,:]
        t = threading.Thread(target=batch_huge_volume,args = (batch_stock,q,date_str,type))
        t.start()
        threads.append(t)
    for thread in threads:
        thread.join()
    results = []
    for _ in range(thread_count):
        results.append(q.get())
    return results

def high_opening(stock_code,high_opening_list,date_str,last_date_str):
    result_df = ts.get_k_data(stock_code,start = last_date_str,end=date_str)
    # print result_df
    if not result_df is None and result_df.index.size == 2:
        if result_df.iloc[0]['close'] < result_df.iloc[1]['open']:
            high_opening_list.append(stock_code)

def limit_up(stock_code,limit_up_list,date_str,last_date_str) :
    result_df = ts.get_k_data(stock_code, start=last_date_str, end=date_str)
    if not result_df is None and result_df.index.size == 2:
        if result_df.iloc[0]['close'] * 1.09 < result_df.iloc[1]['close']:
            limit_up_list.append(stock_code)

def single_date_test():
    total_stock = ts.get_stock_basics()
    start = time.time()
    results = muliti_thread_huge_volume(total_stock=total_stock, date_str='2017-08-22', type='tt')
    end = time.time()
    interval = end - start
    print(interval)
    high_opening_list = []
    for stock in results:
        high_opening(stock, high_opening_list)
    limit_up_list = []
    for high_open_stock in high_opening_list:
        limit_up(high_open_stock, limit_up_list=limit_up_list)
    print

def batch_date_test():
    total_stock = ts.get_stock_basics()
    today = datetime.datetime.now()
    huge_high_list = []
    huge_high_limit_list = []
    for i in range(10):
        date = today + datetime.timedelta(days=(-1 * (i + 1)))
        last_date = date + datetime.timedelta( days= (-1))
        date_str = date.strftime('%Y-%m-%d')
        last_date_str = last_date.strftime('%Y-%m-%d')
        print('%s :', date_str)
        start = time.time()
        results = muliti_thread_huge_volume(total_stock=total_stock, date_str=date_str, type='tt')
        end = time.time()
        interval = end - start
        print(interval)
        high_opening_list = []
        for stock in results:
            high_opening(stock_code= stock.name, high_opening_list= high_opening_list,date_str=date_str,last_date_str=last_date_str)
        huge_high_list.extend(high_opening_list)
        limit_up_list = []
        for high_open_stock_code in high_opening_list:
            limit_up(stock_code= high_open_stock_code, limit_up_list=limit_up_list,date_str=date_str,last_date_str=last_date_str)
        huge_high_limit_list.extend(limit_up_list)

    result = (huge_high_limit_list *1.0) / (huge_high_list * 1.0)
    print(result)

if __name__ == '__main__':
    batch_date_test()