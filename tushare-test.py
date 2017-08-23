import tushare as ts
import threading
import Queue
import time

def huge_valume(stock_df,date_str,type,q):
    detail_df = ts.get_tick_data(stock_df.name, date_str,src = type)

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


if __name__ == '__main__':
    total_stock = ts.get_stock_basics()
    start = time.time()
    results = muliti_thread_huge_volume(total_stock= total_stock,date_str='2017-08-22',type ='tt')
    end = time.time()
    interval =  end - start
    print(interval)
    print
#
# count = 0
# huge_set = set()
# for index,stock in total_stock.iterrows():
#     count += 1
#     print(str(count)+':'+ str(len(huge_set)))
#     # time.sleep(0.2)
#     try:
#         if huge_valume(stock_df = stock,date_str='2017-08-22',type = 'tt'):
#             huge_set.add(stock.name)
#     except BaseException:
#         print('error code :' + stock.name)
# print('-------------sep-----------')
# print(huge_set)