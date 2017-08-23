import tushare as ts
import time

def huge_valume(stock_df,date_str,type):
    return_value = False
    detail_df = ts.get_tick_data(stock_df.name, date_str,src = type)

    if type == 'tt' :
        if not detail_df is None and detail_df.size > 0:
            if detail_df.iloc[0]['time'] < '09:30:00':
                if stock_df['outstanding'] < 20 and stock_df['outstanding'] > 10:
                    if detail_df.iloc[0]['volume'] > 6000:
                        return_value = True
                elif stock_df['outstanding'] <= 10 and stock_df['outstanding'] > 6 :
                    if detail_df.iloc[0]['volume'] > 3000:
                        return_value = True
                elif stock_df['outstanding'] <= 6 and stock_df['outstanding'] > 4:
                    if detail_df.iloc[0]['volume'] > 2500:
                        return_value = True
                elif stock_df['outstanding'] <= 4 and stock_df['outstanding'] > 2:
                    if detail_df.iloc[0]['volume'] > 2000:
                        return_value = True
                elif stock_df['outstanding'] <= 2 and stock_df['outstanding'] > 1:
                    if detail_df.iloc[0]['volume'] > 1500:
                        return_value = True
                elif stock_df['outstanding'] <= 1 and stock_df['outstanding'] > 0.6:
                    if detail_df.iloc[0]['volume'] > 1000:
                        return_value = True
                elif stock_df['outstanding'] <= 0.6 :
                    if detail_df.iloc[0]['volume'] > 800:
                        return_value = True
    elif type == 'sn' :
        if detail_df.iloc[-1]['time'] < '09:30:00':
            if stock_df['outstanding'] < 20 and stock_df['outstanding'] > 10:
                if detail_df.iloc[-1]['volume'] > 6000:
                    return_value = True
            elif stock_df['outstanding'] <= 10 and stock_df['outstanding'] > 6:
                if detail_df.iloc[-1]['volume'] > 3000:
                    return_value = True
            elif stock_df['outstanding'] <= 6 and stock_df['outstanding'] > 4:
                if detail_df.iloc[-1]['volume'] > 2500:
                    return_value = True
            elif stock_df['outstanding'] <= 4 and stock_df['outstanding'] > 2:
                if detail_df.iloc[-1]['volume'] > 2000:
                    return_value = True
            elif stock_df['outstanding'] <= 2 and stock_df['outstanding'] > 1:
                if detail_df.iloc[-1]['volume'] > 1500:
                    return_value = True
            elif stock_df['outstanding'] <= 1 and stock_df['outstanding'] > 0.6:
                if detail_df.iloc[-1]['volume'] > 1000:
                    return_value = True
            elif stock_df['outstanding'] <= 0.6:
                if detail_df.iloc[-1]['volume'] > 800:
                    return_value = True
    return return_value


total_stock = ts.get_stock_basics()
count = 0
huge_set = set()
for index,stock in total_stock.iterrows():
    count += 1
    print(str(count)+':'+ str(len(huge_set)))
    # time.sleep(0.2)
    try:
        if huge_valume(stock_df = stock,date_str='2017-08-22',type = 'tt'):
            huge_set.add(stock.name)
    except BaseException:
        print('error code :' + stock.name)
print('-------------sep-----------')
print(huge_set)