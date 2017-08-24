import time
import datetime
import tushare as ts

tick = time.time()
print(tick)
localtime = time.localtime(time.time())
print(localtime)

format = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
print(format)

date_time = datetime.datetime.now()
print(date_time)
print(date_time.date())
print((date_time + datetime.timedelta(days= -1000)))

list1 = [1,2,3,4]
list2 = [2,3,4,5]
list1.extend(list2)
print(list1)

# for i in range(1000):
#     print((date_time + datetime.timedelta(days= (-1*i))).date())

# detail_df = ts.get_tick_data(code='600560', date='2017-08-20',src = 'tt')
# print(detail_df)
a = 3
b = 5
d = a/(b*1.0)
print(d)

