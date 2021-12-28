import schedule
from RaychillAccount import Management
import time

# schedule.every().day.at("16:05").do(Management.RayDatabase)
schedule.every().day.at("17:30").do(Management.RayAccount,
                                    {'601328.SH': 900, '600900.SH': 100, '000785.SZ': 0, '002967.SZ': 0,
                                     '000725.SZ': 100, '600399.SH': 0, '000807.SZ': 100})

# 最后两句可以让程序一直运行，每30s检测一次是否有可执行的任务
while True:
    schedule.run_pending()
    time.sleep(30)

