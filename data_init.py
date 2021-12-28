import tushare as ts
import pymysql as ps
from sqlalchemy import create_engine
import time
import numpy as np
from alive_progress import alive_bar


def get_data(code, api, start, end):
    df = ts.pro_bar(ts_code=code, api=api, adj=None, start_date=start, end_date=end)
    return df


# 获取当前交易日最新的股票代码和简称
def get_code():
    codes = pro.stock_basic(list_status='L').ts_code.values
    return codes


# 更新数据或下载其他期间数据
def update_sql(start, end, db_name):
    from datetime import datetime, timedelta
    for code in get_code():
        data = get_data(code, start, end)
        insert_sql(data, db_name)
    print(f'{start}:{end}期间数据已成功更新')


# 设置token
token = '4d9de0d8dfa52117493771de3a6b49bfdbeaaed624d25f538788f5cf'
pro = ts.pro_api(token)

stock_id_list = get_code()

# 在SQL库中批量建表
for item in stock_id_list:
    db = ps.connect(host='localhost', port=3308, user='root', password='JYZ201230', database='stock', charset='utf8')
    cur = db.cursor()
    # 使用pymysql写入，非常麻烦
    table_new = 'CREATE TABLE `'+str(item)+'` (`ts_code` VARCHAR(50), `trade_date` DATE, `open` FLOAT, `high` FLOAT, `low` FLOAT, `close` FLOAT, `pre_close` FLOAT, `change` FLOAT, `pct_chg` FLOAT, `vol` FLOAT, `amount` FLOAT);'
    cur.execute(table_new)
    db.commit()
    cur.close()
    db.close()

# 在 SQL 中直接写入数据
for item in stock_id_list:
    df = get_data(item, api=pro, start='20210101', end='20210712')
    try:
        engine = create_engine("mysql+pymysql://root:JYZ201230@localhost:3308/stock?charset=utf8")
        df.to_sql(name=str(item), con=engine, if_exists='append', index=False, index_label=False)
    except:
        print(str(item) + ' 写入数据库异常')


# 在 SQL 中 Update 数据
with alive_bar(len(stock_id_list), force_tty=True) as bar:
    for item in stock_id_list:
        try:
            insert_data = 'INSERT INTO `' + str(item) + '` (`ts_code`, `trade_date`, `open`, `high`, `low`, `close`, `pre_close`, `change`, `pct_chg`, `vol`, `amount`) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
            df = get_data(item, api=pro, start='19901126', end='19990913')
            df = np.array(df)
            df = df.tolist()
            # 连接 SQL 库
            db = ps.connect(host='Localhost', port=3308, user='root', password='JYZ201230', database='stock',
                            charset='utf8')
            cur = db.cursor()
            cur.executemany(insert_data, df)
            db.commit()
            cur.close()
            db.close()
            bar()
        except:
            print(str(item)+"写入数据库异常")

