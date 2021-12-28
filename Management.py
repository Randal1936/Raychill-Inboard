import tushare as ts
import xlwings as xw
import pandas as pd
from datetime import datetime
import pymysql as ps
from sqlalchemy import create_engine
import re
import numpy as np
from alive_progress import alive_bar
from RaychillAccount import fill_distri
from RaychillAccount import Fill_err
from RaychillAccount import Par


class RayDatabase:

    def __init__(self):
        self.time = datetime.now().strftime('%Y%m%d')

        # 设置token
        token = 'XXXXXXXXXXXXXXXX'
        self.pro = ts.pro_api(token)

        # 获取当前交易日最新的股票代码和简称
        self.stock_id_list = list(self.pro.stock_basic(list_status='L').ts_code.values)

        # 获取当前交易日最新的可转债代码和简称
        self.cbond_id_list = list(self.pro.cb_basic(fields="ts_code").ts_code)

        # 将所有 id 列表汇总在一起
        self.security_id_list = self.stock_id_list + self.cbond_id_list

        # 初始化 sql 引擎
        self.engine = create_engine("mysql+pymysql://root:JYZ201230@localhost:3308/stock?charset=utf8")

        # 更新数据
        self.auto_update()
        # self.manual_update('20210712')

    def auto_update(self):
        for item in self.security_id_list:
            try:
                if item[:2] in ['11', '12']:
                    # 获取可转债行情
                    df = self.pro.cb_daily(ts_code=item, start_date=self.time, end_date=self.time)
                else:
                    # 获取股票数据
                    df = ts.pro_bar(ts_code=item, api=self.pro, adj=None, start_date=self.time, end_date=self.time)

                # 连接 SQL 库
                # df = np.array(df)
                # df = df.tolist()
                # db = ps.connect(host='localhost', port=3308, user='root', password='JYZ201230', database='stock',
                #                 charset='utf8')
                # cur = db.cursor()
                # insert_data = 'INSERT INTO `' + str(item) + '` (`ts_code`, `trade_date`, `open`, `high`, `low`, `close`, `pre_close`, `change`, `pct_chg`, `vol`, `amount`) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                # cur.executemany(insert_data, df)
                # db.commit()
                # cur.close()
                # db.close()

                # 这两行足矣

                df.to_sql(name=str(item), con=self.engine, if_exists='append', index=False, index_label=False)

            except:
                print(str(item) + " 写入数据库异常")
                pass

    def manual_update(self, start_date, end_date):
        """
        start_date: string, 格式如 '20210819'，表示要查询的起始日期
        end_date: string, 格式如 '20210819'，表示要查询的终止日期
        如果 start_date 为 None, 默认按照 end_date 查询，且只查询一天
        """
        with alive_bar(len(self.stock_id_list), force_tty=True) as bar:
            for item in self.stock_id_list:
                try:
                    # 获取股票数据
                    df = ts.pro_bar(ts_code=item, api=self.pro, adj=None, start_date=self.time, end_date=self.time)
                    df = np.array(df)
                    df = df.tolist()

                    # 连接 SQL 库
                    db = ps.connect(host='localhost', port=3308, user='root', password='JYZ201230', database='stock',
                                    charset='utf8')
                    cur = db.cursor()
                    insert_data = 'INSERT INTO `' + str(item) + '` (`ts_code`, `trade_date`, `open`, `high`, `low`, `close`, `pre_close`, `change`, `pct_chg`, `vol`, `amount`) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                    try:
                        cur.executemany(insert_data, df)
                        db.commit()
                    except:
                        table_new = 'CREATE TABLE `' + str(
                            item) + '` (`ts_code` VARCHAR(50), `trade_date` DATE, `open` FLOAT, `high` FLOAT, `low` FLOAT, `close` FLOAT, `pre_close` FLOAT, `change` FLOAT, `pct_chg` FLOAT, `vol` FLOAT, `amount` FLOAT);'
                        cur.execute(table_new)
                        db.commit()
                        cur.executemany(insert_data, df)
                        db.commit()
                    cur.close()
                    db.close()
                    bar()
                except:
                    print(str(item) + " 写入数据库异常")
                    pass


class RayAccount:
    def __init__(self, stock_pool, trade_info=None, auto_update_today=True):
        self.stock_pool = stock_pool
        self.trade_info = trade_info

        # 设置token
        token = '4d9de0d8dfa52117493771de3a6b49bfdbeaaed624d25f538788f5cf'
        self.pro = ts.pro_api(token)

        self.weekdays = {6: 'Sunday', 0: 'Monday', 1: 'Tuesday', 2: 'Wednesday',
                         3: 'Thursday', 4: 'Friday', 5: 'Saturday'}

        start_date = datetime.strptime('20210104', '%Y%m%d')
        self.time_now = datetime.today()

        # time span 是由当前时间减去 2021/1/4 (光寒入市时间) 得到的天数
        # 在这个天数的基础上加 9 就等于交易日 entry 所对应的行数 (持仓表格式)
        self.time_span = (self.time_now - start_date).days

        # 这个时间专用于 SQL 数据库查询, e.g. 20050506
        self.date_now_query = self.time_now.strftime('%Y%m%d')

        # 这个时间专用于账目记录, e.g. 2005/05/06
        self.date_now_keep = self.time_now.strftime('%Y/%m/%d')

        # 该选项若为 True，则自动按照当天日期记账
        if auto_update_today:
            self.stock_info = {}
            self.auto_query()
            self.auto_book_keeping()

    def auto_query(self):
        """
        return tuple(tuple(ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount))
        """
        # db = ps.connect(host='localhost', port=3308, user='root', password='JYZ201230', database='stock',
        #                 charset='utf8')

        # 从 SQL 数据库中获取数据

        # cur = db.cursor()
        # # 输入的交易时间字段必须是 YYYYmmdd 形式，如 19950506
        # sql = 'SELECT * FROM `'+str(key)+'` WHERE trade_date='+str(self.date_now_query)
        # cur.execute(sql)
        # # 提取所有数据，并赋值给 data
        # data = cur.fetchall()
        # db.commit()
        # cur.close()

        # 直接从 Tushare 查询数据
        rule = re.compile(u'[.A-Za-z]')  # 只保留股票代码的数字
        for item in self.stock_pool.keys():
            key = rule.sub('', item)
            df = pd.DataFrame()  # In case df referenced before assignment
            try:
                if item[:2] in ['11', '12']:
                    # 获取可转债行情
                    df = self.pro.cb_daily(ts_code=item,
                                           start_date=self.date_now_query,
                                           end_date=self.date_now_query)

                elif item[:2] in ['30', '00', '60', '68']:
                    # 获取股票数据
                    df = self.pro.daily(ts_code=item,
                                        start_date=self.date_now_query,
                                        end_date=self.date_now_query)

                elif item[:2] in ['15', '16', '18', '50', '51']:
                    # 获取基金数据
                    df = self.pro.fund_daily(ts_code=item,
                                             start_date=self.date_now_query,
                                             end_date=self.date_now_query)
                    """
                    15 开头：深交所 ETF 和分级基金
                    16 开头：深交所 LOF 和预备 LOF 基金
                    18 开头：深交所封闭式
                    
                    50 开头：上交所封闭式
                    51 开头：上交所 ETF
                    """
                self.stock_info[key] = df
            except:
                print(str(key)+' 查询数据异常')
                pass

    def manual_query(self, start_date, end_date):
        # 直接从 Tushare 查询数据
        rule = re.compile(u'[.A-Za-z]')  # 只保留股票代码的数字
        for item in self.stock_pool.keys():
            key = rule.sub('', item)
            df = pd.DataFrame()  # In case df referenced before assignment
            try:
                if item[:2] in ['11', '12']:
                    # 获取可转债行情
                    df = self.pro.cb_daily(ts_code=item,
                                           start_date=start_date,
                                           end_date=end_date)

                elif item[:2] in ['30', '00', '60', '68']:
                    # 获取股票数据
                    df = ts.pro_bar(ts_code=item,
                                    api=self.pro,
                                    adj=None,
                                    start_date=start_date,
                                    end_date=end_date)

                elif item[:2] in ['15', '16', '18', '50', '51']:
                    # 获取基金数据
                    df = self.pro.fund_daily(ts_code=item,
                                             start_date=start_date,
                                             end_date=end_date)
                    """
                    15 开头：深交所 ETF 和分级基金
                    16 开头：深交所 LOF 和预备 LOF 基金
                    18 开头：深交所封闭式

                    50 开头：上交所封闭式
                    51 开头：上交所 ETF
                    """
                self.stock_info[key] = df
            except:
                print(str(key) + ' 查询数据异常')
                pass

    def auto_book_keeping(self):

        """
        ========================================
        打开光寒账目
        ========================================
        """

        app1 = xw.App(visible=False, add_book=False)
        wb = app1.books.open(r'D:\Raychill Capital\光寒账目.xlsx')

        try:
            "------------------------------------------------------------------------"
            "Daily Entry (without transaction)"
            "------------------------------------------------------------------------"

            rule = re.compile(u'[.A-Za-z]')  # 只保留股票代码的数字
            for item in self.stock_pool.keys():
                stock_id = rule.sub('', item)
                sht = wb.sheets(str(stock_id))
                row = self.time_span + 9

                # 调用 VBA api 自动填充，以前两天为基准，填充到今天
                sourceRange = sht.range('B%s:R%s'%(row-2, row-1)).api
                fillRange = sht.range('B%s:R%s'%(row-2, row)).api
                AutoFillType = 0 # 0 代表默认，由 Excel 判断如何填充
                sourceRange.AutoFill(fillRange, AutoFillType)

                # 输入当前时间
                sht['B' + str(row)].value = self.date_now_keep

                # 输入当前持仓数
                sht['J'+str(row)].value = self.stock_pool[item]

                # 输入今日开盘价
                try:
                    # sht['L'+str(row)].value = self.stock_info[str(item)][0][2]
                    sht['L'+str(row)].value = self.stock_info[str(stock_id)].open.item()

                    # 输入今日收盘价
                    # sht['M'+str(row)].value = self.stock_info[str(item)][0][5]
                    sht['M' + str(row)].value = self.stock_info[str(stock_id)].close.item()
                except ValueError:
                    print('Price input failure  // stock id: '+str(stock_id) + ' // last price assigned')
                    sht['L' + str(row)].value = sht['M' + str(row-1)].value
                    sht['M' + str(row)].value = sht['M' + str(row-1)].value
                    pass

            "------------------------------------------------------------------------"
            "Ledger"
            "------------------------------------------------------------------------"

            sht = wb.sheets('基金损益')
            row = self.time_span + 10
            # 更改 row 后自动填充
            sourceRange = sht.range('B%s:W%s' % (row - 2, row - 1)).api
            fillRange = sht.range('B%s:W%s' % (row - 2, row)).api
            AutoFillType = 0
            sourceRange.AutoFill(fillRange, AutoFillType)
            # 填入日期
            sht['B' + str(row)].value = self.date_now_keep

            # 获取全部 sheets 名称
            rule = re.compile(u'[\[\]< >]')  # 去除特殊符号及无关字符
            sheets = []
            for sheet in wb.sheets:
                sheet = rule.sub('', str(sheet))
                sheet = sheet.replace('光寒账目.xlsx', '')
                sheet = sheet.replace('Sheet', '')
                sheets.append(sheet)

            sheets.remove('基金损益')
            sheets.remove('交易记录')
            sheets.remove('基金收益')
            sheets.remove('收益分配')
            sheets.remove('误差校正')
            sheets.remove('申赎记录')
            sheets.remove('历史持仓')
            sheets.remove('最新持仓')
            sheets.remove('-------持仓表-------')
            sheets.remove('-------净值表-------')

            # 填入日期
            sht['B' + str(row)].value = self.date_now_keep

            # 填入 fee
            fee = ['\''+sheet+'\'!F'+str(row-1) for sheet in sheets]
            sht['F' + str(row)].value = '=' + '+'.join(fee)

            # 填入 Deal P&L
            deal = ['\'' + sheet + '\'!G' + str(row-1) for sheet in sheets]
            sht['G' + str(row)].value = '=' + '+'.join(deal)

            # 填入 Floating P&L
            floating = ['\'' + sheet + '\'!H' + str(row-1) for sheet in sheets]
            sht['H' + str(row)].value = '=' + '+'.join(floating)

            # 填入 Par P&L
            par = ['\'' + sheet + '\'!C' + str(row-1) for sheet in sheets]
            sht['J' + str(row)].value = '=' + '+'.join(par)

            # 填入 Fund P&L
            fund = ['\'' + sheet + '\'!D' + str(row-1) for sheet in sheets]
            sht['K' + str(row)].value = '=' + '+'.join(fund)

            # 填入 Shares
            shares = ['\'' + sheet + '\'!Q' + str(row-1) for sheet in sheets]
            sht['M' + str(row)].value = '=' + '+'.join(shares)

            # TB 试算格式调整：如果试算数不为 0，则变为红色
            if sht.range('W'+str(row)).value != 0:
                sht.range('W'+str(row)).api.Font.Color = 0x0000ff

            # Cash, Interest, Equity 修正(自动填充容易出错)
            sht.range('N' + str(row)).value = sht.range('N' + str(row-1)).value
            sht.range('O' + str(row)).value = 0
            sht.range('P' + str(row)).value = 0

            # 更新持仓
            sht = wb.sheets('历史持仓')
            row = self.time_span + 9
            # 更改 row 后自动填充
            sourceRange = sht.range('B%s:Q%s' % (row - 2, row - 1)).api
            fillRange = sht.range('B%s:Q%s' % (row - 2, row)).api
            AutoFillType = 0
            sourceRange.AutoFill(fillRange, AutoFillType)
            sht['B' + str(row)].value = self.date_now_keep

            # 收益分配刷格式
            sht = wb.sheets('收益分配')
            row = self.time_span + 8
            # 更改 row 后自动填充
            sourceRange = sht.range('B%s:P%s' % (row - 2, row - 1)).api
            fillRange = sht.range('B%s:P%s' % (row - 2, row)).api
            AutoFillType = 0
            sourceRange.AutoFill(fillRange, AutoFillType)
            sht['B' + str(row)].value = self.date_now_keep

            # 误差校正刷格式
            sht = wb.sheets('误差校正')
            row = self.time_span + 8
            # 更改 row 后自动填充
            sourceRange = sht.range('B%s:P%s' % (row - 2, row - 1)).api
            fillRange = sht.range('B%s:P%s' % (row - 2, row)).api
            AutoFillType = 0
            sourceRange.AutoFill(fillRange, AutoFillType)
            sht['B' + str(row)].value = self.date_now_keep

            # 记录最新总资产
            sht = wb.sheets('基金损益')
            row = self.time_span + 10
            total_asset = sht.range('Q%s' % row).value

            # 基金收益刷格式
            sht = wb.sheets('基金收益')
            row = self.time_span + 7
            # 更改 row 后自动填充
            sourceRange = sht.range('B%s:I%s' % (row - 2, row - 1)).api
            fillRange = sht.range('B%s:I%s' % (row - 2, row)).api
            AutoFillType = 0
            sourceRange.AutoFill(fillRange, AutoFillType)
            sht.range('H%s' % row).value = total_asset
            sht['B' + str(row)].value = self.date_now_keep

            # 保存账目
            wb.save()
            wb.close()
        finally:
            app1.quit()

        # 计算净值
        "Dividend"
        fill_distri.FillDist(end_date=self.date_now_query, start_date=None)

        "Errors and Residuals"
        Fill_err.FillErr(end_date=self.date_now_query, start_date=None)

        "Par_profit"
        Par.FillPar(end_date=self.date_now_query, start_date=None)

        print('☑=======================')
        print('Weekday: '+self.weekdays[self.time_now.weekday()])
        print(self.date_now_keep + ' Book keeping complete')
        print('☑=======================')


# Manual run
# RayAccount({'601328.SH': 900, '600900.SH': 300, '601728.SH': 400,
#                         '000785.SZ': 400, '002967.SZ': 200, '000725.SZ': 100})

