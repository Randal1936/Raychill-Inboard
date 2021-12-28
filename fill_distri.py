import xlwings as xw
import os
import string
from datetime import datetime

os.chdir(r'D:\Raychill Capital')


class FillDist:

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.alphabet = list(string.ascii_uppercase)
        self.letters = ['F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P']
        self.refs = ['G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q']
        self._map = dict([(letter, ref) for letter, ref in zip(self.letters, self.refs)])

        # Interact with the workbook
        app = xw.App(visible=False, add_book=False)
        try:
            wb = app.books.open('./光寒账目.xlsx')
            sht = wb.sheets('收益分配')
            gap = str(self.date_loc(self.end_date))

            if start_date is None:
                # Input one day only
                for letter, ref in self._map.items():
                    loc = letter + gap
                    sht[loc].value = '=$D${0}*历史持仓!{1}{0}/历史持仓!$D${0}'.format(gap, self._map[letter])

            else:
                # Input data day by day from start to the end
                start_gap = str(self.date_loc(self.start_date))
                gaps = range(int(start_gap), int(gap)+1, 1)
                for gap in gaps:
                    for letter, ref in self._map.items():
                        loc = letter + str(gap)
                        sht[loc].value = '=$D${0}*历史持仓!{1}{0}/历史持仓!$D${0}'.format(gap, self._map[letter])

            wb.save()
        finally:
            app.quit()

    def date_loc(self, date):
        """
        date: datetime.datetime
        return: string, the row number according to the date
        """
        if type(date) is str:
            try:
                date = datetime.strptime(date, '%Y/%m/%d')
            except ValueError:
                try:
                    date = datetime.strptime(date, '%Y-%m-%d')
                except ValueError:
                    try:
                        date = datetime.strptime(date, '%Y%m%d')
                    except ValueError:
                        raise ValueError('time data {0} does not match format'.format(date))
                    pass
                pass

        init_day = datetime.strptime('20201227', '%Y%m%d')
        return (date - init_day).days



