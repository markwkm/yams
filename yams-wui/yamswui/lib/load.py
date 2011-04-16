from webhelpers.html import literal

from sqlalchemy.sql.expression import text

from yamswui.lib.helpers import YamsChart
from yamswui.model.meta import Session


class LoadChart(YamsChart):
    def __init__(self, host, duration=None, end_ctime=None):
        self.tablename = 'vl_load'
        self.type = None
        self.type_instance = None
        YamsChart.__init__(self, host, duration, end_ctime)

    def _get_data(self):
        if self.details is None:
            return

        transaction = self.connection.begin()
        tuples = self.connection.execute(text(
"""SELECT EXTRACT(EPOCH FROM time) * 1000 AS time, values
FROM vl_load
WHERE time > :starttime
  AND time <= :endtime
  AND host = :name
ORDER BY time ASC;"""), name=self.host, starttime=self.dates[0],
                endtime=self.dates[1])
        transaction.commit()

        if tuples.rowcount < 1:
            return

        self.data = list()
        vl = dict()

        for name in self.details['dsnames']:
            vl[name] = list()

        for row in tuples:
            ctime = int(row['time'])
            index = 0
            for name in self.details['dsnames']:
                vl[name].append('[%d, %f]' % (ctime, row['values'][index]))
                index += 1

        for name in self.details['dsnames']:
            self.data.append(', '.join(vl[name]))

    def javascript(self):
        return YamsChart.javascript(self, 'Load')
