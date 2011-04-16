from webhelpers.html import literal

from sqlalchemy.sql.expression import text

from yamswui.lib.helpers import YamsChartTypeTypeInstance
from yamswui.model.meta import Session


class VmemChart(YamsChartTypeTypeInstance):
    def __init__(self, host, type, type_instance, duration=None,
            end_ctime=None):
        self.tablename = 'vl_vmem'
        YamsChartTypeTypeInstance. __init__(self, host, type, type_instance,
                duration, end_ctime)
        self.ylabel = 'Pages'

    def _get_data(self):
        if self.details is None:
            return

        transaction = self.connection.begin()
        tuples = self.connection.execute(text(
"""SELECT EXTRACT(EPOCH FROM time) * 1000 AS time, values
FROM vl_vmem
WHERE time > :starttime
  AND time <= :endtime
  AND host = :name
  AND type = :type
  AND type_instance = :type_instance
ORDER BY time ASC;"""), name=self.host, type=self.type,
                type_instance=self.type_instance, starttime=self.dates[0],
                endtime=self.dates[1])
        transaction.commit()

        if tuples.rowcount < 1:
            return

        self.data = list()
        tmpdata = dict()
        rows = tuples.fetchall()
        for ds in self.details['dsnames']:
            tmpdata[ds] = list()
        i = 1
        while i < tuples.rowcount:
            ctime = int(rows[i]['time'])
            seconds = (ctime - int(rows[i - 1]['time'])) / 1000
            if seconds == 0:
                i += 1
                continue
            for j in range(len(self.details['dsnames'])):
                tmpdata[self.details['dsnames'][j]].append('[%d, %f]' % \
                        (ctime, float(rows[i]['values'][j] -
                        rows[i - 1]['values'][j])))
            i += 1

        for ds in self.details['dsnames']:
            self.data.append(', '.join(tmpdata[ds]))
