from webhelpers.html import literal

from sqlalchemy.sql.expression import text

from yamswui.lib.helpers import YamsChartTypeInstance
from yamswui.model.meta import Session


class MemoryChart(YamsChartTypeInstance):
    def __init__(self, host, duration=None, end_ctime=None):
        self.tablename = 'vl_memory'
        YamsChartTypeInstance. __init__(self, host, 'memory', '', duration,
                end_ctime)
        self.ylabel = 'Bytes'

    def _get_data(self):
        if self.details is None:
            return

        self.data = list()
        transaction = self.connection.begin()
        for type_instance in self.details['dsnames']:
            tuples = self.connection.execute(text(
"""SELECT EXTRACT(EPOCH FROM time) * 1000 AS time, values[1] AS value
FROM vl_memory
WHERE time > :starttime
  AND time <= :endtime
  AND host = :name
  AND type_instance = :type_instance
ORDER BY time ASC;"""), name=self.host, starttime=self.dates[0],
                    endtime=self.dates[1], type_instance=type_instance)

            vl = list()
            for row in tuples:
                ctime = int(row['time'])
                vl.append('[%d, %f]' % (ctime, float(row['value'])))
            self.data.append(', '.join(vl))
        transaction.commit()
