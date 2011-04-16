from webhelpers.html import literal

from sqlalchemy.sql.expression import text

from yamswui.lib.helpers import YamsPluginInstanceTypeChart
from yamswui.model.meta import Session


class DiskChart(YamsPluginInstanceTypeChart):
    def __init__(self, host, plugin_instance, type, duration=None,
            end_ctime=None):
        self.tablename = 'vl_disk'
        YamsPluginInstanceTypeChart.__init__(self, host, plugin_instance, type,
                duration, end_ctime)

    def _get_data(self):
        if self.details is None:
            return

        transaction = self.connection.begin()
        tuples = self.connection.execute(text(
"""SELECT EXTRACT(EPOCH FROM time) * 1000 AS time, values
FROM vl_disk
WHERE time > :starttime
  AND time <= :endtime
  AND host = :name
  AND type = :type
  AND plugin_instance = :plugin_instance
ORDER BY time ASC;"""), name=self.host, starttime=self.dates[0],
                endtime=self.dates[1], type=self.type,
                plugin_instance=self.plugin_instance)
        transaction.commit()

        if tuples.rowcount < 1:
            return

        rows = tuples.fetchall()

        self.data = list()
        vl = dict()

        for name in self.details['dsnames']:
            vl[name] = list()

        i = 1
        while i < tuples.rowcount:
            ctime = int(rows[i]['time'])
            seconds = (ctime - int(rows[i - 1]['time'])) / 1000
            if seconds == 0:
                i += 1
                continue
            index = 0
            for name in self.details['dsnames']:
                vl[name].append('[%d, %f]' % (ctime, rows[i]['values'][index] -
                        rows[i - 1]['values'][index]))
                index += 1
            i += 1

        for name in self.details['dsnames']:
            self.data.append(', '.join(vl[name]))
