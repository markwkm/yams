from webhelpers.html import literal

from sqlalchemy.sql.expression import text

from yamswui.lib.helpers import YamsChart
from yamswui.model.meta import Session


class CpuChart(YamsChart):
    def __init__(self, host, cpu=None, duration=None, end_ctime=None):
        self.tablename = 'vl_cpu'
        self.type = None
        self.type_instance = None
        self.cpu = cpu
        YamsChart.__init__(self, host, duration, end_ctime)

    def _get_data(self):
        if self.cpu is not None:
            where_clause = 'AND plugin_instance = :cpu'
        else:
            where_clause = ''

        select_clause = ''
        for dsname in self.details['dsnames']:
            select_clause += ',\n       SUM(CASE WHEN type_instance = ' \
                    '\'%s\' THEN values[1] ELSE 0 END) AS %s' % \
                    (dsname, dsname)

        sql = text(
"""SELECT EXTRACT(EPOCH FROM time) * 1000 AS time%s
FROM vl_cpu
WHERE time > :starttime
  AND time <= :endtime
  AND host = :name
  %s
GROUP BY time
ORDER BY time ASC;""" % (select_clause, where_clause))

        transaction = self.connection.begin()
        if self.cpu is not None:
            tuples = self.connection.execute(sql, name=self.host,
                    starttime=self.dates[0], endtime=self.dates[1],
                    cpu=str(self.cpu))
        else:
            tuples = self.connection.execute(sql, name=self.host,
                    starttime=self.dates[0], endtime=self.dates[1])
        transaction.commit()

        if tuples.rowcount < 1:
            return

        rows = tuples.fetchall()

        vl = dict()
        for dsname in self.details['dsnames']:
            vl[dsname] = list()

        i = 1
        while i < tuples.rowcount:
            # Calculate change in values.
            total = 0
            val = dict()
            for dsname in self.details['dsnames']:
                val[dsname] = rows[i][dsname] - rows[i - 1][dsname]
                total += val[dsname]

            if total == 0:
                # Proactive handling of divide by 0.
                for dsname in self.details['dsnames']:
                    val[dsname] = 0
            else:
                # Convert jiffies to percentages.
                for dsname in self.details['dsnames']:
                    val[dsname] /= total / 100

            # Convert into how flotr wants the data.
            ctime = int(rows[i]['time'])
            for dsname in self.details['dsnames']:
                vl[dsname].append('[%d, %f]' % (ctime, val[dsname]))

            i += 1

        # Generate strings for javascript to use.
        self.data = list()
        for dsname in self.details['dsnames']:
            self.data.append(', '.join(vl[dsname]))

    def _get_details(self):
        transaction = self.connection.begin()
        # Everyone has 'idle' right?
        time = self.connection.execute(text(
"""SELECT time
FROM %s
WHERE time > :starttime
  AND time <= :endtime
  AND host = :name
  AND type_instance = 'idle'
LIMIT 1
OFFSET 1;
""" % self.tablename), name=self.host, starttime=self.dates[0],
endtime=self.dates[1]).first()['time']

        tuples = self.connection.execute(text(
"""SELECT type_instance, dstypes
FROM %s
WHERE time = :time
  AND host = :name
  AND plugin_instance = '0'
ORDER BY type_instance;
""" % self.tablename), name=self.host, time=time).fetchall()
        transaction.commit()

        self.details = {'dsnames': list(), 'dstypes': list()}
        for tuple in tuples:
            self.details['dsnames'].append(tuple['type_instance'])
            self.details['dstypes'].append(tuple['dstypes'][0])

    def javascript(self):
        if self.cpu is not None:
            title = 'Processor %d Utilization' % self.cpu
        else:
            title = 'Processor Utilization'
        return YamsChart.javascript(self, title)
