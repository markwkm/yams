"""Helper functions

Consists of functions to typically be used within templates, but also
available to Controllers. This module is available to templates as 'h'.
"""

from datetime import datetime
from time import gmtime, time
import pytz

from pylons import config

from sqlalchemy.sql.expression import text

from yamswui.lib.flotr import Flotr
from yamswui.model.meta import Session


class YamsChart:
    def __init__(self, host, duration=None, end_ctime=None):
        self.host = host
        self.end_ctime = end_ctime
        if duration is None:
            self.duration = int(config['duration'])
        else:
            self.duration = duration

        self.ylabel = None
        self._set_dates(self.duration, self.end_ctime)

        self.connection = Session.connection()
        self._get_details()

    def _set_dates(self, duration, end_ctime):
        """Calculate the date range by specifying the end time and how far back
        in time to display data.
        """

        if end_ctime is None:
            end_ctime = time()

        ts = gmtime(end_ctime)
        end_timestamp = datetime(ts.tm_year, ts.tm_mon, ts.tm_mday, ts.tm_hour,
                ts.tm_min, ts.tm_sec, 0, pytz.utc)

        ts = gmtime(end_ctime - duration)
        start_timestamp = datetime(ts.tm_year, ts.tm_mon, ts.tm_mday,
                ts.tm_hour, ts.tm_min, ts.tm_sec, 0, pytz.utc)

        self.dates = [start_timestamp, end_timestamp]

    def _get_details(self):
        transaction = self.connection.begin()
        tuples = self.connection.execute(text(
"""SELECT dsnames, dstypes
FROM %s
WHERE time > :starttime
  AND time <= :endtime
  AND host = :name
LIMIT 1
OFFSET 1;
""" % self.tablename), name=self.host, starttime=self.dates[0],
                endtime=self.dates[1]).first()
        transaction.commit()

        self.details = tuples

    def javascript(self, title):
        self._get_data()
        f = Flotr(data=self.data, title=title, legend=self.legend(),
                ylabel=self.ylabel)
        return f.javascript()

    def legend(self):
            return self.details['dsnames']


class YamsPluginInstanceTypeChart(YamsChart):
    def __init__(self, host, plugin_instance, type, duration=None,
            end_ctime=None):
        self.host = host
        self.plugin_instance = plugin_instance
        self.type = type
        self.end_ctime = end_ctime
        if duration is None:
            self.duration = int(config['duration'])
        else:
            self.duration = duration

        self.ylabel = None
        self._set_dates(self.duration, self.end_ctime)

        self.connection = Session.connection()
        self._get_details()

    def _get_details(self):
        transaction = self.connection.begin()
        time = self.connection.execute(text(
"""SELECT time
FROM %s
WHERE time > :starttime
  AND time <= :endtime
  AND host = :name
  AND plugin_instance = :plugin_instance
  AND type = :type
LIMIT 1
OFFSET 1;
""" % self.tablename), name=self.host, starttime=self.dates[0],
                endtime=self.dates[1], plugin_instance=self.plugin_instance,
                type=self.type).first()['time']

        tuple = self.connection.execute(text(
"""SELECT dsnames, dstypes
FROM %s
WHERE time > :starttime
  AND time <= :endtime
  AND host = :name
  AND plugin_instance = :plugin_instance
  AND type = :type
LIMIT 1
OFFSET 1;
""" % self.tablename), name=self.host, starttime=self.dates[0],
                endtime=self.dates[1], plugin_instance=self.plugin_instance,
                type=self.type).first()
        transaction.commit()

        self.details = {'dsnames': tuple['dsnames'], 'dstypes':
                tuple['dstypes']}

    def javascript(self, title=None):
        if title is None:
            title = '%s %s' % (self.type, self.plugin_instance)
        return YamsChart.javascript(self, title)

    def legend(self):
            return self.details['dsnames']


class YamsChartType(YamsChart):
    def __init__(self, host, type, type_instance, duration=None,
            end_ctime=None):
        self.host = host
        self.type = type
        self.type_instance = type_instance
        self.end_ctime = end_ctime
        if duration is None:
            self.duration = int(config['duration'])
        else:
            self.duration = duration

        self._set_dates(self.duration, self.end_ctime)

        self.connection = Session.connection()
        self._get_details()

    def get_details(self):
        transaction = self.connection.begin()
        time = self.connection.execute(text(
"""SELECT time
FROM %s
WHERE time > :starttime
  AND time <= :endtime
  AND host = :name
LIMIT 1
OFFSET 1;
""" % self.tablename), name=self.host, starttime=self.dates[0],
                endtime=self.dates[1]).first()['time']

        tuples = self.connection.execute(text(
"""SELECT type_instance, dstypes
FROM %s
WHERE time = :time
  AND host = :name
  AND type = :type
ORDER BY type_instance;
""" % self.tablename), name=self.host, time=time, type=self.type).fetchall()
        transaction.commit()

        data = {'dsnames': list(), 'dstypes': list()}
        for tuple in tuples:
            data['dsnames'].append(tuple['type_instance'])
            data['dstypes'].append(tuple['dstypes'][0])

        return data

    def javascript(self, title=None):
        if title is None:
            title = '%s %s' % (self.type, self.type_instance)
        return YamsChart.javascript(self, title)


class YamsChartTypeInstance(YamsChart):
    def __init__(self, host, type, type_instance, duration=None,
            end_ctime=None):
        self.host = host
        self.type = type
        self.type_instance = type_instance
        self.end_ctime = end_ctime
        if duration is None:
            self.duration = int(config['duration'])
        else:
            self.duration = duration

        self.ylabel = None
        self._set_dates(self.duration, self.end_ctime)

        self.connection = Session.connection()
        self._get_details()

    def _get_details(self):
        transaction = self.connection.begin()
        time = self.connection.execute(text(
"""SELECT time
FROM %s
WHERE time > :starttime
  AND time <= :endtime
  AND host = :name
LIMIT 1;
""" % self.tablename), name=self.host, starttime=self.dates[0],
                endtime=self.dates[1]).first()['time']

        tuples = self.connection.execute(text(
"""SELECT type_instance, dstypes
FROM %s
WHERE time = :time
  AND host = :name
ORDER BY type_instance;
""" % self.tablename), name=self.host, time=time).fetchall()
        transaction.commit()

        self.details = {'dsnames': list(), 'dstypes': list()}
        for tuple in tuples:
            self.details['dsnames'].append(tuple['type_instance'])
            self.details['dstypes'].append(tuple['dstypes'][0])

    def javascript(self, title=None):
        if title is None:
            title = '%s %s' % (self.type, self.type_instance)
        return YamsChart.javascript(self, title)


class YamsChartTypeTypeInstance(YamsChart):
    def __init__(self, host, type, type_instance, duration=None,
            end_ctime=None):
        self.host = host
        self.type = type
        self.type_instance = type_instance
        self.end_ctime = end_ctime
        if duration is None:
            self.duration = int(config['duration'])
        else:
            self.duration = duration

        self.ylabel = None
        self._set_dates(self.duration, self.end_ctime)

        self.connection = Session.connection()
        self._get_details()

    def _get_details(self):
        transaction = self.connection.begin()
        time = self.connection.execute(text(
"""SELECT time
FROM %s
WHERE time > :starttime
  AND time <= :endtime
  AND host = :name
LIMIT 1;
""" % self.tablename), name=self.host, starttime=self.dates[0],
                endtime=self.dates[1]).first()['time']

        tuple = self.connection.execute(text(
"""SELECT dsnames, dstypes
FROM %s
WHERE time = :time
  AND host = :name
  AND type = :type
  AND type_instance = :type_instance
ORDER BY type_instance;
""" % self.tablename), name=self.host, time=time, type=self.type,
                type_instance=self.type_instance).first()
        transaction.commit()

        self.details = {'dsnames': tuple['dsnames'], 'dstypes':
                tuple['dstypes']}

    def javascript(self, title=None):
        if title is None:
            title = '%s %s' % (self.type, self.type_instance)
        return YamsChart.javascript(self, title)
