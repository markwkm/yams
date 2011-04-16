import logging

from pylons import request, response, session, tmpl_context as c, url
from pylons.controllers.util import abort, redirect

from sqlalchemy.sql.expression import text

from yamswui.model.meta import Session

from yamswui.lib.base import BaseController, render
from yamswui.lib.cpu import CpuChart
from yamswui.lib.disk import DiskChart
from yamswui.lib.interface import InterfaceChart
from yamswui.lib.load import LoadChart
from yamswui.lib.memory import MemoryChart
from yamswui.lib.vmem import VmemChart

log = logging.getLogger(__name__)


class SystemController(BaseController):

    def cpu(self, id):
        c.host = id

        c.chart = dict()

        # cpu plugin
        cpu_chart = CpuChart(c.host)
        c.chart['cpu'] = cpu_chart.javascript()

        # chart per cpu
        connection = Session.connection()
        transaction = connection.begin()
        c.lprocs = connection.execute(text(
                'SELECT lprocs FROM systems WHERE name = :host;'),
                host=c.host).scalar()
        transaction.commit()

        for cpu in range(c.lprocs):
            cpu_chart = CpuChart(c.host, cpu)
            c.chart['cpu%d' % cpu] = cpu_chart.javascript()

        return render('/system-cpu.mako')

    def disk(self, id):
        c.host = id

        c.chart1 = dict()
        c.chart2 = dict()

        connection = Session.connection()
        transaction = connection.begin()
        c.disks = connection.execute(text(
                'SELECT disks FROM systems WHERE name = :host;'),
                host=c.host).scalar()
        transaction.commit()

        for disk in c.disks:
            disk_chart = DiskChart(c.host, disk, 'disk_ops')
            c.chart1[disk] = disk_chart.javascript()

        for disk in c.disks:
            disk_chart = DiskChart(c.host, disk, 'disk_octets')
            c.chart2[disk] = disk_chart.javascript()

        return render('/system-disk.mako')

    def index(self):
        connection = Session.connection()
        transaction = connection.begin()
        c.systems = connection.execute(
                'SELECT name, plugins, lprocs, interfaces, disks ' \
                'FROM systems ORDER BY name;')
        transaction.commit()
        return render('/system.mako')

    def interface(self, id):
        c.host = id

        c.chart = dict()

        connection = Session.connection()
        transaction = connection.begin()
        c.interfaces = connection.execute(text(
                'SELECT interfaces FROM systems WHERE name = :host;'),
                host=c.host).scalar()
        transaction.commit()

        # interface plugin
        for interface in c.interfaces:
            interface_chart = InterfaceChart(c.host, 'if_octets', interface)
            c.chart[interface] = interface_chart.javascript()

        return render('/system-interface.mako')

    def load(self, id):
        c.host = id

        c.chart = dict()

        # load plugin
        load_chart = LoadChart(c.host)
        c.chart['load'] = load_chart.javascript()

        return render('/system-load.mako')

    def memory(self, id):
        c.host = id

        c.chart = dict()

        # memory plugin
        memory_chart = MemoryChart(c.host)
        c.chart['memory'] = memory_chart.javascript()

        return render('/system-memory.mako')

    def vmem(self, id):
        c.host = id

        c.chart = dict()

        # vmem plugin

        # memory
        memory_chart = VmemChart(c.host, 'vmpage_io', 'memory')
        c.chart['memory'] = memory_chart.javascript()

        # swap
        swap_chart = VmemChart(c.host, 'vmpage_io', 'swap')
        c.chart['swap'] = swap_chart.javascript()

        return render('/system-vmem.mako')
