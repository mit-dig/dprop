import dbus, gobject, traceback, sys
from dbus.mainloop.glib import DBusGMainLoop

import dpropjson

def new_contents_signal_handler(raw_data):
    data = dpropjson.loads(raw_data)
    print "Got New Contents: %s" % (`data`)

def merge_contents_signal_handler(raw_data):
    data = dpropjson.loads(raw_data)
    print "Got Merge Contents: %s" % (`data`)
    cellman.changeCell(raw_data,
                       dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    print "Set to %s" % (`data`)

def destroy_signal_handler():
    print "Cell destroyed.  Cleaning up."

DBusGMainLoop(set_as_default=True)

bus = dbus.SystemBus()
try:
    propman = bus.get_object('edu.mit.csail.dig.DPropMan',
                             '/DPropMan')
    uuid = 'c470a053-c59d-4133-80fd-e9b65c0659d3'
    uuid = propman.registerCell(uuid,
                                dbus_interface='edu.mit.csail.dig.DPropMan')
    cellman = bus.get_object('edu.mit.csail.dig.DPropMan',
                             '/Cells/%s' % (uuid))
    print "JSON is %s" % (cellman.data(dbus_interface='edu.mit.csail.dig.DPropMan.Cell'))
    print "Data is %s" % (`dpropjson.loads(str(cellman.data(dbus_interface='edu.mit.csail.dig.DPropMan.Cell')))`)
    cellman.connect_to_signal('NewContentsSignal', new_contents_signal_handler,
                              dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    cellman.connect_to_signal('MergeContentsSignal', merge_contents_signal_handler,
                              dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    cellman.connect_to_signal('DestroySignal', destroy_signal_handler,
                              dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
except dbus.DBusException:
    traceback.print_exc()
#    print usage
    sys.exit(1)

loop = gobject.MainLoop()
loop.run()
