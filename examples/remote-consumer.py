import dbus, gobject, traceback, sys
from dbus.mainloop.glib import DBusGMainLoop

import dpropjson

def update_signal_handler(raw_data, peer):
    data = dpropjson.loads(raw_data)
    print "Got Update: %s" % (`data`)
    cellman.changeCell(raw_data,
                       dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    print "Set to %s" % (`data`)

#def destroy_signal_handler():
#    print "Cell destroyed.  Cleaning up."

DBusGMainLoop(set_as_default=True)

bus = dbus.SystemBus()
try:
    propman = bus.get_object('edu.mit.csail.dig.DPropMan',
                             '/DPropMan')
    name = '/mr-burns.w3.org:37767/Cells/c470a053c59d413380fde9b65c0659d3'
    url = "https:/%s" % (name)
    uuid = propman.registerRemoteCell(url,
                                      dbus_interface='edu.mit.csail.dig.DPropMan')
    cellman = bus.get_object('edu.mit.csail.dig.DPropMan',
                             '/Cells/%s' % (uuid))
    cellman.connectToRemote(url,
                            dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    print "JSON is %s" % (cellman.data(dbus_interface='edu.mit.csail.dig.DPropMan.Cell'))
    print "Data is %s" % (`dpropjson.loads(str(cellman.data(dbus_interface='edu.mit.csail.dig.DPropMan.Cell')))`)
    cellman.connect_to_signal('UpdateSignal', update_signal_handler,
                              dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
#    cellman.connect_to_signal('DestroySignal', destroy_signal_handler,
#                              dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
except dbus.DBusException:
    traceback.print_exc()
#    print usage
    sys.exit(1)

loop = gobject.MainLoop()
loop.run()
