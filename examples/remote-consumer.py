import dbus, gobject, traceback, sys
from dbus.mainloop.glib import DBusGMainLoop

def new_contents_signal_handler(raw_data):
    data = raw_data
    print "Received New Contents: %s" % (data)

def destroy_signal_handler():
    print "Cell destroyed.  Cleaning up."

DBusGMainLoop(set_as_default=True)

bus = dbus.SystemBus()
try:
    propman = bus.get_object('edu.mit.csail.dig.DPropMan',
                             '/DPropMan')
    name = '/mr-burns.w3.org:37767/Cells/edu/mit/csail/dig/DPropMan/Examples/Producer/cell'
    url = "http:/%s" % (name)
    name = str(propman.escapePath(name,
                                  dbus_interface='edu.mit.csail.dig.DPropMan'))
    propman.registerRemoteCell(url,
                               dbus_interface='edu.mit.csail.dig.DPropMan')
    cellman = bus.get_object('edu.mit.csail.dig.DPropMan',
                             '/RemoteCell%s' % (name))
    print "Data is %s" % (cellman.data(dbus_interface='edu.mit.csail.dig.DPropMan.Cell'))
    cellman.connect_to_signal('NewContentsSignal', new_contents_signal_handler,
                              dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    cellman.connect_to_signal('DestroySignal', destroy_signal_handler,
                              dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
except dbus.DBusException:
    traceback.print_exc()
#    print usage
    sys.exit(1)

loop = gobject.MainLoop()
loop.run()
