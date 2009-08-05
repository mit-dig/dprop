import dbus, gobject, traceback, sys
from dbus.mainloop.glib import DBusGMainLoop

def change_signal_handler(raw_data):
    data = raw_data
    print "Received Change Signal (contents: %s)" % (data)
    # We automatically merge changes by replacement.
    cellman.changeCell(raw_data,
                       dbus_interface='edu.mit.csail.dig.DPropMan.Cell')

def destroy_signal_handler():
    print "Cell destroyed.  Cleaning up."

DBusGMainLoop(set_as_default=True)

bus = dbus.SessionBus()
try:
    propman = bus.get_object('edu.mit.csail.dig.DPropMan',
                             '/edu/mit/csail/dig/DPropMan')
    name = propman.remoteCell('http://mr-burns.w3.org:37767/Cells/someName',
                              dbus_interface='edu.mit.csail.dig.DPropMan')
    cellman = bus.get_object('edu.mit.csail.dig.DPropMan',
                             '/edu/mit/csail/dig/DPropMan/RemoteCells/%s' % (name))
    print "Data is %s" % (cellman.data(dbus_interface='edu.mit.csail.dig.DPropMan.Cell'))
    cellman.connect_to_signal('ChangeSignal', change_signal_handler,
                              dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    cellman.connect_to_signal('DestroySignal', destroy_signal_handler,
                              dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
except dbus.DBusException:
    traceback.print_exc()
#    print usage
    sys.exit(1)

loop = gobject.MainLoop()
loop.run()
