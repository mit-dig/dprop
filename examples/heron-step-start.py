import dbus, gobject, traceback, sys
from dbus.mainloop.glib import DBusGMainLoop

def process1():
    print "Starting HeronStep"
    print "Setting X to 2"
    x_cell.changeCell('2',
                      dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    print "Sleeping 2 seconds"
    gobject.timeout_add(2000, process2)
    return False

def process2():
    print "Good Morning! Back to work!"
    print "Setting G to 1.4"
    g_cell.changeCell('1.4',
                      dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    return False

def change_x_signal_handler(raw_data):
    data = raw_data
    print "X changed to %s" % (data)
    x_cell.changeCell(raw_data,
                      dbus_interface='edu.mit.csail.dig.DPropMan.Cell')

def change_g_signal_handler(raw_data):
    data = raw_data
    print "G changed to %s" % (data)
    g_cell.changeCell(raw_data,
                      dbus_interface='edu.mit.csail.dig.DPropMan.Cell')

def change_h_signal_handler(raw_data):
    data = raw_data
    print "H changed to %s" % (data)
    h_cell.changeCell(raw_data,
                      dbus_interface='edu.mit.csail.dig.DPropMan.Cell')

def destroy_x_signal_handler():
    print "X destroyed.  Cleaning up."

def destroy_g_signal_handler():
    print "G destroyed.  Cleaning up."

def destroy_h_signal_handler():
    print "H destroyed.  Cleaning up."

DBusGMainLoop(set_as_default=True)

bus = dbus.SessionBus()
try:
    propman = bus.get_object('edu.mit.csail.dig.DPropMan',
                             '/edu/mit/csail/dig/DPropMan')
    name = propman.registerCell('x',
                                dbus_interface='edu.mit.csail.dig.DPropMan')
    x_cell = bus.get_object('edu.mit.csail.dig.DPropMan',
                            '/edu/mit/csail/dig/DPropMan/Cells/%s' % (name))
    x_cell.connect_to_signal('ChangeSignal', change_x_signal_handler,
                             dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    x_cell.connect_to_signal('DestroySignal', destroy_x_signal_handler,
                             dbus_interface='edu.mit.csail.dig.DPropMan.Cell')

    name = propman.registerCell('g',
                                dbus_interface='edu.mit.csail.dig.DPropMan')
    g_cell = bus.get_object('edu.mit.csail.dig.DPropMan',
                            '/edu/mit/csail/dig/DPropMan/Cells/%s' % (name))
    g_cell.connect_to_signal('ChangeSignal', change_g_signal_handler,
                             dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    g_cell.connect_to_signal('DestroySignal', destroy_g_signal_handler,
                             dbus_interface='edu.mit.csail.dig.DPropMan.Cell')

    name = propman.remoteCell('http://pipian.com:37767/Cells/h',
                              dbus_interface='edu.mit.csail.dig.DPropMan')
    h_cell = bus.get_object('edu.mit.csail.dig.DPropMan',
                            '/edu/mit/csail/dig/DPropMan/RemoteCells/%s' % (name))
    h_cell.connect_to_signal('ChangeSignal', change_h_signal_handler,
                             dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    h_cell.connect_to_signal('DestroySignal', destroy_h_signal_handler,
                             dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
except dbus.DBusException:
    traceback.print_exc()
#    print usage
    sys.exit(1)

gobject.timeout_add(2000, process1)

loop = gobject.MainLoop()
loop.run()
