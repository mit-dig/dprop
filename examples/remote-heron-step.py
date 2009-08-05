import dbus, gobject, traceback, sys
from dbus.mainloop.glib import DBusGMainLoop

x = None
g = None

def process():
    global h_cell
    h = (x / g + g) / 2
    print "X = %f, G = %f, H = %f" % (x, g, h)
    h_cell.changeCell(str(h),
                      dbus_interface='edu.mit.csail.dig.DPropMan.Cell')

def change_x_signal_handler(raw_data):
    global x, g
    data = raw_data
    print "X changed to %s" % (data)
    x = float(data)
    x_cell.changeCell(raw_data,
                      dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    if g != 'NULL':
        process()

def change_g_signal_handler(raw_data):
    global x, g
    data = raw_data
    print "G changed to %s" % (data)
    g = float(data)
    g_cell.changeCell(raw_data,
                      dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    if x != 'NULL':
        process()

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
    name = propman.remoteCell('http://mr-burns.w3.org:37767/Cells/x',
                              dbus_interface='edu.mit.csail.dig.DPropMan')
    x_cell = bus.get_object('edu.mit.csail.dig.DPropMan',
                             '/edu/mit/csail/dig/DPropMan/RemoteCells/%s' % (name))
    x_cell.connect_to_signal('ChangeSignal', change_x_signal_handler,
                              dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    x_cell.connect_to_signal('DestroySignal', destroy_x_signal_handler,
                              dbus_interface='edu.mit.csail.dig.DPropMan.Cell')

    name = propman.remoteCell('http://mr-burns.w3.org:37767/Cells/g',
                              dbus_interface='edu.mit.csail.dig.DPropMan')
    g_cell = bus.get_object('edu.mit.csail.dig.DPropMan',
                             '/edu/mit/csail/dig/DPropMan/RemoteCells/%s' % (name))
    g_cell.connect_to_signal('ChangeSignal', change_g_signal_handler,
                              dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    g_cell.connect_to_signal('DestroySignal', destroy_g_signal_handler,
                              dbus_interface='edu.mit.csail.dig.DPropMan.Cell')

    name = propman.registerCell('h',
                                dbus_interface='edu.mit.csail.dig.DPropMan')
    h_cell = bus.get_object('edu.mit.csail.dig.DPropMan',
                            '/edu/mit/csail/dig/DPropMan/Cells/%s' % (name))
    h_cell.connect_to_signal('ChangeSignal', change_h_signal_handler,
                             dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    h_cell.connect_to_signal('DestroySignal', destroy_h_signal_handler,
                             dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    
    x = x_cell.data(dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    g = g_cell.data(dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    if x != 'NULL' and g != 'NULL':
        process()
except dbus.DBusException:
    traceback.print_exc()
#    print usage
    sys.exit(1)

loop = gobject.MainLoop()
loop.run()
