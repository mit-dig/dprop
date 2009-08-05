import dbus, gobject, traceback, sys, dbus.service, dbus.mainloop.glib

def emit_signal():
    cellman.changeCell('producer.py',
                       dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    
    gobject.timeout_add(2000, emit_signal)
    
    return False

dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)

bus = dbus.SessionBus()
try:
    propman = bus.get_object('edu.mit.csail.dig.DPropMan',
                             '/edu/mit/csail/dig/DPropMan')
    name = propman.registerCell('someName',
                                dbus_interface='edu.mit.csail.dig.DPropMan')
    cellman = bus.get_object('edu.mit.csail.dig.DPropMan',
                             '/edu/mit/csail/dig/DPropMan/Cells/%s' % (name))
except dbus.DBusException:
    traceback.print_exc()
#    print usage
    sys.exit(1)

gobject.timeout_add(2000, emit_signal)

loop = gobject.MainLoop()
loop.run()
