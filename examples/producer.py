import dbus, gobject, traceback, sys, dbus.service, dbus.mainloop.glib

import dpropjson

def emit_signal():
    cellman.changeCell(dpropjson.dumps('producer.py'),
                       dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    
    gobject.timeout_add(2000, emit_signal)
    
    return False

dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)

bus = dbus.SystemBus()
try:
    propman = bus.get_object('edu.mit.csail.dig.DPropMan',
                             '/DPropMan')
    uuid = 'c470a053-c59d-4133-80fd-e9b65c0659d3'
    uuid = propman.registerCell(uuid,
                                dbus_interface='edu.mit.csail.dig.DPropMan')
    cellman = bus.get_object('edu.mit.csail.dig.DPropMan',
                             '/Cells/%s' % (uuid))
except dbus.DBusException:
    traceback.print_exc()
#    print usage
    sys.exit(1)

gobject.timeout_add(2000, emit_signal)

loop = gobject.MainLoop()
loop.run()
