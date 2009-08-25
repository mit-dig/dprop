import dbus, gobject, traceback, sys, dbus.service, dbus.mainloop.glib

import dpropjson

def emit_signal():
    cellman.mergeCell(dpropjson.dumps('remote-alt-producer.py'),
                      dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    
    gobject.timeout_add(2000, emit_signal)
    
    return False

dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)

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
except dbus.DBusException:
    traceback.print_exc()
#    print usage
    sys.exit(1)

gobject.timeout_add(2000, emit_signal)

loop = gobject.MainLoop()
loop.run()
