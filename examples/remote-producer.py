import dbus, gobject, traceback, sys, dbus.service, dbus.mainloop.glib

import dpropjson

def emit_signal():
    cellman.updateCell(dpropjson.dumps('remote-producer.py'),
                       dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    
    gobject.timeout_add(2000, emit_signal)
    
    return False

dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)

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
except dbus.DBusException:
    traceback.print_exc()
#    print usage
    sys.exit(1)

gobject.timeout_add(2000, emit_signal)

loop = gobject.MainLoop()
loop.run()
