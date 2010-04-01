import dbus, gobject, traceback, sys, dbus.service, dbus.mainloop.glib
import httplib

import dpropjson, pydprop

def mergeRange(cell, raw_data, peer):
    update = dpropjson.loads(raw_data)
    print "Got Update: %s" % (`update`)
    data = dpropjson.loads(cell.data())
    
    if not isinstance(update, dict):
        print "Don't know how to handle data type.  Not merging."
    elif 'type' not in update:
        print "Can't handle arbitrary dicts.  Not merging."
    elif update['type'] == 'range':
        if 'min' not in update or 'max' not in update:
            print "Hey, this isn't a range type!  Not merging."
        elif not isinstance(update['min'], (int, long, float)) or not isinstance(update['max'], (int, long, float)):
            print "Hey, these maxs/mins aren't numbers!  Not merging."
        elif isinstance(data, dpropjson.Nothing):
            # Initialize the data.
            cell.set(raw_data)
            print "Cell empty! Set to %s" % (`str(raw_data)`)
        else:
            # Intersect the ranges.
            updated = False
            if update['min'] < data['min']:
                updated = True
                data['min'] = update['min']
            if update['max'] > data['max']:
                updated = True
                data['max'] = update['max']
            if updated:
                cell.set(dpropjson.dumps(data))
                print "Cell now set to %s" % (`data`)
    else:
        print "Don't know how to handle this type.  Not merging."

def convert_temperature_range(range):
    return {'type': 'range',
            'min': (float(range['min']) - 32) * 5 / 9,
            'max': (float(range['max']) - 32) * 5 / 9}

def convertTempRange():
    print "Woke up converter propagator"
    
    data = dpropjson.loads(temp_cell.data())
    
    if isinstance(data, dpropjson.Nothing):
        return
    elif not isinstance(data, dict):
        return
    elif 'type' not in data:
        return
    elif data['type'] != 'range':
        return
    elif 'min' not in data or 'max' not in data:
        return
    elif not isinstance(data['min'], int) or not isinstance(data['max'], int):
        return
    else:
        print "Converted temperature to %s" % (`convert_temperature_range(data)`)
        converted_cell.update(dpropjson.dumps(convert_temperature_range(data)))

dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)

bus = dbus.SystemBus()
try:
    # UUID for the temp. cell
    name = '/pipian.com:37767/Cells/c4add3d03cf311df98790800200c9a66'
    url = "https:/%s" % (name)
    
    temp_cell = pydprop.RemoteCell(url, mergeRange)
    converterPropagator = pydprop.Propagator('temp-converter',
                                             [temp_cell],
                                             convertTempRange)
    
    uuid = "46d081b0-3cf8-11df-9879-0800200c9a66"
    converted_cell = pydprop.LocalCell(uuid, mergeRange)
except dbus.DBusException:
    traceback.print_exc()
    print usage
    sys.exit(1)

loop = gobject.MainLoop()
loop.run()
