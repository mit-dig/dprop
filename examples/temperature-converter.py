import dbus, gobject, traceback, sys, dbus.service, dbus.mainloop.glib
import httplib

import dpropjson, pydprop

def mergeRange(cell, raw_data, peer):
    update = dpropjson.loads(raw_data)
    if str(peer) == cell.url():
        print "\x1B[01;37;45mGot Local Update: %s\x1B[00m" % (`update`)
    else:
        print "\x1B[01;37;46mGot Remote Update: %s\x1B[00m" % (`update`)
    data = dpropjson.loads(cell.data())
    
    if not isinstance(update, dict):
        print "\x1B[01;37;41mDon't know how to handle data type.  Not merging.\x1B[00m"
    elif 'type' not in update:
        print "\x1B[01;37;41mCan't handle arbitrary dicts.  Not merging.\x1B[00m"
    elif update['type'] == 'range':
        if 'min' not in update or 'max' not in update:
            print "\x1B[01;37;41mHey, this isn't a range type!  Not merging.\x1B[00m"
        elif not isinstance(update['min'], (int, long, float)) or not isinstance(update['max'], (int, long, float)):
            print "\x1B[01;37;41mHey, these maxs/mins aren't numbers!  Not merging.\x1B[00m"
        elif isinstance(data, dpropjson.Nothing):
            # Initialize the data.
            cell.set(raw_data)
            print "\x1B[01;37;44mCell empty! Set to %s\x1B[00m" % (`str(raw_data)`)
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
                print "\x1B[01;37;44mCell now set to %s\x1B[00m" % (`data`)
    else:
        print "\x1B[01;37;41mDon't know how to handle this type.  Not merging.\x1B[00m"

def convert_temperature_range(range):
    return {'type': 'range',
            'min': (float(range['min']) - 32) * 5 / 9,
            'max': (float(range['max']) - 32) * 5 / 9}

def convertTempRange():
    print "\x1B[01;43mWoke up converter propagator\x1B[00m"
    
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
        print "\x1B[01;43mConverted temperature to %s\x1B[00m" % (`convert_temperature_range(data)`)
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
