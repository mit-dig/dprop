import dbus, gobject, traceback, sys, dbus.service, dbus.mainloop.glib
import httplib

import dpropjson, pydprop

WFO = "box" # Forecast Office: Boston
STATION = "BOS" # Station: Boston - Logan Airport

def fetch_temperatures(date = None):
    url = "/climate/getclimate.php?wfo=%s" % (WFO)
    if date is None:
        params = "product=CF6&station=%s&recent=yes" % (STATION)
    else:
        params = "product=CF6&station=%s&recent=no&date=%s" % (STATION, date)
    
    h = httplib.HTTPConnection('www.weather.gov')
    
    h.request("POST", url, params,
              {"Content-type": "application/x-www-form-urlencoded"})
    resp = h.getresponse()
    data = resp.read()
    h.close()

    data = data[data.find("<font size=3>") + 13:data.find("</font></pre>")]
    
    # Find the rows of each day.
    lastRow = None
    foundRows = False
    rows = []
    for row in data.split("\n"):
        row = row.split()
        if len(row) == 0:
            continue
        elif row[0] == '1':
            lastRow = row
        elif row[0] == '2' and lastRow is not None:
            rows.append(lastRow)
            rows.append(row)
            foundRows = True
        elif lastRow is not None:
            lastRow = None
        elif foundRows and row[0][0] != '=':
            rows.append(row)
        elif foundRows and row[0][0] == '=':
            foundRows = False
    
    return rows

def emit_temperature_update(rows):
    print "\x1B[01;37;42mSending update for Day %s...\x1B[00m" % (rows[0][0])
    print "\x1B[01;37;42m - range(%d, %d)\x1B[00m" % (int(rows[0][2]), int(rows[0][1]))
    temp_cell.update(dpropjson.dumps({'type': 'range',
                                      'min': int(rows[0][2]),
                                      'max': int(rows[0][1])}))
    
    if len(rows[1:]) > 0:
        gobject.timeout_add(2000, lambda: emit_temperature_update(rows[1:]))
    
    return False

def mergeRange(cell, raw_data, peer):
    update = dpropjson.loads(raw_data)
    if str(peer) == cell.url():
        print "\x1B[01;37;45mGot Local Update: %s\x1B[00m" % (`update`)
    else:
        print "\x1B[01;37;46mGot Remote Update: %s\x1B[00m" % (`update`)
    data = dpropjson.loads(cell.data())
    
    if not isinstance(update, dict):
        print "\x1B[01;37;41mDon't know how to handle data type.  Not merging.\x1B[0m"
    elif 'type' not in update:
        print "\x1B[01;37;41mCan't handle arbitrary dicts.  Not merging.\x1B[0m"
    elif update['type'] == 'range':
        if 'min' not in update or 'max' not in update:
            print "\x1B[01;37;41mHey, this isn't a range type!  Not merging.\x1B[0m"
        elif not isinstance(update['min'], (int, long, float)) or not isinstance(update['max'], (int, long, float)):
            print "\x1B[01;37;41mHey, these maxs/mins aren't numbers!  Not merging.\x1B[0m"
        elif isinstance(data, dpropjson.Nothing):
            # Initialize the data.
            cell.set(raw_data)
            print "\x1B[01;37;44mCell empty! Set to %s\x1B[0m" % (`str(raw_data)`)
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
                print "\x1B[01;37;44mCell now set to %s\x1B[0m" % (`data`)
    else:
        print "\x1B[01;37;41mDon't know how to handle this type.  Not merging.\x1B[0m"

temps = fetch_temperatures('20100228')

dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)

bus = dbus.SystemBus()
try:
    # UUID for the temp. cell
    name = '/pipian.com:37767/Cells/c4add3d03cf311df98790800200c9a66'
    url = "https:/%s" % (name)
    
    temp_cell = pydprop.RemoteCell(url, mergeRange)
except dbus.DBusException:
    traceback.print_exc()
    print usage
    sys.exit(1)

gobject.timeout_add(4000, lambda: emit_temperature_update(temps))

loop = gobject.MainLoop()
loop.run()
