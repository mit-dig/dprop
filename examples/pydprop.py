import dbus, uripath, gobject, dpropjson, socket

class DPropException(Exception):
    pass

class Cell:
    def addNeighbor(self, propagator):
        self.dbusCell.connect_to_signal('NotifyPropagatorsSignal',
                                        lambda: propagator.function(),
                                        dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
        propagator.function()
    
    def update(self, data):
        self.dbusCell.updateCell(data,
                                 dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    
    def set(self, data):
        self.dbusCell.changeCell(data,
                                 dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    
    def data(self):
        return str(self.dbusCell.data(dbus_interface='edu.mit.csail.dig.DPropMan.Cell'))
    
    def url(self):
        return str(self.dbusCell.url(dbus_interface='edu.mit.csail.dig.DPropMan.Cell'))

class LocalCell(Cell):
    def __init__(self, uuid, mergeFunction):
        bus = dbus.SystemBus()
        propman = bus.get_object('edu.mit.csail.dig.DPropMan',
                                 '/DPropMan')
        self.uuid = str(propman.registerCell(uuid,
                                             dbus_interface='edu.mit.csail.dig.DPropMan'))
        self.dbusCell = bus.get_object('edu.mit.csail.dig.DPropMan',
                                       '/Cells/%s' % (self.uuid))
        self.dbusCell.connect_to_signal('UpdateSignal',
                                        lambda raw_data, peer: mergeFunction(self, str(raw_data), str(peer)),
                                        dbus_interface='edu.mit.csail.dig.DPropMan.Cell')

class RemoteCell(Cell):
    def __init__(self, url, mergeFunction):
        bus = dbus.SystemBus()
        propman = bus.get_object('edu.mit.csail.dig.DPropMan',
                                 '/DPropMan')
        self.url = url
        self.uuid = str(propman.registerRemoteCell(self.url,
                                                   dbus_interface='edu.mit.csail.dig.DPropMan'))
        self.dbusCell = bus.get_object('edu.mit.csail.dig.DPropMan',
                                       '/Cells/%s' % (self.uuid))
        print "registerin' signal"
        self.dbusCell.connect_to_signal('UpdateSignal',
                                        lambda raw_data, peer: mergeFunction(self, str(raw_data), str(peer)),
                                        dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
        self.dbusCell.connectToRemote(self.url,
                                      dbus_interface='edu.mit.csail.dig.DPropMan.Cell')

def mergeAncestors(ancA, ancB):
    iterStack = []
    ancBStack = []
    ancCStack = []
    ancC = ancA
    
    iterStack.append((iter(sorted(ancC['ancestors'].keys())),
                      iter(sorted(ancB['ancestors'].keys()))))
    ancBStack.append(ancB['ancestors'])
    ancCStack.append(ancC['ancestors'])
    
    itemA = None
    itemB = None
    
    while len(iterStack) > 0:
        # Get the last iterator we were walking through.
        iters = iterStack.pop()
        curAncB = ancBStack.pop()
        curAncC = ancCStack.pop()
        
        # Try getting the next item from both iterators.
        try:
            if itemA is None:
                itemA = iters[0].next()
        except StopIteration:
            # ancA ran out of items.  Add all of the ancB items.
            itemA = None
            print "curAncC ran out of items..."
            while True:
                try:
                    itemB = iters[1].next()
                except StopIteration:
                    print "curAncB ran out of items..."
                    itemB = None
                    break
                print "copying %s into curAncC..." % (itemB)
                curAncC[itemB] = curAncB[itemB]
            continue
        
        try:
            if itemB is None:
                itemB = iters[1].next()
        except StopIteration:
            # ancB ran out of items.  We're done here.
            print "curAncB ran out of items..."
            itemA = None
            itemB = None
            continue
        
        if itemA < itemB:
            # ancA has something ancB doesn't.  We don't need to add
            # to ancC (as it has all of ancA's items alread.
            print "%s < %s" % (itemA, itemB)
            itemA = None
        elif itemB < itemA:
            # ancB has something ancA doesn't.  We can just add it to ancC.
            print "%s > %s" % (itemA, itemB)
            itemB = None
            print "copying %s into curAncC" % (itemB)
            curAncC[itemB] = curAncB[itemB]
        
        # Always push the existing state back on if we haven't
        # finished with a level.
        iterStack.append(iters)
        ancBStack.append(curAncB)
        ancCStack.append(curAncC)
        
        # And if the items were equal, then push an extra layer on to
        # dig deeper.
        if itemA == itemB:
            print "%s == %s" % (itemA, itemB)
            iterStack.append((iter(sorted(curAncC[itemA]['ancestors'].keys())),
                              iter(sorted(curAncB[itemB]['ancestors'].keys()))))
            ancBStack.append(curAncB[itemB]['ancestors'])
            ancCStack.append(curAncC[itemA]['ancestors'])
            itemA = None
            itemB = None
            
    print ancC
    
    return ancC

class ProvenanceAwareCell(Cell):
    def mainCellMerge(self, raw_data, peer):
        data = dpropjson.loads(raw_data)
        curData = dpropjson.loads(self.mainCell.data())
        
        print "mainCellMerge"
        if not isinstance(data, dict):
            print "Main cell received non-dict.  Not merging."
        elif 'type' not in data:
            print "Main update missing type!  Not merging."
        elif data['type'] != 'dpropMainCell':
            print "Main update had unexpected type!  Not merging."
        elif isinstance(curData, dpropjson.Nothing):
            self.mainCell.set(raw_data)
        else:
            print "Main cell already set!  Not merging."
    
    def provCellMerge(self, provMergeFunction, raw_data, peer):
        data = dpropjson.loads(raw_data)
        curData = dpropjson.loads(self.provCell.data())
        
        if not isinstance(data, dict):
            print "Provenance cell received non-dict.  Not merging."
        elif 'type' not in data:
            print "Provenance update missing type!  Not merging."
        elif data['type'] == 'dpropProvCell':
            if isinstance(curData, dpropjson.Nothing):
                self.provCell.set(raw_data)
            else:
                if 'mainCell' not in data:
                    print "Provenance update missing main cell!  Not merging."
                elif 'mainCell' not in curData:
                    print "Unexpected local provenance!  Not merging."
                elif data['mainCell'] != curData['mainCell']:
                    print "Main cell pointer in local provenance didn't match!  Not merging."
                elif 'data' not in data:
                    print "Provenance update missing data!  Not merging."
                elif 'data' not in curData:
                    print "Local provenance missing data!  Not merging."
                elif provMergeFunction is not None:
                    provMergeFunction(self, raw_data, peer)
                else:
                    self.defaultProvMergeFunction(self, raw_data, peer)
        elif data['type'] == 'dpropProvenance':
            if 'data' not in data:
                print "Provenance update missing data!  Not merging."
            elif 'data' not in curData:
                print "Local provenance missing data!  Not merging."
            elif provMergeFunction is not None:
                provMergeFunction(self, raw_data, peer)
            else:
                self.defaultProvMergeFunction(raw_data, peer)
        else:
            print "Provenance update had unexpected type!  Not merging."
    
    def dataCellMerge(self, mergeFunction, raw_data, peer):
        data = dpropjson.loads(raw_data)
        curData = dpropjson.loads(self.dataCell.data())
        
        if not isinstance(data, dict):
            if isinstance(curData, dict) and 'type' in curData and curData['type'] == 'dpropDataCell':
                mergeFunction(self, raw_data, peer)
            else:
                print "Data cell received non-dict.  Not merging."
        elif 'type' not in data:
            if isinstance(curData, dict) and 'type' in curData and curData['type'] == 'dpropDataCell':
                mergeFunction(self, raw_data, peer)
            else:
                print "Data update missing type!  Not merging."
        elif data['type'] == 'dpropDataCell':
            if isinstance(curData, dpropjson.Nothing):
                self.dataCell.set(raw_data)
            else:
                if 'mainCell' not in data:
                    print "Data update missing main cell!  Not merging."
                elif 'mainCell' not in curData:
                    print "Unexpected local data!  Not merging."
                elif data['mainCell'] != curData['mainCell']:
                    print "Main cell pointer in local data didn't match!  Not merging."
                elif 'data' not in data:
                    print "Data update missing data!  Not merging."
                else:
                    mergeFunction(self, dpropjson.dumps(data['data']), peer)
        else:
            if isinstance(curData, dict) and 'type' in curData and curData['type'] == 'dpropDataCell':
                mergeFunction(self, raw_data, peer)
            else:
                print "Data update has unrecognized type!  Not merging."
    
    def defaultProvMergeFunction(self, raw_data, peer):
        data = dpropjson.loads(raw_data)
        curData = dpropjson.loads(self.provCell.data())
        
        print "current provenance data: %s" % (self.provCell.data())
        
        # Already done typechecking.
        if data['type'] == 'dpropProvCell':
            # Merge the provenances, so as to not clobber our data.
            newProv = data['data']
            oldProv = curData['data']
            
            if 'ancestors' not in newProv:
                print "Missing ancestors in provenance data type.  Not merging."
            elif isinstance(oldProv, dpropjson.Nothing):
                self.provCell.set(raw_data)
            elif 'ancestors' not in oldProv:
                print "Missing ancestors in local provenance.  Not merging."
            else:
                # Merge the contents of ancestors INTELLIGENTLY (as we
                # may be clobbering with outdated provenance.)
                changed = False
                for ancestor in newProv['ancestors']:
                    if ancestor in oldProv['ancestors']:
                        mergedAncestors = mergeAncestors(
                            oldProv['ancestors'][ancestor],
                            newProv['ancestors'][ancestor])
#                        if mergedAncestors != oldProv['ancestors'][ancestor]:
                        oldProv['ancestors'][ancestor] = mergedAncestors
                        changed = True
                    else:
                        oldProv['ancestors'][ancestor] = newProv['ancestors'][ancestor]
                        changed = True
                
                if changed:
                    curData['data'] = oldProv
                    self.provCell.set(dpropjson.dumps(curData))
        elif data['type'] == 'dpropProvenance':
            # Update the provenance.
            newProv = data['data']
            oldProv = curData['data']
            
            if 'id' not in newProv:
                print "Unexpected provenance data type.  Not merging."
            elif isinstance(oldProv, dpropjson.Nothing):
                oldProv = {'ancestors': {newProv['id']: newProv}}
                curData['data'] = oldProv
                self.provCell.set(dpropjson.dumps(curData))
            elif 'ancestors' not in oldProv:
                print "Unexpected local provenance data type.  Not merging."
            elif newProv['id'] not in oldProv['ancestors']:
                oldProv['ancestors'][newProv['id']] = newProv
                curData['data'] = oldProv
                self.provCell.set(dpropjson.dumps(curData))
            elif 'ancestors' not in oldProv['ancestors'][newProv['id']]:
                print "Unexpected local provenance ancestor data type.  Not merging."
            else:
                # To be on the safe side, merge the ancestors.
                changed = False
                mergedAncestors = mergeAncestors(
                    oldProv['ancestors'][newProv['id']],
                    newProv)
#                if mergedAncestors != oldProv['ancestors'][newProv['id']]:
                oldProv['ancestors'][newProv['id']] = mergedAncestors
                changed = True
                
                if changed:
                    print "Done merging provenance update!"
                    curData['data'] = oldProv
                    self.provCell.set(dpropjson.dumps(curData))
        else:
            print "Provenance update had unexpected type!  Not merging."

    def addNeighbor(self, propagator):
        if isinstance(propagator, ProvenanceAwarePropagator):
            self.provCell.dbusCell.connect_to_signal('NotifyPropagatorsSignal',
                                                     lambda: propagator.provFunction())
        self.dataCell.dbusCell.connect_to_signal('NotifyPropagatorsSignal',
                                                 lambda: propagator.function(),
                                                 dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
        propagator.function()
    
    def url(self):
        return self.mainCell.url()
    
    def set(self, data):
        data = {'type': 'dpropDataCell',
                'mainCell': self.mainCell.uuid,
                'data': dpropjson.loads(data)}
        self.dataCell.set(dpropjson.dumps(data))
    
    def data(self):
        if isinstance(self.dataCell.data(), dpropjson.Nothing):
            return self.dataCell.data()
        else:
            data = dpropjson.loads(self.dataCell.data())
            if 'type' not in data:
                return self.dataCell.data()
            elif data['type'] == 'dpropDataCell':
                return dpropjson.dumps(data['data'])
            else:
                return self.dataCell.data()
    
    def updateProvenance(self, data):
        self.provCell.update(data)
    
    def setProvenance(self, data):
        self.provCell.change(data)
    
    def provenanceData(self):
        data = dpropjson.loads(self.provCell.data())
        if isinstance(data, dpropjson.Nothing):
            return self.provCell.data()
        else:
            if 'type' not in data:
                return self.provCell.data()
            elif data['type'] == 'dpropProvCell':
                return dpropjson.dumps(data['data'])
            else:
                return self.provCell.data()

class ProvenanceAwareLocalCell(ProvenanceAwareCell, LocalCell):
    def __init__(self, mainUUID, provUUID, dataUUID, mergeFunction, provMergeFunction=None):
        self.mainCell = LocalCell(mainUUID, lambda mainCell, raw_data, peer: self.mainCellMerge(str(raw_data), str(peer)))
        self.uuid = self.mainCell.uuid
        
        self.provCell = LocalCell(provUUID, lambda provCell, raw_data, peer: self.provCellMerge(provMergeFunction, str(raw_data), str(peer)))
        self.dataCell = LocalCell(dataUUID, lambda dataCell, raw_data, peer: self.dataCellMerge(mergeFunction, str(raw_data), str(peer)))
        
        self.dbusCell = self.dataCell.dbusCell
        
        mainData = dpropjson.loads(self.mainCell.data())
        if isinstance(mainData, dpropjson.Nothing):
            self.mainCell.update(dpropjson.dumps({'type': 'dpropMainCell',
                                                  'provCell': self.provCell.uuid,
                                                  'dataCell': self.dataCell.uuid}))
        elif isinstance(mainData, dict):
            if 'type' not in mainData or 'provCell' not in mainData or 'dataCell' not in mainData:
                raise DPropException("Main cell doesn't match expected cell type!")
            elif mainData['type'] != 'dpropMainCell':
                raise DPropException("Main cell wasn't a dpropMainCell!")
            elif mainData['provCell'] != self.provCell.uuid:
                raise DPropException("Main cell's provCell doesn't match expected UUID!")
            elif mainData['dataCell'] != self.dataCell.uuid:
                raise DPropException("Main cell's dataCell doesn't match expected UUID!")
        else:
            raise DPropException("Main cell didn't contain a dictionary!")
        
        provData = dpropjson.loads(self.provCell.data())
        if isinstance(provData, dpropjson.Nothing):
            self.provCell.update(dpropjson.dumps({'type': 'dpropProvCell',
                                                  'mainCell': self.mainCell.uuid,
                                                  'data': provData}))
        elif isinstance(provData, dict):
            if 'type' not in provData or 'mainCell' not in provData or 'data' not in provData:
                raise DPropException("Provenance cell doesn't match expected cell type!")
            elif provData['type'] != 'dpropProvCell':
                raise DPropException("Provenance cell wasn't a dpropProvCell!")
            elif provData['mainCell'] != self.mainCell.uuid:
                raise DPropException("Provenance cell's mainCell doesn't match expected UUID!")
        else:
            raise DPropException("Provenance cell didn't contain a dictionary!")

        dataData = dpropjson.loads(self.dataCell.data())
        if isinstance(dataData, dpropjson.Nothing):
            self.dataCell.update(dpropjson.dumps({'type': 'dpropDataCell',
                                                  'mainCell': self.mainCell.uuid,
                                                  'data': dataData}))
        elif isinstance(dataData, dict):
            if 'type' not in dataData or 'mainCell' not in dataData or 'data' not in dataData:
                raise DPropException("Data cell doesn't match expected cell type!")
            elif dataData['type'] != 'dpropDataCell':
                raise DPropException("Data cell wasn't a dpropDataCell!")
            elif dataData['mainCell'] != self.mainCell.uuid:
                raise DPropException("Data cell's mainCell doesn't match expected UUID!")
        else:
            raise DPropException("Data cell didn't contain a dictionary!")

class ProvenanceAwareRemoteCell(ProvenanceAwareCell, RemoteCell):
    def __init__(self, mainURL, mergeFunction, provMergeFunction=None):
        # Try to be smart.  We might have accidentally been passed the
        # provCell or dataCell.
        bus = dbus.SystemBus()
        propman = bus.get_object('edu.mit.csail.dig.DPropMan',
                                 '/DPropMan')
        self.initURLType = None
        self.url = mainURL
        self.uuid = str(propman.registerRemoteCell(self.url,
                                                   dbus_interface='edu.mit.csail.dig.DPropMan'))
        self.dbusCell = bus.get_object('edu.mit.csail.dig.DPropMan',
                                       '/Cells/%s' % (self.uuid))
        self.dbusCell.connect_to_signal('UpdateSignal',
                                        lambda raw_data, peer: self.typeChecker(str(raw_data), str(peer)),
                                        dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
        self.dbusCell.connectToRemote(self.url,
                                      dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
        
        # Wait for the typeChecker to have gotten something.
        # WARNING: We assume python-gobject is being used.
        while self.initURLType is None:
            gobject.MainLoop().get_context().iteration(True)
        
        self.provCell = None
        self.dataCell = None
        if self.initURLType == 'dpropProvCell':
            print "Actually passed a provenance cell!"
            
            provURL = mainURL
            self.provCell = RemoteCell(provURL, lambda provCell, raw_data, peer: self.provCellMerge(provMergeFunction, str(raw_data), str(peer)))
            
            self.provCellMerge(provMergeFunction,
                               str(self.initRawData),
                               str(self.initPeer))
            
            provData = dpropjson.loads(self.provCell.data())
            if isinstance(provData, dict):
                if 'type' not in provData or 'mainCell' not in provData or 'data' not in provData:
                    raise DPropException("Provenance cell doesn't match expected cell type!")
                elif provData['type'] != 'dpropProvCell':
                    raise DPropException("Provenance cell wasn't a dpropProvCell!")
            else:
                raise DPropException("Provenance cell didn't contain a dictionary!")
            
            mainURL = uripath.join(provURL, provData['provCell'])
        elif self.initURLType == 'dpropDataCell':
            print "Actually passed a data cell!"
            
            dataURL = mainURL
            self.dataCell = RemoteCell(dataURL, lambda dataCell, raw_data, peer: self.dataCellMerge(mergeFunction, str(raw_data), str(peer)))
            
            self.dataCellMerge(mergeFunction,
                               str(self.initRawData),
                               str(self.initPeer))
            
            dataData = dpropjson.loads(self.dataCell.data())
            print dataData
            if isinstance(dataData, dict):
                if 'type' not in dataData or 'mainCell' not in dataData or 'data' not in dataData:
                    raise DPropException("Data cell doesn't match expected cell type!")
                elif dataData['type'] != 'dpropDataCell':
                    raise DPropException("Data cell wasn't a dpropDataCell!")
            else:
                raise DPropException("Data cell didn't contain a dictionary!")
            
            mainURL = uripath.join(dataURL, dataData['mainCell'])

        print mainURL
        self.mainCell = RemoteCell(mainURL, lambda mainCell, raw_data, peer: self.mainCellMerge(str(raw_data), str(peer)))
        self.uuid = self.mainCell.uuid
        
        if self.initURLType == 'dpropMainCell':
            print "Passed a main cell"
            self.mainCellMerge(str(self.initRawData),
                               str(self.initPeer))
        
        mainData = dpropjson.loads(self.mainCell.data())
        
        while isinstance(mainData, dpropjson.Nothing):
            # Gotta wait a few cycles (I really don't like this...)
            gobject.MainLoop().get_context().iteration(True)
            mainData = dpropjson.loads(self.mainCell.data())
        
        if isinstance(mainData, dict):
            if 'type' not in mainData or 'provCell' not in mainData or 'dataCell' not in mainData:
                raise DPropException("Main cell doesn't match expected cell type!")
            elif mainData['type'] != 'dpropMainCell':
                raise DPropException("Main cell wasn't a dpropMainCell!")
        else:
            raise DPropException("Main cell didn't contain a dictionary!")
        
        # URL for the provCell is relative to mainCell.
        if self.provCell == None:
            provURL = uripath.join(mainURL, mainData['provCell'])
            self.provCell = RemoteCell(provURL, lambda provCell, raw_data, peer: self.provCellMerge(provMergeFunction, str(raw_data), str(peer)))
        if self.dataCell == None:
            dataURL = uripath.join(mainURL, mainData['dataCell'])
            self.dataCell = RemoteCell(dataURL, lambda dataCell, raw_data, peer: self.dataCellMerge(mergeFunction, str(raw_data), str(peer)))
        
        provData = dpropjson.loads(self.provCell.data())
        
        while isinstance(provData, dpropjson.Nothing):
            # Gotta wait a few cycles (I really don't like this...)
            gobject.MainLoop().get_context().iteration(True)
            provData = dpropjson.loads(self.provCell.data())
        
        if isinstance(provData, dict):
            if 'type' not in provData or 'mainCell' not in provData or 'data' not in provData:
                raise DPropException("Provenance cell doesn't match expected cell type!")
            elif provData['type'] != 'dpropProvCell':
                raise DPropException("Provenance cell wasn't a dpropProvCell!")
            elif provData['mainCell'] != self.mainCell.uuid:
                raise DPropException("Provenance cell's mainCell doesn't match expected UUID!")
        else:
            raise DPropException("Provenance cell didn't contain a dictionary!")

        dataData = dpropjson.loads(self.dataCell.data())
        
        while isinstance(dataData, dpropjson.Nothing):
            # Gotta wait a few cycles (I really don't like this...)
            gobject.MainLoop().get_context().iteration(True)
            dataData = dpropjson.loads(self.dataCell.data())
        
        if isinstance(dataData, dict):
            if 'type' not in dataData or 'mainCell' not in dataData or 'data' not in dataData:
                raise DPropException("Data cell doesn't match expected cell type!")
            elif dataData['type'] != 'dpropDataCell':
                raise DPropException("Data cell wasn't a dpropDataCell!")
            elif dataData['mainCell'] != self.mainCell.uuid:
                raise DPropException("Data cell's mainCell doesn't match expected UUID!")
        else:
            raise DPropException("Data cell didn't contain a dictionary!")
    
    def typeChecker(self, raw_data, peer):
        # A dummy "merge" function that just lets us know what type a
        # cell is.
        if self.initURLType is not None:
            return
        
        data = dpropjson.loads(str(raw_data))
        if not isinstance(data, dict):
            self.initURLType = 'dpropDataCell'
        elif 'type' not in data:
            self.initURLType = 'dpropDataCell'
        elif data['type'] == 'dpropMainCell':
            self.initURLType = 'dpropMainCell'
        elif data['type'] == 'dpropProvCell' or data['type'] == 'dpropProvenance':
            self.initURLType = 'dpropProvCell'
        else:
            self.initURLType = 'dpropDataCell'
        
        self.initRawData = raw_data
        self.initPeer = peer

class Propagator:
    def __init__(self, id, neighbors, function):
        self.id = id
        self.myFunction = function
        
        for neighbor in neighbors:
            neighbor.addNeighbor(self)
    
    def function(self):
        self.myFunction()

class ProvenanceAwarePropagator(Propagator):
    def __init__(self, id, neighbors, outputs, function, provFunction = None):
        self.id = id
        self.myFunction = function
        self.myProvFunction = provFunction
        self.hasPropagated = False
        self.neighbors = neighbors
        self.outputs = outputs
        
        for neighbor in self.neighbors:
            neighbor.addNeighbor(self)
    
    def function(self):
        self.myFunction()
        
        if not self.hasPropagated:
            self.hasPropagated = True
            self.provFunction()
    
    def provFunction(self):
        if not self.hasPropagated:
            return
        
        if self.myProvFunction is not None:
            self.myProvFunction()
        else:
            self.defaultProvFunction()
    
    def defaultProvFunction(self):
        # Make sure we add the host that the propagator ran on.
        provenance = {'type': 'dpropProvenance',
                      'data': {'id': self.id,
                               'ancestors': {},
                               'host': socket.getfqdn()}}
        
        for neighbor in self.neighbors:
            if isinstance(neighbor, ProvenanceAwareCell):
                print "Making propagator provenance from:"
                print neighbor.provenanceData()
                provenance['data']['ancestors'][neighbor.uuid] = dpropjson.loads(neighbor.provenanceData())
                provenance['data']['ancestors'][neighbor.uuid]['id'] = neighbor.uuid
            else:
                provenance['data']['ancestors'][neighbor.uuid] = dpropjson.Nothing()
#                provenance['data']['ancestors'][neighbor.uuid]['id'] = neighbor.uuid
        
        for output in self.outputs:
            if isinstance(neighbor, ProvenanceAwareCell):
                output.updateProvenance(dpropjson.dumps(provenance))
