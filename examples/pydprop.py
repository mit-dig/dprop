import dbus, uripath

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
        self.uuid = propman.registerCell(uuid,
                                         dbus_interface='edu.mit.csail.dig.DPropMan')
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
        self.uuid = propman.registerRemoteCell(self.url,
                                               dbus_interface='edu.mit.csail.dig.DPropMan')
        self.dbusCell = bus.get_object('edu.mit.csail.dig.DPropMan',
                                       '/Cells/%s' % (self.uuid))
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
    
    iterStack.append((iter(sorted(ancA['ancestors'].keys())),
                      iter(sorted(ancB['ancestors'].keys()))))
    ancBStack.append(ancB['ancestors'])
    ancCStack.append(ancC['ancestors'])
    
    itemA = None
    itemB = None
    
    while len(stack) > 0:
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
            while True:
                try:
                    itemB = iters[1].next()
                except StopIteration:
                    break
                curAncC[itemB] = curAncB[itemB]
            continue
        
        try:
            if itemB is None:
                itemB = iters[1].next()
        except StopIteration:
            # ancB ran out of items.  We're done here.
            continue
        
        if itemA < itemB:
            # ancA has something ancB doesn't.  We don't need to add
            # to ancC (as it has all of ancA's items alread.
            itemA = None
        elif itemB < itemA:
            # ancB has something ancA doesn't.  We can just add it to ancC.
            itemB = None
            curAncC[itemB] = curAncB[itemB]
        
        # Always push the existing state back on if we haven't
        # finished with a level.
        iterStack.push(iters)
        ancBStack.push(curAncB)
        ancCStack.push(curAncC)
        
        # And if the items were equal, then push an extra layer on to
        # dig deeper.
        if itemA == itemB:
            iterStack.push((iter(sorted(curAncC[itemA]['ancestors'].keys())),
                            iter(sorted(curAncB[itemB]['ancestors'].keys()))))
            ancBStack.push(curAncB[itemB]['ancestors'])
            ancCStack.push(curAncC[itemA]['ancestors'])
    
    return ancC

class ProvenanceAwareCell(Cell):
    def mainCellMerge(self, raw_data, peer):
        data = dpropjson.loads(raw_data)
        curData = dpropjson.loads(self.mainCell.data())
        
        if not isinstance(data, dict):
            print "Main cell received non-dict.  Not merging."
        elif 'type' not in data:
            print "Main update missing type!  Not merging."
        elif 'type' != 'dpropMainCell':
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
        elif 'type' == 'dpropProvCell':
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
        elif 'type' == 'dpropProvenance':
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
            print "Provenance cell received non-dict.  Not merging."
        elif 'type' not in data:
            print "Provenance update missing type!  Not merging."
        elif 'type' == 'dpropProvCell':
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
        elif 'type' == 'dpropData':
            if 'data' not in data:
                print "Data update missing data!  Not merging."
            else:
                mergeFunction(self, dpropjson.dumps(data['data']), peer)
        else:
            print "Data update had unexpected type!  Not merging."
    
    def defaultProvMergeFunction(self, raw_data, peer):
        data = dpropjson.loads(raw_data)
        curData = dpropjson.loads(self.dataCell.data())
        
        # Already done typechecking.
        if data['type'] == 'dpropProvCell':
            # Merge the provenances, so as to not clobber our data.
            newProv = data['data']
            oldProv = curData['data']
            
            if 'type' not in newProv:
                print "Unexpected provenance data type.  Not merging."
            elif 'ancestors' not in newProv:
                print "Missing ancestors in provenance data type.  Not merging."
            elif isinstance(oldProv, dpropjson.Nothing):
                self.provCell.set(raw_data)
            elif 'type' not in oldProv:
                print "Unexpected local provenance data type.  Not merging."
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
                        if mergedAncestors != oldProv['ancestors'][ancestor]:
                            oldProv['ancestors'][ancestor] = mergedAncestors
                            changed = True
                    else:
                        oldProv['ancestors'][ancestor] = newProv['ancestors'][ancestor]
                        changed = True
                
                if changed:
                    curData.update({'data': oldProv})
                    self.provCell.set(dpropjson.dumps(curData))
        elif data['type'] == 'dpropProvenance':
            # Update the provenance.
            newProv = data['data']
            oldProv = curData['data']
            
            if 'id' not in newProv:
                print "Unexpected provenance data type.  Not merging."
            elif 'ancestors' not in oldProv:
                print "Unexpected local provenance data type.  Not merging."
            elif newProv['id'] not in oldProv['ancestors']:
                oldProv['ancestors'][newProv['id']] = newProv
                curData.update({'data': oldProv})
                self.provCell.set(dpropjson.dumps(curData))
            elif 'ancestors' not in oldProv['ancestors'][newProv['id']]:
                print "Unexpected local provenance ancestor data type.  Not merging."
            else:
                # To be on the safe side, merge the ancestors.
                changed = False
                mergedAncestors = mergeAncestors(
                    oldProv['ancestors'][newProv['id']],
                    newProv)
                if mergedAncestors != oldProv['ancestors'][newProv['id']]:
                    oldProv['ancestors'][newProv['id']] = mergedAncestors
                    changed = True
                
                if changed:
                    curData.update({'data': oldProv})
                    self.provCell.set(dpropjson.dumps(curData))
        else:
            print "Provenance update had unexpected type!  Not merging."

    def addNeighbor(self, propagator):
        if isinstance(propagator, ProvenanceAwarePropagator):
            self.provCell.connect_to_signal('NotifyPropagatorsSignal',
                                            lambda: propagator.provFunction())
        self.dataCell.connect_to_signal('NotifyPropagatorsSignal',
                                        lambda: propagator.function(),
                                        dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
        propagator.function()
    
    def url(self):
        return self.mainCell.url()
    
    def updateProvenance(self, data):
        self.provCell.updateCell(data,
                                 dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    
    def setProvenance(self, data):
        self.provCell.changeCell(data,
                                 dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
    
    def provenanceData(self):
        return str(self.provCell.data(dbus_interface='edu.mit.csail.dig.DPropMan.Cell'))

class ProvenanceAwareLocalCell(ProvenanceAwareCell, LocalCell):
    def __init__(self, mainUUID, provUUID, dataUUID, mergeFunction, provMergeFunction=None):
        self.mainCell = LocalCell(mainUUID, lambda raw_data, peer: self.mainCellMerge(str(raw_data), str(peer)))
        
        self.provCell = LocalCell(provUUID, lambda raw_data, peer: self.provCellMerge(provMergeFunction, str(raw_data), str(peer)))
        self.dataCell = LocalCell(dataUUID, lambda raw_data, peer: self.dataCellMerge(mergeFunction, str(raw_data), str(peer)))
        
        self.dbusCell = self.dataCell
        
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
        self.mainCell = RemoteCell(mainURL, lambda raw_data, peer: self.mainCellMerge(str(raw_data), str(peer)))
        
        mainData = dpropjson.loads(self.mainCell.data())
        if isinstance(mainData, dict):
            if 'type' not in mainData or 'provCell' not in mainData or 'dataCell' not in mainData:
                raise DPropException("Main cell doesn't match expected cell type!")
            elif mainData['type'] != 'dpropMainCell':
                raise DPropException("Main cell wasn't a dpropMainCell!")
        else:
            raise DPropException("Main cell didn't contain a dictionary!")
        
        # URL for the provCell is relative to mainCell.
        provURL = uripath.join(mainURL, mainData['provCell'])
        self.provCell = RemoteCell(provURL, lambda raw_data, peer: self.provCellMerge(provMergeFunction, str(raw_data), str(peer)))
        dataURL = uripath.join(mainURL, mainData['dataCell'])
        self.dataCell = RemoteCell(dataURL, lambda raw_data, peer: self.dataCellMerge(mergeFunction, str(raw_data), str(peer)))
        
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
        if isinstance(dataData, dict):
            if 'type' not in dataData or 'mainCell' not in dataData or 'data' not in dataData:
                raise DPropException("Data cell doesn't match expected cell type!")
            elif dataData['type'] != 'dpropDataCell':
                raise DPropException("Data cell wasn't a dpropDataCell!")
            elif dataData['mainCell'] != self.mainCell.uuid:
                raise DPropException("Data cell's mainCell doesn't match expected UUID!")
        else:
            raise DPropException("Data cell didn't contain a dictionary!")

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
        provenance = {'type': 'dpropProvenance',
                      'id': self.id,
                      'ancestors': {}}
        
        for neighbor in self.neighbors:
            if isinstance(neighbor, ProvenanceAwarePropagator):
                provenance['ancestors'][neighbor.url()] = dpropjson.loads(neighbor.provenanceData())
            else:
                provenance['ancestors'][neighbor.url()] = dpropjson.Nothing()
        
        for output in outputs:
            if isinstance(neighbor, ProvenanceAwarePropagator):
                output.updateProvenance(dpropjson.dumps(provenance))
