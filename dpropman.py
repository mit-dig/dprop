import gobject, traceback, sys, urllib, httplib, socket, re, sys
from urlparse import urlparse
import dbus, dbus.service, dbus.mainloop.glib
import twisted.internet.glib2reactor
if __name__ == '__main__':
    twisted.internet.glib2reactor.install()
import twisted.web.resource, twisted.internet.reactor, twisted.web.server, twisted.internet.defer, twisted.web.error
import OpenSSL.SSL

# TODO: Can this ever not be set?
HostName = socket.getfqdn()
Port = 37767
UseSSL = False

def makeETag(data):
    """Generate an ETag."""
    if hash(data) < 0:
        return "%08X" % (hash(data) + 2 * (sys.maxint + 1))
    else:
        return "%08X" % (hash(data))

class Cell(dbus.service.Object):
    """A local cell in a propagator network."""
    
    def __init__(self, conn, name, object_path):
        """Initialize the Cell."""
        self.object_path = object_path
        self.name = name
        self.data = 'NULL'
        self.neighbors = {}
        if UseSSL:
            self.referer = "https://%s:%d/Cells/%s" % (HostName, Port, name)
        else:
            self.referer = "http://%s:%d/Cells/%s" % (HostName, Port, name)
        dbus.service.Object.__init__(self, conn, object_path)
    
    @dbus.service.signal('edu.mit.csail.dig.DPropMan.Cell')
    def ChangeSignal(self, message):
        """Signals a change in the cell's contents."""
        pass
    
    @dbus.service.signal('edu.mit.csail.dig.DPropMan.Cell')
    def DestroySignal(self):
        """Signals the destruction of the cell."""
        pass
    
    def notifyNeighbors(self):
        """Notify all neighboring remote cells of any data changes."""
        for neighbor in self.neighbors:
            # TODO: Why not?
            # Need to forward the change along to the remote cells.
            if not self.neighbors[neighbor]['notified'] and len(self.neighbors[neighbor]['data']) > 0:
                try:
                    url = urlparse(neighbor)
                    if url.scheme == 'http':
                        h = httplib.HTTPConnection(url.netloc)
                    elif url.scheme == 'https':
                        h = httplib.HTTPSConnection(url.netloc, key_file='./server.pem', cert_file='./server.pem')
                    h.request('POST', url.path,
                              urllib.urlencode({'hash': makeETag(self.neighbors[neighbor]['data'][0])}),
                              {'Referer': self.referer,
                               'Content-Type': 'application/x-www-form-urlencoded'})
                    # TODO: If cells are remote cells?
                    resp = h.getresponse()
                    if resp.status == httplib.NOT_MODIFIED:
                        self.neighbors[neighbor]['data'].pop(0)
                    elif resp.status != httplib.ACCEPTED:
                        # TODO: Handle errors
                        pass
                    h.close()
                    if resp.status == httplib.ACCEPTED:
                        self.neighbors[neighbor]['notified'] = True
                except httplib.HTTPException:
                    # TODO: Handle exceptions
                    pass
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan.Cell',
                         in_signature='s')
    def changeCell(self, data):
        """Locally change the cell's contents to data."""
        # TODO: Track update rate for remote cells.
        if self.data != str(data):
            for neighbor in self.neighbors.keys():
                self.neighbors[neighbor]['data'].append(str(data))
            self.data = str(data)
            self.ChangeSignal(str(data))
            self.notifyNeighbors()
    
    def remoteChangeCell(self, data):
        """Change the cell's contents remotely."""
        if self.data != str(data):
            # Shift merge into programs; so we don't update OUR data yet.
#            self.data = data
            self.ChangeSignal(str(data))
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan.Cell',
                         out_signature='s')
    def data(self):
        """Return the cell's data."""
        return self.data
    
    def remoteData(self, neighbor):
        """Return the next data a particular neighbor should see and the
        number of changes still remaining.."""
        if neighbor in self.neighbors and len(self.neighbors[neighbor]['data']) > 0:
            self.neighbors[neighbor]['notified'] = False
            return self.neighbors[neighbor]['data'].pop(0), len(self.neighbors[neighbor]['data'])
        else:
            return self.data, 0
    
    def addNeighbor(self, neighbor):
        """Add a new neighbor."""
        self.neighbors[neighbor] = self.neighbors.get(neighbor,
                                                      {'notified': False,
                                                       'data': []})

    def deleteNeighbor(self, neighbor):
        """Remove an old neighbor."""
        if neighbor in self.neighbors:
            del self.neighbors[neighbor]

    def destroyNeighbors(self):
        """Notify all neighbors that the cell has been destroyed."""
        for neighbor in self.neighbors.keys():
            # Need to forward the destruction along to the remote cells.
            try:
                url = urlparse(neighbor)
                if url.scheme == 'http':
                    h = httplib.HTTPConnection(url.netloc)
                elif url.scheme == 'https':
                    h = httplib.HTTPSConnection(url.netloc, key_file='./server.pem', cert_file='./server.pem')
                # TODO: If cells are remote cells?
                h.request('DELETE', url.path,
                          header={'Referer': self.referer})
                resp = h.getresponse()
                if resp.status != httplib.ACCEPTED:
                    # TODO: Handle errors
                    pass
                h.close()
            except httplib.HTTPException:
                # TODO: Handle exceptions
                pass
    
    def destroy(self):
        """Destroy the cell."""
        self.DestroySignal()
        self.destroyNeighbors()
        self.remove_from_connection()

class RemoteCell(Cell):
    """A cell referring to a remote cell."""
    
    def __init__(self, conn, name, object_path, remote_path):
        """Initialize a remote cell."""
        Cell.__init__(self, conn, name, object_path)
        
        url = urlparse(remote_path)
        if UseSSL:
            self.referer = "https://%s:%d/RemoteCells/%s%s" % (
                HostName, Port, url.netloc, url.path)
        else:
            self.referer = "http://%s:%d/RemoteCells/%s%s" % (
                HostName, Port, url.netloc, url.path)
        
        # Get the data from the remote server.
        self.remote_path = remote_path
        # TODO: Security (MitM)? Might be handled by HTTPS
        try:
            url = urlparse(self.remote_path)
            if url.scheme == 'http':
                h = httplib.HTTPConnection(url.netloc)
            elif url.scheme == 'https':
                h = httplib.HTTPSConnection(url.netloc, key_file='./server.pem', cert_file='./server.pem')
            h.request('GET', url.path,
                      headers={'Referer': self.referer})
            resp = h.getresponse()
            if resp.status == httplib.NOT_FOUND:
                self.data = 'NULL'
            elif resp.status != httplib.OK:
                # TODO: Handle errors
                pass
            else:
                # This changes based on the input.
                self.data = str(resp.read())
            h.close()
        except httplib.HTTPException:
            # TODO: Handle errors
            pass
        
        # TODO: Spawn refresh timer to determine if we need to manage
        # refreshes (in case we're behind a NAT).  Note that the way
        # we manage this is by first assuming that the remote cell
        # will contact us again NO LATER than 3 sigmas from the
        # average refresh rate or 5 minutes (whichever is greater).  If
        # we hear nothing, we need to poll for the data ourselves and
        # update the average refresh rate timer (it might just be an
        # outlier that is extra long, say, if a value has stabilized).
    
    @dbus.service.signal('edu.mit.csail.dig.DPropMan.Cell')
    def ChangeSignal(self, message):
        pass
    
    @dbus.service.signal('edu.mit.csail.dig.DPropMan.Cell')
    def DestroySignal(self):
        """Signals the destruction of the cell."""
        pass
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan.Cell',
                         in_signature='s')
    def changeCell(self, data):
        """Change the RemoteCell locally."""
        if self.data != str(data):
            self.data = str(data)
            
            # Need to forward the change along to the remote cell.
            try:
                url = urlparse(self.remote_path)
                if url.scheme == 'http':
                    h = httplib.HTTPConnection(url.netloc)
                elif url.scheme == 'https':
                    h = httplib.HTTPSConnection(url.netloc, key_file='./server.pem', cert_file='./server.pem')
                h.request('POST', url.path,
                          urllib.urlencode({'hash': makeETag(self.data)}),
                          {'Referer': self.referer,
                           'Content-Type': 'application/x-www-form-urlencoded'})
                resp = h.getresponse()
                if resp.status != httplib.ACCEPTED:
                    # TODO: Handle errors
                    pass
                h.close()
            except httplib.HTTPException:
                # TODO: Handle errors
                pass
            
            self.ChangeSignal(data)
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan.Cell',
                         out_signature='s')
    def data(self):
        """Return the cell's data."""
        return self.data

    def destroy(self):
        """Destroy the cell locally."""
        self.DestroySignal()

        # Notify the remote cell that we don't want data anymore.
        try:
            url = urlparse(self.remote_path)
            if url.scheme == 'http':
                h = httplib.HTTPConnection(url.netloc)
            elif url.scheme == 'https':
                h = httplib.HTTPSConnection(url.netloc, key_file='./server.pem', cert_file='./server.pem')
            h.request('DELETE', url.path,
                      headers={'Referer': self.referer})
            resp = h.getresponse()
            if resp.status != httplib.ACCEPTED:
                # TODO: Handle errors
                pass
            h.close()
        except httplib.HTTPException:
            # TODO: Handle errors
            pass
        
        self.remove_from_connection()

    # TODO: Diff-tracking?

class DPropServCell(twisted.web.resource.Resource):
    maxMem = 100 * 1024
    maxFields = 1024
    maxSize = 10 * 1024 * 1024
    
    def __init__(self, dpropman, path):
        twisted.web.resource.Resource.__init__(self)
        self.dpropman = dpropman
        self.path = path
    
    def getChild(self, path, request):
        return self
    
    def render_GET(self, request):
        # Register interest to notify the next time this cell changes.
        if UseSSL:
            cert = request.channel.transport.getPeerCertificate()
        data = self.dpropman.gotRemoteCellRequest(
            self.path,
            request.getHeader('Referer'))
        if isinstance(data, twisted.web.error.NoResource):
            return data
        request.setResponseCode(httplib.OK)
        request.setETag(makeETag(data))
        return data
    
    def render_POST(self, request):
        if request.args['hash'][0] != makeETag(self.dpropman.regNames[self.path].data):
            self.dpropman.gotRemoteCellChange(
                self.path,
                request.getHeader('Referer'))
            request.setResponseCode(httplib.ACCEPTED)
            request.setETag(makeETag(self.dpropman.regNames[self.path].data))
            return self.dpropman.regNames[self.path].data
        else:
            request.setResponseCode(httplib.NOT_MODIFIED)
            return ""
    
    def render_DELETE(self, request):
        # You can't delete the resource, but you CAN delete your interest.
        self.dpropman.gotRemoteCellDestroy(self.path,
                                           request.headers.get('referer',
                                                               None))
        request.setResponseCode(httplib.ACCEPTED)
        request.setETag(makeETag(self.dpropman.regNames[self.path].data))
        return self.dprop.regNames[self.path].data

class DPropServCells(twisted.web.resource.Resource):
    def __init__(self, dpropman):
        twisted.web.resource.Resource.__init__(self)
        self.dpropman = dpropman
    
    def getChild(self, path, request):
        if path in self.dpropman.regNames and not isinstance(self.dpropman.regNames[path], RemoteCell):
            return DPropServCell(self.dpropman, path)
        elif path == '':
            return self
        else:
            self.dpropman.gotRemoteCellRequest(
                path,
                request.getHeader('Referer'))
            return twisted.web.error.NoResource("No such child resource.")
    
    def render_GET(self, request):
        request.setResponseCode(httplib.OK)
        request.setHeader('content-type', 'text/html')
        return """<html>
<head><title>DProp Local Cells</title></head>
<body>
<h1>DProp Local Cells</h1>
<p>Hi! Local Cells live under me!</p>
</body>
</html>"""

class DPropServRemoteCells(twisted.web.resource.Resource):
    addSlash = True

    def __init__(self, dpropman):
        twisted.web.resource.Resource.__init__(self)
        self.dpropman = dpropman
    
    def getChild(self, path, request):
        path = '/'.join([path] + request.postpath)
        if canonicalObjectPath(path) in self.dpropman.regNames and isinstance(self.dpropman.regNames[canonicalObjectPath(path)], RemoteCell):
            return DPropServCell(self.dpropman, canonicalObjectPath(path))
        elif path == '':
            return self
        else:
            self.dpropman.gotRemoteCellRequest(
                path,
                request.getHeader('Referer'))
            return twisted.web.error.NoResource("No such child resource.")
    
    def render_GET(self, request):
        request.setResponseCode(httplib.OK)
        request.setHeader('content-type', 'text/html')
        return """<html>
<head><title>DProp Remote Cells</title></head>
<body>
<h1>DProp Remote Cells</h1>
<p>Hi! Remote Cells live under me!</p>
</body>
</html>"""

class DPropServToplevel(twisted.web.resource.Resource):
    """Top-level of the HTTP server (no cells)"""
    
    def __init__(self, dpropman):
        twisted.web.resource.Resource.__init__(self)
        self.cells = DPropServCells(dpropman)
        self.remoteCells = DPropServRemoteCells(dpropman)
        self.putChild('Cells', self.cells)
        self.putChild('RemoteCells', self.remoteCells)
        self.putChild('', self)
    
    def render_GET(self, request):
        request.setResponseCode(httplib.OK)
        request.setHeader('content-type', 'text/html')
        return """<html>
<head><title>DProp Server</title></head>
<body>
<h1>DProp Server</h1>
<p>Hi! I'm a DProp server set up on %s!</p>
</body>
</html>""" % (HostName)

ObjectPathRegexp = re.compile('[A-Za-z0-9]')

def canonicalObjectPath(object_path):
    return '/'.join(map(lambda(seg): canonicalObjectPathSegment(seg), object_path.split('/')))

def canonicalObjectPathSegment(object_path_segment):
    canonical = ''
    for char in object_path_segment:
        if ObjectPathRegexp.match(char):
            canonical += char
        else:
            canonical += '_' + str(ord(char))
    return canonical

class DPropMan(dbus.service.Object):
    def __init__(self, conn, object_path='/edu/mit/csail/dig/DPropMan'):
        self.regNames = {}
        self.interestedCells = {}
        dbus.service.Object.__init__(self, conn, object_path)
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan',
                         in_signature='s', out_signature='s')
    def registerCell(self, reqName):
        # TODO: Disallow slashes in the name.
        reqName = canonicalObjectPathSegment(reqName)
        if reqName not in self.regNames:
            self.regNames[reqName] = Cell(bus,
                                          reqName,
                                          "/edu/mit/csail/dig/DPropMan/Cells/%s" %
                                          (reqName))
            if reqName in self.interestedCells:
                for cell in self.interestedCells[reqName]:
                    self.regNames[reqName].addNeighbor(cell)
                del self.interestedCells[reqName]
        return reqName
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan',
                         in_signature='s', out_signature='s')
    def remoteCell(self, path):
        # Are we already connected to the remote cell?  If so, return
        # the local name.
        reqName = urlparse(path)
        if reqName.scheme != 'http':
            # TODO: Handle bad schemes.
            pass
        reqName = reqName.netloc + reqName.path
        reqName = canonicalObjectPath(reqName)
        if reqName not in self.regNames:
            self.regNames[reqName] = RemoteCell(
                bus,
                reqName,
                "/edu/mit/csail/dig/DPropMan/RemoteCells/%s" % (reqName),
                path)
        return reqName
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan',
                         in_signature='s')
    def unregisterCell(self, name):
        # TODO: If we're deleting a cell a remote cell is connected
        # to, notify that cell.
        self.regNames[name].destroy()
        self.regNames[name] = None
    
    def fetchRemoteCell(self, name, remotePath):
        url = urlparse(remotePath)
        if UseSSL:
            referer = "https://%s:%d/RemoteCells/%s%s" % (
                HostName, Port, url.netloc, url.path)
        else:
            referer = "http://%s:%d/RemoteCells/%s%s" % (
                HostName, Port, url.netloc, url.path)
        
        try:
            if url.scheme == 'http':
                h = httplib.HTTPConnection(url.netloc)
            elif url.scheme == 'https':
                h = httplib.HTTPSConnection(url.netloc, key_file='./server.pem', cert_file='./server.pem')
            h.request('GET', url.path,
                      headers={'Referer': referer})
            resp = h.getresponse()
            if resp.status != httplib.OK:
                # TODO: Handle errors
                pass
            # This changes based on the input.
            self.regNames[name].remoteChangeCell(resp.read())
            h.close()
        except httplib.HTTPException:
            # TODO: Handle errors
            pass
        return False
    
    def gotRemoteCellChange(self, name, remotePath):
        # Request new data for the local cell when we can.  Register
        # the thunk now.
        gobject.idle_add(lambda: self.fetchRemoteCell(name, remotePath))
        
    def gotRemoteCellDestroy(self, name, remotePath):
        # No longer interested in this cell's changes.
        self.regNames[name].deleteNeighbor(remotePath)
    
    def gotRemoteCellRequest(self, name, remotePath):
        # Save the interest from the remote cell (TODO: and make sure we
        # send a notification at the average refresh rate, so that the
        # other end can (hopefully) know that it's not behind a NAT.)
        if remotePath is not None and name in self.regNames:
            self.regNames[name].addNeighbor(remotePath)
            data, count = self.regNames[name].remoteData(remotePath)
            if count > 0:
                gobject.idle_add(lambda: self.regNames[name].notifyNeighbors())
            return data
        elif remotePath is not None:
            self.interestedCells[name] = self.interestedCells.get(name, [])
            self.interestedCells[name].append(remotePath)
            return None
        elif name in self.regNames:
            return self.regNames[name].data

class ServerContextFactory:
    def getContext(self):
        ctx = OpenSSL.SSL.Context(OpenSSL.SSL.SSLv23_METHOD)
        ctx.use_certificate_file('server.pem')
        ctx.use_privatekey_file('server.pem')
        ctx.set_verify(OpenSSL.SSL.VERIFY_PEER, self.verify)
        return ctx
    
    def verify(self, conn, cert, errnum, errdepth, retcode):
        return True

if __name__ == '__main__':
    dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)
    
    bus = dbus.SessionBus()
    name = dbus.service.BusName('edu.mit.csail.dig.DPropMan', bus)
    dpropman = DPropMan(bus, '/edu/mit/csail/dig/DPropMan')
    
    site = twisted.web.server.Site(DPropServToplevel(dpropman))
    if UseSSL:
        twisted.internet.reactor.listenSSL(Port, site, ServerContextFactory())
    else:
        twisted.internet.reactor.listenTCP(Port, site)
    
    mainloop = gobject.MainLoop()
    mainloop.run()
