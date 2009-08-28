#!/usr/bin/env python

import sys
import socket
import httplib
import re
import urllib
from optparse import OptionParser
from urlparse import urlparse

import gobject
import dbus, dbus.service
from dbus.mainloop.glib import DBusGMainLoop
import twisted.internet.glib2reactor
if __name__ == '__main__':
    twisted.internet.glib2reactor.install()
from twisted.web.error import NoResource
from twisted.web.resource import Resource
from twisted.web.server import Site as TwistedSite
from twisted.internet.reactor import listenTCP, listenSSL
import OpenSSL.SSL
import netifaces

from dpropjson import Undefined

def ipAddresses():
    addrs = []
    for iface in netifaces.interfaces():
        ifaddrs = netifaces.ifaddresses(iface)
        for type in ifaddrs:
            for item in ifaddrs[type]:
                if 'addr' in item:
                    addrs.append(item['addr'])
    return addrs

def makeETag(data):
    """Generate an ETag."""
    if hash(data) < 0:
        return "%08X" % (hash(data) + 2 * (sys.maxint + 1))
    else:
        return "%08X" % (hash(data))

def formatCertName(cert):
    """Generate the string for the given X509Name."""
    return "/C=%s/ST=%s/L=%s/O=%s/OU=%s/CN=%s/emailAddress=%s" % (cert.C, cert.ST, cert.L, cert.O, cert.OU, cert.CN, cert.emailAddress)

class DPropManCell(Resource):
    """HTTP resource representing a single cell."""
    
    maxMem = 100 * 1024
    maxFields = 1024
    maxSize = 10 * 1024 * 1024
    
    def __init__(self, dpropman, path):
        Resource.__init__(self)
        self.dpropman = dpropman
        self.path = path
    
    def getChild(self, path, request):
        return self
    
    def render_GET(self, request):
        # Register interest to notify the next time this cell changes.
        if self.dpropman.useSSL:
            cert = request.channel.transport.getPeerCertificate()
        if self.path in self.dpropman.remoteCells:
            data = self.dpropman.remoteCells[self.path].remoteData(request.getHeader('Referer'))
            if data[1] > 0:
                self.dpropman.remoteCells[self.path].notifyRemote()
            return str(data[0])
        data = self.dpropman.remoteFetchCell(
            self.path,
            request.getHeader('Referer'))
        if isinstance(data, NoResource):
            return data
        if self.dpropman.cells[self.path].accessForCert(cert) == 'r':
            request.setHeader('X-Access', 'read')
        elif self.dpropman.cells[self.path].accessForCert(cert) == 'w':
            request.setHeader('X-Access', 'write')
        request.setResponseCode(httplib.OK)
        request.setETag(makeETag(data))
        return data
    
    def render_POST(self, request):
        if self.path in self.dpropman.cells and request.args['hash'][0] != makeETag(self.dpropman.cells[self.path].data):
            self.dpropman.addFetchRemoteThunk(
                self.path,
                request.getHeader('Referer'))
            request.setResponseCode(httplib.ACCEPTED)
            request.setETag(makeETag(self.dpropman.cells[self.path].data))
            return self.dpropman.cells[self.path].data
        elif self.path in self.dpropman.remoteCells and request.args['hash'][0] != makeETag(self.dpropman.remoteCells[self.path].data):
            self.dpropman.addFetchRemoteThunk(
                self.path,
                request.getHeader('Referer'))
            request.setResponseCode(httplib.ACCEPTED)
            request.setETag(makeETag(self.dpropman.remoteCells[self.path].data))
            return ""
        else:
            request.setResponseCode(httplib.NOT_MODIFIED)
            return ""
    
    def render_DELETE(self, request):
        # You can't delete the resource, but you CAN delete your interest.
        self.dpropman.gotRemoteCellDestroy(self.path,
                                           request.headers.get('referer',
                                                               None))
        request.setResponseCode(httplib.ACCEPTED)
        if self.path in self.dpropman.cells:
            request.setETag(makeETag(self.dpropman.cells[self.path].data))
            return self.dpropman.cells[self.path].data
        elif self.path in self.dpropman.remoteCells:
            request.setETag(makeETag(self.dpropman.remoteCells[self.path].data))
            return ""

class DPropManCells(Resource):
    """Top-level of cell access."""

    def __init__(self, dpropman):
        Resource.__init__(self)
        self.dpropman = dpropman
    
    def getChild(self, path, request):
        path = '/' + '/'.join([path] + request.postpath)
        if path in self.dpropman.cells or path in self.dpropman.remoteCells:
            return DPropManCell(self.dpropman, path)
        elif path == '':
            return self
        else:
            self.dpropman.remoteFetchCell(
                path,
                request.getHeader('Referer'))
            return twisted.web.error.NoResource("No such child resource.")
    
    def render_GET(self, request):
        request.setResponseCode(httplib.OK)
        request.setHeader('content-type', 'text/html')
        return """<html>
<head><title>DProp Cells</title></head>
<body>
<h1>DProp Cells</h1>
<p>Hi! Cells live under me!</p>
</body>
</html>"""

class DPropManTopLevel(Resource):
    """Top-level of the HTTP server (no cells)"""
    
    def __init__(self, dpropman):
        Resource.__init__(self)
        self.dpropman = dpropman
        self.cells = DPropManCells(dpropman)
        self.putChild('Cells', self.cells)
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
</html>""" % (self.dpropman.hostname)

class InvalidURLException(dbus.DBusException):
    """An invalid URL has been specified."""
    _dbus_error_name = 'edu.mit.csail.dig.DPropMan.InvalidURLException'

class NonExistantCellPathException(dbus.DBusException):
    """No cell with the given path exists."""
    _dbus_error_name = 'edu.mit.csail.dig.DPropMan.InvalidURLException'

class BadAccessTypeException(dbus.DBusException):
    """The access type provided was not recognized."""
    _dbus_error_name = 'edu.mit.csail.dig.DPropMan.BadAccessTypeException'

class Cell(dbus.service.Object):
    """A local cell in a propagator network."""
    
    def __init__(self, conn, name, object_path, referer, cert, key):
        """Initialize the Cell."""
        self.object_path = object_path
        self.referer = referer
        self.name = name
        self.data = str(Undefined())
        self.neighbors = {}
        self.neighborCount = 0
        self.cert = cert
        self.key = key
        self.readAccess = set()
        self.writeAccess = set()
        dbus.service.Object.__init__(self, conn, object_path)
    
    @dbus.service.signal('edu.mit.csail.dig.DPropMan.Cell')
    def MergeContentsSignal(self, message):
        """[DBUS SIGNAL] Signals a requested merge in the cell's content."""
        pass
    
    @dbus.service.signal('edu.mit.csail.dig.DPropMan.Cell')
    def NewContentsSignal(self, message):
        """[DBUS SIGNAL] Signals updated cell content."""
        pass
    
    @dbus.service.signal('edu.mit.csail.dig.DPropMan.Cell')
    def DestroySignal(self):
        """[DBUS SIGNAL] Signals the destruction of the cell."""
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
                        h = httplib.HTTPSConnection(url.netloc,
                                                    key_file=self.key,
                                                    cert_file=self.cert)
                    # TODO: Is POST satisfactory?
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
                         in_signature='s', out_signature='')
    def changeCell(self, data):
        """[DBUS METHOD] Locally change the cell's contents to data."""
        # TODO: Track update rate for remote cells.
        if self.data != str(data):
            for neighbor in self.neighbors.keys():
                self.neighbors[neighbor]['data'].append(str(data))
            self.data = str(data)
            self.NewContentsSignal(str(data))
            self.notifyNeighbors()
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan.Cell',
                         in_signature='s', out_signature='')
    def mergeCell(self, data):
        """[DBUS METHOD] Locally attempt to merge the cell's contents with
        data."""
        # TODO: Track update rate for remote cells.
        self.MergeContentsSignal(str(data))
    
    def remoteMergeCell(self, remote_path):
        """Fetch a remote change to the cell and merge it."""
        url = urlparse(remote_path)
        
        # Get the data from the remote server.
        # TODO: Security (MitM)? Might be handled by HTTPS
        try:
            url = urlparse(remote_path)
            if url.scheme == 'http':
                h = httplib.HTTPConnection(url.netloc)
            elif url.scheme == 'https':
                h = httplib.HTTPSConnection(url.netloc,
                                            key_file=self.key,
                                            cert_file=self.cert)
            h.request('GET', url.path,
                      headers={'Referer': self.referer})
            resp = h.getresponse()
            if resp.status == httplib.NOT_FOUND:
                data = str(Undefined())
            elif resp.status != httplib.OK:
                # TODO: Handle errors
                pass
            else:
                # This changes based on the input.
                data = str(resp.read())
            h.close()
        except httplib.HTTPException:
            # TODO: Handle errors
            pass
        
        # TODO: Track update rate for remote cells.
        self.MergeContentsSignal(data)
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan.Cell',
                         in_signature='', out_signature='s')
    def data(self):
        """[DBUS METHOD] Return the cell's data."""
        return self.data
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan.Cell',
                         in_signature='ass', out_signature='')
    def grantAccessTo(self, certs, accessType):
        """[DBUS METHOD] Grant remote access of type accessType to a cell to
        people bearing any of the provided certificates."""
        if accessType == 'r':
            # Read Access
            for cert in certs:
                self.readAccess.add(cert)
        elif accessType == 'w':
            # Read/Write Access
            for cert in certs:
                self.writeAccess.add(cert)
            for cert in certs:
                if cert in self.readAccess:
                    self.readAccess.remove(cert)
        else:
            raise BadAccessTypeException(
                "Don't understand access type '%s'" % (accessType))
    
    def remoteData(self, neighbor):
        """Return the next data a particular neighbor should see and the
        number of changes still remaining.."""
        if neighbor in self.neighbors and len(self.neighbors[neighbor]['data']) > 0:
            self.neighbors[neighbor]['notified'] = False
            return self.neighbors[neighbor]['data'].pop(0), len(self.neighbors[neighbor]['data'])
        else:
            return self.data, 0
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan.Cell',
                         in_signature='', out_signature='s')
    def accessType(self):
        """[DBUS METHOD] Return the type of access granted (always 'w' for
        local cells)."""
        return 'w'
    
    def accessForCert(self, cert):
        """Return the type of access granted to the certificate provided."""
        cert = formatCertName(cert.get_subject())
        if cert in self.readAccess:
            return 'r'
        elif cert in self.writeAccess:
            return 'w'
        else:
            return ''
    
    def addNeighbor(self, neighbor=None):
        """Add a new neighbor."""
        if neighbor is not None:
            self.neighbors[neighbor] = self.neighbors.get(neighbor,
                                                          {'notified': False,
                                                           'data': []})
        self.neighborCount += 1

    def removeNeighbor(self, neighbor=None):
        """Remove an old neighbor."""
        if neighbor is not None and neighbor in self.neighbors:
            del self.neighbors[neighbor]
        self.neighborCount -= 1

    def destroyNeighbors(self):
        """Notify all neighbors that the cell has been destroyed."""
        for neighbor in self.neighbors.keys():
            # Need to forward the destruction along to the remote cells.
            try:
                url = urlparse(neighbor)
                if url.scheme == 'http':
                    h = httplib.HTTPConnection(url.netloc)
                elif url.scheme == 'https':
                    h = httplib.HTTPSConnection(url.netloc,
                                                key_file=self.key,
                                                cert_file=self.cert)
                # TODO: If cells are remote cells?
                # TODO: DELETE is no longer satisfactory.
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
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan.Cell',
                         in_signature='', out_signature='b')
    def exists(self):
        """[DBUS METHOD] A hack to test for a claim on a cell."""
        return True
    
    def destroy(self):
        """Destroy the cell."""
        self.DestroySignal()
        self.destroyNeighbors()
        self.remove_from_connection()

class RemoteCell(Cell):
    """A cell referring to a remote cell."""
    
    def __init__(self, conn, name, object_path, remote_path, referer,
                 cert, key):
        """Initialize a remote cell."""
        Cell.__init__(self, conn, name, object_path, referer, cert, key)
        
        url = urlparse(remote_path)
        
        # Get the data from the remote server.
        self.remote_path = remote_path
        # TODO: Security (MitM)? Might be handled by HTTPS
        try:
            url = urlparse(self.remote_path)
            if url.scheme == 'http':
                h = httplib.HTTPConnection(url.netloc)
            elif url.scheme == 'https':
                h = httplib.HTTPSConnection(url.netloc,
                                            key_file=self.key,
                                            cert_file=self.cert)
            h.request('GET', url.path,
                      headers={'Referer': self.referer})
            resp = h.getresponse()
            self.access = ''
            if resp.status == httplib.NOT_FOUND:
                self.data = str(Undefined())
            elif resp.status != httplib.OK:
                # TODO: Handle errors
                pass
            else:
                # This changes based on the input.
                self.data = str(resp.read())
                self.access = resp.getHeader('X-Access', '')
            h.close()
        except httplib.HTTPException:
            # TODO: Handle errors
            pass
        
        self.merge_data = []
        self.notified_remote = False
        
        # TODO: Spawn refresh timer to determine if we need to manage
        # refreshes (in case we're behind a NAT).  Note that the way
        # we manage this is by first assuming that the remote cell
        # will contact us again NO LATER than 3 sigmas from the
        # average refresh rate or 5 minutes (whichever is greater).  If
        # we hear nothing, we need to poll for the data ourselves and
        # update the average refresh rate timer (it might just be an
        # outlier that is extra long, say, if a value has stabilized).
    
    @dbus.service.signal('edu.mit.csail.dig.DPropMan.Cell')
    def MergeContentsSignal(self, message):
        """[DBUS SIGNAL] Signals a requested merge in the cell's content."""
        pass
    
    @dbus.service.signal('edu.mit.csail.dig.DPropMan.Cell')
    def NewContentsSignal(self, message):
        """[DBUS SIGNAL] Signals updated cell content."""
        pass
    
    @dbus.service.signal('edu.mit.csail.dig.DPropMan.Cell')
    def DestroySignal(self):
        """[DBUS SIGNAL] Signals the destruction of the cell."""
        pass
    
    def notifyRemote(self):
        """Notify the remote cell of any data to be merged."""
        # TODO: Why not?
        # Need to forward the change along to the remote cells.
        if not self.notified_remote and len(self.merge_data) > 0:
            try:
                url = urlparse(self.remote_path)
                if url.scheme == 'http':
                    h = httplib.HTTPConnection(url.netloc)
                elif url.scheme == 'https':
                    h = httplib.HTTPSConnection(url.netloc,
                                                key_file=self.key,
                                                cert_file=self.cert)
                # TODO: Is POST satisfactory?
                h.request('POST', url.path,
                          urllib.urlencode({'hash': makeETag(self.merge_data[0])}),
                          {'Referer': self.referer,
                           'Content-Type': 'application/x-www-form-urlencoded'})
                # TODO: If cells are remote cells?
                resp = h.getresponse()
                if resp.status == httplib.NOT_MODIFIED:
                    self.merge_data.pop(0)
                elif resp.status != httplib.ACCEPTED:
                    # TODO: Handle errors
                    pass
                h.close()
                if resp.status == httplib.ACCEPTED:
                    self.notified_remote = True
            except httplib.HTTPException:
                # TODO: Handle exceptions
                pass
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan.Cell',
                         in_signature='s', out_signature='')
    def mergeCell(self, data):
        """[DBUS METHOD] Locally attempt to merge the cell's contents with
        data.  We do this by appending the data to the list of data to
        merge remotely, and then notify the remote cell if we haven't."""
        if self.data != str(data):
            if len(self.merge_data) == 0 or self.merge_data[-1] != str(data):
                self.merge_data.append(data)
                if not self.notified_remote:
                    self.notifyRemote()
    
    def remoteChangeCell(self, remote_path):
        """Fetch a remote change to the cell and notify local clients."""
        if remote_path != self.remote_path:
            return # Ignore attempts to change from other locations.
        
        # Get the data from the remote server.
        # TODO: Security (MitM)? Might be handled by HTTPS
        try:
            url = urlparse(self.remote_path)
            if url.scheme == 'http':
                h = httplib.HTTPConnection(url.netloc)
            elif url.scheme == 'https':
                h = httplib.HTTPSConnection(url.netloc,
                                            key_file=self.key,
                                            cert_file=self.cert)
            h.request('GET', url.path,
                      headers={'Referer': self.referer})
            resp = h.getresponse()
            if resp.status == httplib.NOT_FOUND:
                self.data = str(Undefined())
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
        
        self.NewContentsSignal(self.data)
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan.Cell',
                         in_signature='', out_signature='s')
    def data(self):
        """[DBUS METHOD] Return the cell's data."""
        return self.data

    def remoteData(self, neighbor):
        """Return the next data a particular neighbor should see and the
        number of changes still remaining.."""
        if neighbor == self.remote_path and len(self.merge_data) > 0:
            self.notified_remote = False
            return self.merge_data.pop(0), len(self.merge_data)
        elif neighbor == self.remote_path:
            return self.data, 0
        else:
            return "", 0
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan.Cell',
                         in_signature='', out_signature='s')
    def accessType(self):
        """[DBUS METHOD] Return the type of access granted to the certificate
        provided."""
        return self.access
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan.Cell',
                         in_signature='', out_signature='b')
    def exists(self):
        """[DBUS METHOD] A hack to test for a claim on a cell."""
        return True
    
    def destroy(self):
        """Destroy the cell locally."""
        self.DestroySignal()

        # Notify the remote cell that we don't want data anymore.
        try:
            url = urlparse(self.remote_path)
            if url.scheme == 'http':
                h = httplib.HTTPConnection(url.netloc)
            elif url.scheme == 'https':
                h = httplib.HTTPSConnection(url.netloc,
                                            key_file=self.key,
                                            cert_file=self.cert)
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

ObjectPathRegexp = re.compile('[A-Za-z0-9/]')

class DPropMan(dbus.service.Object):
    """The DProp manager.  Contains functions accessible by DBus."""
    
    def __init__(self, conn, object_path, port, hostname,
                 useSSL=False, cert=None, key=None):
        """Initializes the DPropMan object."""
        dbus.service.Object.__init__(self, conn, object_path)
        self.cells = {}
        self.remoteCells = {}
        self.requestedCells = {}
        self.conn = conn
        self.port = port
        self.hostname = hostname
        self.useSSL = useSSL
        self.cert = cert
        self.key = key
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan',
                         in_signature='s', out_signature='s')
    def escapePath(self, cellPath):
        """[DBUS METHOD] Escape a cell path."""
        canonical = ''
        for char in cellPath:
            if ObjectPathRegexp.match(char):
                canonical += char
            else:
                canonical += "_%02X" % (ord(char))
        return canonical
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan',
                         in_signature='', out_signature='b')
    def useSSL(self):
        """[DBUS METHOD] Returns True if this DPropMan daemon is using SSL."""
        return self.useSSL
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan',
                         in_signature='', out_signature='i')
    def port(self):
        """[DBUS METHOD] Returns the port this DPropMan instance is running
        on."""
        return self.port
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan',
                         in_signature='s', out_signature='')
    def registerCell(self, cellPath):
        """[DBUS METHOD] Registers a new cell under the given cell path if it
        does not yet exist.  Otherwise, registers interest in the cell."""
        if cellPath not in self.cells:
            if self.useSSL:
                referer = "https://%s:%d/Cells%s" % (self.hostname,
                                                     self.port,
                                                     cellPath)
            else:
                referer = "http://%s:%d/Cells%s" % (self.hostname,
                                                    self.port,
                                                    cellPath)
            self.cells[cellPath] = Cell(self.conn,
                                        cellPath,
                                        "/Cell%s" %
                                        (cellPath),
                                        referer,
                                        self.cert,
                                        self.key)
            if cellPath in self.requestedCells:
                # Notify anyone waiting to hear from a non-existent
                # cell that it now exists.
                for client in self.requestedCells[cellPath]:
                    # And automatically add it as a neighbor to the cell.
                    self.cells[cellPath].addNeighbor(client)
                # And delete it from the requestedCells after we're done.
                del self.requestedCells[cellPath]
        self.cells[cellPath].addNeighbor()
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan',
                         in_signature='s', out_signature='s')
    def registerRemoteCell(self, url):
        """[DBUS METHOD] Registers interest in a cell on a remote server at
        the given URL."""
        # Are we already connected to the remote cell?  If so, return
        # the local name.
        parsed_url = urlparse(url)
        if parsed_url.scheme != 'http' and parsed_url.scheme != 'https':
            # Balk at non-HTTP addresses.
            raise InvalidURLException(
                'The remote cell URL %s is not a valid HTTP URL.' % (url))
        # Is the remote client us?
        cellPath = self.escapePath('/' + parsed_url.netloc + parsed_url.path)
        
        # Register the remote cell and send out a query for it.
        if cellPath not in self.remoteCells:
            if self.useSSL:
                referer = "https://%s:%d/Cells%s" % (self.hostname,
                                                     self.port,
                                                     cellPath)
            else:
                referer = "http://%s:%d/Cells%s" % (self.hostname,
                                                    self.port,
                                                    cellPath)
            if parsed_url.netloc in map(lambda x: '%s:%d' % (x, self.port),
                                        ipAddresses()):
                if parsed_url.path[6:] in self.cells:
                    self.cells[parsed_url.path[6:]].addNeighbor()
                    return '/Cell' + parsed_url.path[6:]
            else:
                self.remoteCells[cellPath] = RemoteCell(
                    self.conn,
                    cellPath,
                    "/RemoteCell%s" % (cellPath),
                    url,
                    referer,
                    self.cert,
                    self.key)
        self.remoteCells[cellPath].addNeighbor()
        return cellPath
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan',
                         in_signature='s', out_signature='')
    def unregisterCell(self, cellPath):
        """[DBUS METHOD] Unregisters interest in a cell."""
        # TODO: Notify remote cells that we don't exist any more.
        if cellPath in self.cells:
            self.cells[cellPath].removeNeighbor()
            if len(self.cells[cellPath].neighbors) == 0:
                self.cells[cellPath].destroy()
                del self.cells[cellPath]
        elif cellPath in self.remoteCells:
            self.remoteCells[cellPath].removeNeighbor()
            if len(self.remoteCells[cellPath].neighbors) == 0:
                self.remoteCells[cellPath].destroy()
                del self.remoteCells[cellPath]
        else:
            raise NonExistantCellPathException(
                'The cell with path %s does not exist.' % (cellPath))
    
    def remoteFetchCell(self, cellPath, client):
        """[HTTP METHOD] Registers interest in a cell from a remote client."""
        # Save the interest from the remote cell (TODO: and make sure we
        # send a notification at the average refresh rate, so that the
        # other end can (hopefully) know that it's not behind a NAT.)
        if client is not None and cellPath in self.cells:
            self.cells[cellPath].addNeighbor(client)
            # Get the next data the remote client should see.
            data, count = self.cells[cellPath].remoteData(client)
            if count > 0:
                gobject.idle_add(lambda: self.cells[cellPath].notifyNeighbors())
            return data
        elif client is not None:
            self.requestedCells[cellPath] = self.requestedCells.get(cellPath,
                                                                    [])
            self.requestedCells[cellPath].append(client)
            return None
        elif cellPath in self.cells:
            return self.cells[cellPath].data
    
    def addFetchRemoteThunk(self, cellPath, client):
        """Adds a thunk for fetchRemoteCell for the given cell to the runtime
        loop."""
        if cellPath in self.cells:
            gobject.idle_add(lambda: self.cells[cellPath].remoteMergeCell(client))
        elif cellPath in self.remoteCells:
            gobject.idle_add(lambda: self.remoteCells[cellPath].remoteChangeCell(client))
        
    def gotRemoteCellDestroy(self, cellPath, client):
        # No longer interested in this cell's changes.
        if cellPath in self.cells:
            self.cells[cellPath].deleteNeighbor(client)
        elif cellPath in self.remoteCells:
            self.remoteCells[cellPath].deleteNeighbor(client)
    
class ServerContextFactory:
    """Generates the server SSL context."""
    def __init__(self, cert, key):
        self.cert = cert
        self.key = key
    
    def getContext(self):
        """Sets up the OpenSSL context."""
        ctx = OpenSSL.SSL.Context(OpenSSL.SSL.SSLv23_METHOD)
        ctx.use_certificate_file(self.cert)
        ctx.use_privatekey_file(self.key)
        ctx.set_verify(OpenSSL.SSL.VERIFY_PEER, self.verify)
        return ctx
    
    def verify(self, conn, cert, errnum, errdepth, retcode):
        """Verify the client certificate.  We always return True, as we use
        FOAF+SSL."""
        return True

def main():
    """Main program logic."""
    
    # Before anything, parse command-line options.
    parser = OptionParser()
    parser.add_option('-p', '--port', dest='port',
                      help='set DProp port number [default: %default]',
                      default='37767')
    parser.add_option('-c', '--cert', dest='cert',
                      help='set certificate file [default: %default]',
                      default='./server.pem')
    parser.add_option('-k', '--key', dest='key',
                      help='set private key file [default: %default]',
                      default='./server.pem')
    parser.add_option('-s', '--no-ssl', dest='ssl', action="store_false",
                      help='do not use SSL',
                      default=True)
    (options, args) = parser.parse_args()
    port = int(options.port)
    hostname = socket.getfqdn()
    useSSL = options.ssl
    
    # First, set up the DBus connection.
    DBusGMainLoop(set_as_default=True)
    bus = dbus.SystemBus()
    name = dbus.service.BusName('edu.mit.csail.dig.DPropMan', bus)
    dpropman = DPropMan(bus, '/DPropMan', port, hostname,
                        useSSL, options.cert, options.key)
    
    # Second, set up the server for remote connections.
    site = TwistedSite(DPropManTopLevel(dpropman))
    if useSSL:
        listenSSL(port, site, ServerContextFactory(options.cert, options.key))
    else:
        listenTCP(port, site)
    
    # Finally, run the glib loop
    mainloop = gobject.MainLoop()
    mainloop.run()

if __name__ == '__main__':
    main()
