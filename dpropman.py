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
from twisted.web.http import parse_qs
from twisted.internet.reactor import listenTCP, listenSSL
import OpenSSL.SSL
import netifaces

from dpropjson import Nothing
import dpropjson

SYNC_INTERVAL = 30000
DEBUG = True

def pdebug(s):
    if DEBUG:
        print s

def ipAddresses():
    addrs = []
    for iface in netifaces.interfaces():
        try:
            ifaddrs = netifaces.ifaddresses(iface)
        except ValueError:
            continue
        
        for type in ifaddrs:
            for item in ifaddrs[type]:
                if 'addr' in item:
                    addrs.append(item['addr'])
    return addrs

def makeEtag(data):
    """Generate an ETag."""
    if hash(data) < 0:
        return "%08X" % (hash(data) + 2 * (sys.maxint + 1))
    else:
        return "%08X" % (hash(data))

def formatCertName(cert):
    """Generate the string for the given X509Name."""
    return "/C=%s/ST=%s/L=%s/O=%s/OU=%s/CN=%s/emailAddress=%s" % (cert.C, cert.ST, cert.L, cert.O, cert.OU, cert.CN, cert.emailAddress)

def pathifyURL(url):
    parsed = urlparse(url)
    return parsed.netloc + '/' + parsed.path

class DPropManCellPeer(Resource):
    """HTTP collection representing a peer of a single cell."""
    
    def __init__(self, dpropman, path, peer):
        Resource.__init__(self)
        self.dpropman = dpropman
        self.path = path
        self.peer = peer

    def render_GET(self, request):
        # Just some filler if someone pulls the peer list.
        request.setResponseCode(httplib.OK)
        request.setHeader('content-type', 'text/html')
        return """<html>
<head><title>DProp Cell Peer</title></head>
<body>
<h1>DProp Cell Peer</h1>
<p>Hi! I'm a cell peer!</p>
</body>
</html>"""
    
    def render_POST(self, request):
        if request.args['_method'].upper() == 'PUT':
            return self.render_DELETE(request)
        else:
            request.setResponseCode(httplib.METHOD_NOT_ALLOWED)
            return ""
    
    def render_DELETE(self, request):
        # Extract the certificate for access control purposes.
        cert = False
        if self.dpropman.useSSL:
            cert = request.channel.transport.getPeerCertificate()
        
        # Get the cell data.
        cell = self.dpropman.cells[self.path]
        
        # Perform access control (simple read/write limits for now)
        if cell.accessForCert(cert) == 'r':
            request.setHeader('X-Access', 'read')
        elif cell.accessForCert(cert) == 'w':
            request.setHeader('X-Access', 'write')
        else:
            # Do not return the data if the certificate lacks access.
            request.setResponseCode(httplib.FORBIDDEN)
            return ""
        
        if cell.peers[self.peer] != cert:
            # Do not return the data if the certificate lacks access.
            request.setResponseCode(httplib.FORBIDDEN)
            return ""
        
        # Remove the peer from the peers collection.
        del cell.peers[request.args['peer']]
        request.setResponseCode(httplib.OK)
        return ""

class DPropManCellPeers(Resource):
    """HTTP collection representing the peers of a single cell."""
    
    def __init__(self, dpropman, path):
        Resource.__init__(self)
        self.dpropman = dpropman
        self.path = path

#    def getChild(self, path, request):
#        child = request.postpath
#        if child in self.dpropman.cells[self.path].peers:
#            return DPropManCellPeer(self.dpropman, path, child)
#        elif child == '':
#            return self
#        else:
#            return twisted.web.error.NoResource("No such resource.")
    
    def render_GET(self, request):
        pdebug("Received GET for Peers of %s" % (self.path))
        cell = self.dpropman.cells[self.path]
        
        if request.getHeader('If-None-Match') == cell.peersEtag:
            pdebug("Responding with NOT MODIFIED")
            request.setResponseCode(httplib.NOT_MODIFIED)
            return ""
        else:
            # Return the data and ETag if updated...
            pdebug("Responding with OK")
            request.setResponseCode(httplib.OK)
            request.setETag(cell.peersEtag)
            return dpropjson.dumps(cell.peers).encode('utf-8')
    
    def render_PUT(self, request):
        # Extract the certificate for access control purposes.
        pdebug("Received PUT for Peers of %s" % (self.path))
        parameters = parse_qs(request.content.read(), 1)
        
        cert = False
        if self.dpropman.useSSL:
            cert = request.channel.transport.getPeerCertificate()
        
        # Get the cell data.
        cell = self.dpropman.cells[self.path]
        
#        # Perform access control (simple read/write limits for now)
#        if cell.accessForCert(cert) == 'r':
#            request.setHeader('X-Access', 'read')
#        elif cell.accessForCert(cert) == 'w':
#            request.setHeader('X-Access', 'write')
#        else:
#            # Do not return the data if the certificate lacks access.
#            request.setResponseCode(httplib.FORBIDDEN)
#            return ""
        
        # Add the peer to the peers collection.
        
        # To make this idempotent, shouldn't this require the cert to
        # be part of the request itself?
        # Why is request.args empty???
        url = dpropjson.loads(parameters['peer'][0])['url']
        path = pathifyURL(url)
        pdebug("Adding %s as peer" % (url))
        cell.peers[url] = {'cert': False, 'url': url}
        cell.peersEtag = makeEtag(dpropjson.dumps(cell.peers))
        pdebug("New peersEtag: %s" % (cell.peersEtag))
        request.setResponseCode(httplib.OK)
        return ""

class DPropManCell(Resource):
    """HTTP resource representing a single cell."""
    
    maxMem = 100 * 1024
    maxFields = 1024
    maxSize = 10 * 1024 * 1024
    
    def __init__(self, dpropman, path):
        Resource.__init__(self)
        self.dpropman = dpropman
        self.path = path
        self.peers = DPropManCellPeers(dpropman, path)
        self.putChild('Peers', self.peers)
        self.putChild('', self)
    
    def render_GET(self, request):
        # Extract the certificate for access control purposes.
        pdebug("Received GET for %s" % (self.path))
        cert = False
        if self.dpropman.useSSL:
            cert = request.channel.transport.getPeerCertificate()
        
        # Get the cell data.
        cell = self.dpropman.cells[self.path]
        
#        # Perform access control (simple read/write limits for now)
#        if cell.accessForCert(cert) == 'r':
#            request.setHeader('X-Access', 'read')
#        elif cell.accessForCert(cert) == 'w':
#            request.setHeader('X-Access', 'write')
#        else:
#            # Do not return the data if the certificate lacks access.
#            request.setResponseCode(httplib.FORBIDDEN)
#            return ""
        
        # Check the ETag.
        if request.getHeader('If-None-Match') == cell.etag:
            pdebug("Responding with NOT MODIFIED")
            request.setResponseCode(httplib.NOT_MODIFIED)
            return ""
        else:
            # Return the data and ETag if updated...
            pdebug("Responding with OK")
            request.setResponseCode(httplib.OK)
            request.setETag(cell.etag)
            return cell.data
    
    def render_PUT(self, request):
        # Extract the certificate for access control purposes.
        pdebug("Received PUT for %s" % (self.path))
        parameters = parse_qs(request.content.read(), 1)
        
        cert = False
        if self.dpropman.useSSL:
            cert = request.channel.transport.getPeerCertificate()
        
        # Is the client allowed to update this cell?
        # For now, they can if they're in the list of peers.
        cell = self.dpropman.cells[self.path]
        if request.getHeader('Referer') not in cell.peers:
#        if cell.accessForCert(cert) != 'w':
            pdebug("PUT came from unrecognized referer! FORBIDDEN!")
            request.setResponseCode(httplib.FORBIDDEN)
            return ""
        
        # If so, forward the update to the cell so it can do the merge.
        pdebug("PUT was ACCEPTED! Forwarding Update!")
        cell.doUpdate(parameters['data'][0], False)
        
        # And let the client know that it was accepted.
        request.setResponseCode(httplib.ACCEPTED)
        return ""
    
    def render_POST(self, request):
        pdebug("Received POST for %s" % (self.path))
        if request.args['_method'].upper() == 'PUT':
            pdebug("But it's actually a PUT!")
            return self.render_PUT(request)
        else:
            pdebug("But it's not actually a PUT.")
            request.setResponseCode(httplib.METHOD_NOT_ALLOWED)
            return ""
    
class DPropManCells(Resource):
    """Top-level of cell access."""

    def __init__(self, dpropman):
        Resource.__init__(self)
        self.dpropman = dpropman
    
    def getChild(self, path, request):
#        path = '/' + '/'.join([path] + request.postpath)
        pdebug("Attempting to find cell for %s" % (path))
#        pdebug("Possible paths include %s" % (`self.dpropman.cells.keys()`))
        if path in self.dpropman.cells:
            pdebug("Found it!")
            return DPropManCell(self.dpropman, path)
        elif path == '':
            pdebug("Oh, well I wasn't looking for a cell anyway.")
            return self
        else:
            pdebug("Couldn't find cell!")
            return twisted.web.error.NoResource("No such resource.")
    
    def render_GET(self, request):
        # Just some filler if someone pulls the cell list.
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
        # Just some filler if someone pulls the main list.
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

#class NonExistantCellPathException(dbus.DBusException):
#    """No cell with the given path exists."""
#    _dbus_error_name = 'edu.mit.csail.dig.DPropMan.InvalidURLException'

#class BadAccessTypeException(dbus.DBusException):
#    """The access type provided was not recognized."""
#    _dbus_error_name = 'edu.mit.csail.dig.DPropMan.BadAccessTypeException'

class UUIDMismatchException(dbus.DBusException):
    """The UUID provided when creating the cell does not match the UUID in
    the URL being used for synchronization."""
    _dbus_error_name = 'edu.mit.csail.dig.DPropMan.UUIDMismatchException'

class Cell(dbus.service.Object):
    """A local cell in a propagator network."""
    
    def __init__(self, conn, object_path, uuid, referer, cert, key):
        """Initialize the Cell."""
        pdebug("Setting up cell %s" % (uuid))
        self.object_path = object_path
        self.uuid = uuid
        self.data = dpropjson.dumps(Nothing())
        self.etag = makeEtag(self.data)
        self.referer = referer
        self.cert = cert
        self.key = key
#        self.readAccess = set()
#        self.writeAccess = set()
        self.peers = {self.referer: {'cert': self.cert, 'url': self.referer}}
        self.peersEtag = makeEtag(dpropjson.dumps(self.peers))
        pdebug("New peersEtag: %s" % (self.peersEtag))
        dbus.service.Object.__init__(self, conn, object_path)
        
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
    
    def doUpdate(self, message, isLocal):
        """Handles sending out an update signal and then maybe updating
        peers."""
        # Push the update along if it was a local update attempt.
        pdebug("%s sending out UpdateSignal" % (self.uuid))
        self.UpdateSignal(message)
        if isLocal:
            self.updatePeers(message)
    
    @dbus.service.signal('edu.mit.csail.dig.DPropMan.Cell')
    def UpdateSignal(self, message):
        """[DBUS SIGNAL] Signals a requested update in the cell's content."""
        pass
    
#    @dbus.service.signal('edu.mit.csail.dig.DPropMan.Cell')
#    def NewContentsSignal(self, message):
#        """[DBUS SIGNAL] Signals updated cell content."""
#        pass
    
#    @dbus.service.signal('edu.mit.csail.dig.DPropMan.Cell')
#    def DestroySignal(self):
#        """[DBUS SIGNAL] Signals the destruction of the cell."""
#        pass
    
    def updatePeers(self, message):
        """Notify all peer cells of any updates."""
        pdebug("%s attempting to update peers" % (self.uuid))

        def thunkifyPeerUpdate(peer, message):
            # Make the thunk to send the update.
            def thunk():
                pdebug("%s attempting to update peer %s" % (self.uuid,
                                                            peer['url']))
                try:
                    url = urlparse(peer['url'])
                    if url.scheme == 'http':
                        h = httplib.HTTPConnection(url.netloc)
                    elif url.scheme == 'https':
                        # TODO: Is it safe to keep these keys and
                        # certs around???
                        h = httplib.HTTPSConnection(url.netloc,
                                                    key_file=self.key,
                                                    cert_file=self.cert)
                    h.request('PUT', url.path,
                              urllib.urlencode(
                            {'data': dpropjson.dumps(message)}),
                              {'Referer': self.referer,
                               'Content-Type':
                                   'application/x-www-form-urlencoded'})
                    resp = h.getresponse()
                    if resp.status != httplib.ACCEPTED:
                        # TODO: Handle errors
                        pdebug("Didn't get ACCEPTED response!!")
                    else:
                        pdebug("PeerUpdate was ACCEPTED!!")
                    h.close()
                except httplib.HTTPException, exc:
                    # TODO: Handle exceptions
                    pdebug("Caught HTTPException: %s: %s" % (type(exc).__name__,
                                                             str(exc)))
                
                return False
            
            return thunk
        
        pdebug("Setting up peerUpdate thunks...")
        for peerKey in self.peers:
            if peerKey == self.referer:
                continue
            # For each peer, add a thunk to update it to the runloop
            gobject.idle_add(thunkifyPeerUpdate(self.peers[peerKey], message))
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan.Cell',
                         in_signature='s', out_signature='')
    def changeCell(self, data):
        """[DBUS METHOD] Locally change the cell's contents to data."""
        pdebug("%s received changeCell() call" % (self.uuid))
        if self.data != str(data):
            self.data = str(data)
            self.etag = makeEtag(self.data)
#            self.NewContentsSignal(str(data))
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan.Cell',
                         in_signature='s', out_signature='')
    def updateCell(self, data):
        """[DBUS METHOD] Locally attempt to update the cell's contents with
        data."""
        pdebug("%s received updateCell() call" % (self.uuid))
        self.doUpdate(str(data), True)
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan.Cell',
                         in_signature='', out_signature='s')
    def data(self):
        """[DBUS METHOD] Return the cell's data."""
        pdebug("%s received data() call" % (self.uuid))
        return self.data
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan.Cell',
                         in_signature='s', out_signature='')
    def connectToRemote(self, url):
        """[DBUS METHOD] Connect to a remote URL and join it for
        synchronization."""
        pdebug("%s received connectToRemote() call" % (self.uuid))
        
        def thunkifyAddToPeer(peer):
            # Make the thunk to send the update.
            def thunk():
                pdebug("Attempting to add %s to peer %s" % (self.uuid,
                                                            peer['url']))
                try:
                    url = urlparse(peer['url'])
                    if url.scheme == 'http':
                        h = httplib.HTTPConnection(url.netloc)
                    elif url.scheme == 'https':
                        # TODO: Is it safe to keep these keys and
                        # certs around???
                        h = httplib.HTTPSConnection(url.netloc,
                                                    key_file=self.key,
                                                    cert_file=self.cert)
                    h.request('PUT', url.path + '/Peers',
                              urllib.urlencode(
                            {'peer': dpropjson.dumps({'url': self.referer})}),
                              {'Referer': self.referer,
                               'Content-Type':
                                   'application/x-www-form-urlencoded'})
                    resp = h.getresponse()
                    if resp.status != httplib.OK:
                        # TODO: Handle errors
                        pdebug("Didn't get OK response!!")
                    else:
                        pdebug("PeerAdd was OK!!")
                    h.close()
                except httplib.HTTPException, exc:
                    # TODO: Handle exceptions
                    pdebug("Caught HTTPException: %s: %s" % (type(exc).__name__,
                                                             str(exc)))
                
                return False
            
            return thunk
        
        def startSyncThunk():
            # A thunk to start synchronization mechanisms.
            pdebug("Attempting to start synchronization thunks")
            def thunkifyPeerSync(peer):
                
                # Make the thunk to sync with the given peer.
                def thunk():
                    pdebug("Attempting to sync %s to peer %s" % (self.uuid,
                                                                 peer['url']))
                    try:
                        url = urlparse(peer['url'])
                        if url.scheme == 'http':
                            h = httplib.HTTPConnection(url.netloc)
                        elif url.scheme == 'https':
                            # TODO: Is it safe to keep these keys and
                            # certs around???
                            h = httplib.HTTPSConnection(url.netloc,
                                                        key_file=self.key,
                                                        cert_file=self.cert)
                        pdebug("Attempting to sync data")
                        h.request('GET', url.path,
                                  headers={'Referer': self.referer,
                                           'If-None-Match': self.etag})
                        resp = h.getresponse()
                        if resp.status == httplib.OK:
                            # Got a response.  Time to merge.
                            pdebug("Got new data in sync. Merging...")
                            self.doUpdate(resp.read(), False)
                        elif resp.status == httplib.NOT_MODIFIED:
                            pdebug("Sync resulted in no new data.")
                        else:
                            # TODO: Handle errors
                            pdebug("Unexpected HTTP response!")
                        
                        # Next, sync peers.
                        pdebug("Attempting to sync peers")
                        h.request('GET', url.path + '/Peers',
                                  headers={'Referer': self.referer,
                                           'If-None-Match': self.peersEtag})
                        resp = h.getresponse()
                        if resp.status == httplib.OK:
                            # Got a response.  Time to merge.
                            pdebug("Got new data in sync. Merging...")
                            self.peers.update(dpropjson.loads(resp.read()))
                            self.peersEtag = makeEtag(dpropjson.dumps(self.peers))
                            pdebug("New etag: %s" % (self.peersEtag))
                        elif resp.status == httplib.NOT_MODIFIED:
                            pdebug("Sync resulted in no new data.")
                        else:
                            # TODO: Handle errors
                            pdebug("Unexpected HTTP response!")
                        
                        h.close()
                    except httplib.HTTPException, exc:
                        # TODO: Handle exceptions
                        pdebug("Caught HTTPException: %s: %s" % (type(exc).__name__,
                                                                 str(exc)))
                    
                    return False
                
                return thunk
            
            for peerKey in self.peers:
                if peerKey == self.referer:
                    continue
                gobject.idle_add(thunkifyPeerSync(self.peers[peerKey]))
            
            pdebug("Done adding thunks.")
            
            # TODO: Ehm, what if we delete this cell???
            return True
        
        parsed_url = urlparse(url)
        if parsed_url.scheme != 'http' and parsed_url.scheme != 'https':
            # Balk at non-HTTP addresses.
            pdebug("Uh oh!  Didn't get an HTTP URL!")
            raise InvalidURLException(
                'The remote cell URL %s is not a valid HTTP URL.' % (url))
        
        uuid = parsed_url.path.split('/')
        if uuid[-1] == '':
            uuid = self.escapePath(uuid[-2].replace('-', ''))
        else:
            uuid = self.escapePath(uuid[-1].replace('-', ''))
        pdebug("Okay, the UUID must be %s" % (uuid))
        
        if uuid != self.uuid:
            # Raise an exception if the UUIDs don't match.
            pdebug("But the UUID of this cell is %s!" % (self.uuid))
            raise UUIDMismatchException(
                'The UUID in the provided URL (%s) does not match the one ' +
                'on record for this cell (%s)' % (uuid, self.uuid))
        
        try:
            parsed_url = urlparse(url)
            if parsed_url.scheme == 'http':
                h = httplib.HTTPConnection(parsed_url.netloc)
            elif parsed_url.scheme == 'https':
                # TODO: Is it safe to keep these keys and
                # certs around???
                h = httplib.HTTPSConnection(parsed_url.netloc,
                                            key_file=self.key,
                                            cert_file=self.cert)
            pdebug("Performing initial data sync.")
            h.request('GET', parsed_url.path,
                      headers={'Referer': self.referer})
            resp = h.getresponse()
            if resp.status != httplib.OK:
                # TODO: Handle errors
                pdebug("Didn't get OK response!!")
            else:
                # Send the response out for a local merge.
                pdebug("Performing merge...")
                self.doUpdate(resp.read(), False)
            
            pdebug("Performing initial peers sync.")
            h.request('GET', parsed_url.path + '/Peers',
                      headers={'Referer': self.referer})
            resp = h.getresponse()
            if resp.status != httplib.OK:
                # TODO: Handle errors
                pdebug("Didn't get OK response!!")
            else:
                pdebug("Merging peers...")
                # Add the peer we connect to before the update.
#                self.peers[str(url)] = {'cert': False, 'url': str(url)}
                self.peers.update(dpropjson.loads(resp.read()))
                self.peersEtag = makeEtag(dpropjson.dumps(self.peers))
                pdebug("New etag: %s" % (self.peersEtag))
            
            h.close()
            
            pdebug("Setting up addToPeer thunks...")
            for peerKey in self.peers:
                if peerKey == self.referer:
                    continue
                gobject.idle_add(thunkifyAddToPeer(self.peers[peerKey]))
            pdebug("Setting up peerSync thunks...")
            gobject.timeout_add(SYNC_INTERVAL, startSyncThunk)
        except httplib.HTTPException, exc:
            # TODO: Handle exceptions
            # e.g. BadStatusError, which might happen when crossing HTTPS and HTTP
            pdebug("Caught HTTPException: %s: %s" % (type(exc).__name__,
                                                     str(exc)))
            pass
    
#    @dbus.service.method('edu.mit.csail.dig.DPropMan.Cell',
#                         in_signature='ass', out_signature='')
#    def grantAccessTo(self, certs, accessType):
#        """[DBUS METHOD] Grant remote access of type accessType to a cell to
#        people bearing any of the provided certificates."""
#        if accessType == 'r':
#            # Read Access
#            for cert in certs:
#                self.readAccess.add(cert)
#        elif accessType == 'w':
#            # Read/Write Access
#            for cert in certs:
#                self.writeAccess.add(cert)
#            for cert in certs:
#                if cert in self.readAccess:
#                    self.readAccess.remove(cert)
#        else:
#            raise BadAccessTypeException(
#                "Don't understand access type '%s'" % (accessType))
    
#    @dbus.service.method('edu.mit.csail.dig.DPropMan.Cell',
#                         in_signature='', out_signature='s')
#    def accessType(self):
#        """[DBUS METHOD] Return the type of access granted (always 'w' for
#        local cells)."""
#        return 'w'
    
#    def accessForCert(self, cert):
#        """Return the type of access granted to the certificate provided."""
#        cert = formatCertName(cert.get_subject())
#        if cert in self.readAccess:
#            return 'r'
#        elif cert in self.writeAccess or cert == False:
#            # TODO: Actually test for UseSSL.
#            return 'w'
#        else:
#            return ''
    
#    def destroyNeighbors(self):
#        """Notify all peers that we no longer care about the cell."""
#        for peer in self.peers:
#            # Need to forward the destruction along to the remote cells.
#            try:
#                url = urlparse(self.peers[peer]['url'] + '/Peers/' +
#                               pathifyURL(self.referer))
#                if url.scheme == 'http':
#                    h = httplib.HTTPConnection(url.netloc)
#                elif url.scheme == 'https':
#                    h = httplib.HTTPSConnection(url.netloc,
#                                                key_file=self.key,
#                                                cert_file=self.cert)
#                h.request('DELETE', url.path,
#                          header={'Referer': self.referer})
#                resp = h.getresponse()
#                if resp.status != httplib.ACCEPTED:
#                    # TODO: Handle errors
#                    pass
#                h.close()
#            except httplib.HTTPException:
#                # TODO: Handle exceptions
#                pass
    
#    @dbus.service.method('edu.mit.csail.dig.DPropMan.Cell',
#                         in_signature='', out_signature='b')
#    def exists(self):
#        """[DBUS METHOD] A hack to test for a claim on a cell."""
#        return True
    
#    def destroy(self):
#        """Destroy the cell."""
#        self.DestroySignal()
#        self.destroyNeighbors()
#        self.remove_from_connection()

ObjectPathRegexp = re.compile('[A-Za-z0-9/]')

class DPropMan(dbus.service.Object):
    """The DProp manager.  Contains functions accessible by DBus."""
    
    def __init__(self, conn, object_path, port, hostname,
                 useSSL=False, cert=None, key=None):
        """Initializes the DPropMan object."""
        pdebug("Initializing DPropMan object")
        dbus.service.Object.__init__(self, conn, object_path)
        self.cells = {}
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
                         in_signature='s', out_signature='s')
    def registerCell(self, uuid):
        """[DBUS METHOD] Registers a new cell belonging to the group
        identified by the given UUID if it does not yet exist.
        
        Returns the UUID of the created cell.
        
        If uuid is not provided, one will be generated.
        
        You should also handle listening for updates to merge this cell."""
        pdebug("Received registerCell() call for %s" % uuid)
        if uuid == '':
            # TODO: Should this use something other than UUID1 in the future?
            uuid = str(uuid1()).replace('-', '')
        else:
            uuid = self.escapePath(uuid.replace('-', ''))
        
        if uuid not in self.cells:
            if self.useSSL:
                referer = "https://%s:%d/Cells/%s" % (self.hostname,
                                                      self.port,
                                                      uuid)
            else:
                referer = "http://%s:%d/Cells/%s" % (self.hostname,
                                                     self.port,
                                                     uuid)
            pdebug("Creating cell!")
            self.cells[uuid] = Cell(self.conn,
                                    "/Cells/%s" % (uuid),
                                    uuid,
                                    referer,
                                    self.cert,
                                    self.key)
        else:
            pdebug("Already created cell!")
        
        return uuid
    
    @dbus.service.method('edu.mit.csail.dig.DPropMan',
                         in_signature='s', out_signature='s')
    def registerRemoteCell(self, url):
        """[DBUS METHOD] Registers interest in a cell on a remote server at
        the given URL.  Returns the UUID.  Make sure you call
        connectToRemote on the cell after you register for merges, as
        this doesn't actually connect to the remote group!
        
        You should also probably register to handle merging of this
        cell."""
        pdebug("Received registerRemoteCell() call for %s" % url)
        # Are we already connected to the remote cell?  If so, return
        # the local name.
        parsed_url = urlparse(url)
        if parsed_url.scheme != 'http' and parsed_url.scheme != 'https':
            # Balk at non-HTTP addresses.
            raise InvalidURLException(
                'The remote cell URL %s is not a valid HTTP URL.' % (url))
        
        uuid = parsed_url.path.split('/')
        if uuid[-1] == '':
            uuid = self.escapePath(uuid[-2])
        else:
            uuid = self.escapePath(uuid[-1])
        pdebug("So the UUID is actually %s" % (uuid))
        
        # Register the cell and send out a query for it.
        if uuid not in self.cells:
            if self.useSSL:
                referer = "https://%s:%d/Cells/%s" % (self.hostname,
                                                      self.port,
                                                      uuid)
            else:
                referer = "http://%s:%d/Cells/%s" % (self.hostname,
                                                     self.port,
                                                     uuid)
            
            # Check if we're the remote host.  If so, we just need to
            # run a local registerCell.
            try:
                ips = socket.gethostbyname_ex(parsed_url.hostname)
            except:
                pass
            for ip in ips[2]:
                ip += ':%d' % (parsed_url.port)

                if ip in map(lambda x: '%s:%d' % (x, self.port),
                             ipAddresses()):
                    pdebug("Wait...  This is actually local!")
                    return self.registerCell(uuid)
            
            # Okay, not the remote host.  Then we need to actually
            # mint the new cell and tell it to pull from the remote
            # host.
            pdebug("Creating cell!")
            self.cells[uuid] = Cell(self.conn,
                                    "/Cells/%s" % (uuid),
                                    uuid,
                                    referer,
                                    self.cert,
                                    self.key)
            
            # Let the client control the first pull/push so they can
            # merge it too.
        return uuid

    @dbus.service.method('edu.mit.csail.dig.DPropMan',
                         in_signature='s', out_signature='b')
    def cellExists(self, uuid):
        """[DBUS METHOD] Returns true if a cell with the given UUID already
        exists."""
        return str(uuid) in self.cells

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
        # TODO: Really???
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
