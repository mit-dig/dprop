"""Some functions to import that will automatically handle client-side
SSL encryption/decryption for DProp.  Use these in a Python app that's
communicating with DPropMan, and these functions will make the
appropriate signal handlers that you can then register as needed.."""

import httplib
import dpropjson
from urlparse import urlparse

def makeSSLSyncSignalThunk(cell, key, cert):
    """Returns a thunk that can be used on receiving a SyncSignal from
    cell.  Will attempt to connect to the requested peer using key and
    cert if the peer is an SSL peer.  The thunk does not create a new
    thread, so you may wish to wrap it with a thunk that does."""
    
    # TODO: Still need to fix security of forwarding
    # updateCell/updatePeers
    
    # TODO: This is something of a carte-blanque to forcing GETs to
    # arbitrary servers.  This should be locked down.
    def thunk(forgeryKey, referer, peer, etag, peersEtag):
        referer = str(referer)
        peer = dpropjson.loads(str(peer))
        etag = str(etag)
        peersEtag = str(peersEtag)
        forgeryKey = str(forgeryKey)
        
        # Check the forgery key against what the cell says.
        if not cell.checkForgeryKey(forgeryKey):
            return
        
        try:
            url = urlparse(peer['url'])
            if url.scheme == 'http':
                h = httplib.HTTPConnection(url.netloc)
            elif url.scheme == 'https':
                h = httplib.HTTPSConnection(url.netloc,
                                            key_file=key,
                                            cert_file=cert)
#            pdebug("Attempting to sync data")
            h.request('GET', url.path,
                      headers={'Referer': referer,
                               'If-None-Match': etag})
            resp = h.getresponse()
            if resp.status == httplib.OK:
                # Got a response.  Time to merge.
#                pdebug("Got new data in sync. Merging...")
                cell.UpdateSignal(forgeryKey, resp.read(), peer['url'],
                                  dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
            elif resp.status == httplib.NOT_MODIFIED:
#                pdebug("Sync resulted in no new data.")
                # Dummy resp.read() to make sure we can reuse h.
                resp.read()
            else:
                # TODO: Handle errors
#                pdebug("Unexpected HTTP response!")
                pass
            
            # Next, sync peers.
#            pdebug("Attempting to sync peers")
            h.request('GET', url.path + '/Peers',
                      headers={'Referer': referer,
                               'If-None-Match': peersEtag})
            # Why do I get a ResponseNotReady error?
            # Why are the peers a bad eTag?
            resp = h.getresponse()
            if resp.status == httplib.OK:
                # Got a response.  Time to merge.
#                pdebug("Got new data in sync. Merging...")
                cell.updatePeers(resp.read(),
                                 dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
            elif resp.status == httplib.NOT_MODIFIED:
#                pdebug("Sync resulted in no new data.")
                pass
            else:
                # TODO: Handle errors
#                pdebug("Unexpected HTTP response!")
                pass
                
            h.close()
        except httplib.HTTPException, exc:
            # TODO: Handle exceptions
#            pdebug("Caught HTTPException: %s: %s" % (type(exc).__name__,
#                                                     str(exc)))
            pass

    return thunk

def makeSSLSendUpdateSignalThunk(cell, key, cert):
    """Returns a thunk that can be used on receiving a SendUpdateSignal
    from cell.  Will attempt to connect to the requested peer using
    key and cert if the peer is an SSL peer.  The thunk does not
    create a new thread, so you may wish to wrap it with a thunk that
    does."""
    # TODO: Do we really want to include the message???  An adversary
    # could forge it.

    # TODO: This is something of a carte-blanque to forcing PUTs to
    # arbitrary servers.  This should be locked down.
    def thunk(forgeryKey, referer, peer, message):
        referer = str(referer)
        peer = dpropjson.loads(str(peer))
        message = str(message)
        forgeryKey = str(forgeryKey)
        
        # Check the forgery key against what the cell says.
        if not cell.checkForgeryKey(forgeryKey):
            return
        
        try:
            url = urlparse(peer['url'])
            if url.scheme == 'http':
                h = httplib.HTTPConnection(url.netloc)
            elif url.scheme == 'https':
                h = httplib.HTTPSConnection(url.netloc,
                                            key_file=key,
                                            cert_file=cert)
            # We don't dpropjson encode here, as the data
            # should already be in JSON.
            h.request('PUT', url.path,
                      urllib.urlencode(
                    {'data': message}),
                      {'Referer': referer,
                       'Content-Type':
                           'application/x-www-form-urlencoded'})
            resp = h.getresponse()
            if resp.status != httplib.ACCEPTED:
                # TODO: Handle errors
#                pdebug("Didn't get ACCEPTED response!!")
                pass
            else:
#                pdebug("PeerUpdate was ACCEPTED!!")
                pass
            h.close()
        except httplib.HTTPException, exc:
            # TODO: Handle exceptions
#            pdebug("Caught HTTPException: %s: %s" % (type(exc).__name__,
#                                                     str(exc)))
            pass
    
    return thunk

def makeSSLPeerAddSignalThunk(cell, key, cert):
    """Returns a thunk that can be used on receiving a PeerAddSignal
    from cell.  Will attempt to connect to the requested peer using
    key and cert if the peer is an SSL peer.  The thunk does not
    create a new thread, so you may wish to wrap it with a thunk that
    does."""
    # TODO: This is something of a carte-blanque to forcing PUTs to
    # arbitrary servers.  This should be locked down.
    
    # Load the certificate from the cert file.
    cf = open(cert, 'r')
    cert_text = cf.read()
    cf.close()
    
    def thunk(forgeryKey, referer, peer):
        referer = str(referer)
        peer = dpropjson.loads(str(peer))
        forgeryKey = str(forgeryKey)
        
        # Check the forgery key against what the cell says.
        if not cell.checkForgeryKey(forgeryKey):
            return
        
        try:
            url = urlparse(peer['url'])
            if url.scheme == 'http':
                h = httplib.HTTPConnection(url.netloc)
            elif url.scheme == 'https':
                h = httplib.HTTPSConnection(url.netloc,
                                            key_file=key,
                                            cert_file=cert)
            h.request('PUT', url.path + '/Peers',
                      urllib.urlencode(
                    {'peer': dpropjson.dumps({'url': referer,
                                              'cert': cert_text})}),
                      {'Referer': referer,
                       'Content-Type':
                           'application/x-www-form-urlencoded'})
            resp = h.getresponse()
            if resp.status != httplib.OK:
                # TODO: Handle errors
#                pdebug("Didn't get OK response!!")
                pass
            else:
#                pdebug("PeerAdd was OK!!")
                pass
            h.close()
        except httplib.HTTPException, exc:
            # TODO: Handle exceptions
#            pdebug("Caught HTTPException: %s: %s" % (type(exc).__name__,
#                                                     str(exc)))
            pass
    
    return thunk

def makeSSLDoInitSignalThunk(cell, key, cert):
    """Returns a thunk that can be used on receiving a DoInitSignal
    from cell.  Will attempt to connect to the requested peer using
    key and cert if the peer is an SSL peer.  The thunk does not
    create a new thread, so you may wish to wrap it with a thunk that
    does."""
    
    # TODO: This is something of a carte-blanque to forcing PUTs to
    # arbitrary servers.  This should be locked down.
    def thunk(forgeryKey, referer, url):
        referer = str(referer)
        url = str(url)
        forgeryKey = str(forgeryKey)
        
        # Check the forgery key against what the cell says.
        if not cell.checkForgeryKey(forgeryKey):
            return
        
        try:
            parsed_url = urlparse(url)
            if parsed_url.scheme == 'http':
                h = httplib.HTTPConnection(parsed_url.netloc)
            elif parsed_url.scheme == 'https':
                h = httplib.HTTPSConnection(parsed_url.netloc,
                                            key_file=key,
                                            cert_file=cert)
            pdebug("Performing initial data sync.")
            h.request('GET', parsed_url.path,
                      headers={'Referer': referer})
            resp = h.getresponse()
            if resp.status != httplib.OK:
                # TODO: Handle errors
#                pdebug("Didn't get OK response!!")
                pass
            else:
                # Send the response out for a local merge.
#                pdebug("Performing merge...")
                cell.UpdateSignal(forgeryKey, resp.read(), url,
                                  dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
            
#            pdebug("Performing initial peers sync.")
            h.request('GET', parsed_url.path + '/Peers',
                      headers={'Referer': referer})
            resp = h.getresponse()
            if resp.status != httplib.OK:
                # TODO: Handle errors
#                pdebug("Didn't get OK response!!")
                pass
            else:
#                pdebug("Merging peers...")
                # Add the peer we connect to before the update.
#               self.peers[str(url)] = {'cert': False, 'url': str(url)}
                cell.updatePeers(resp.read(),
                                 dbus_interface='edu.mit.csail.dig.DPropMan.Cell')
            
            h.close()
        except httplib.HTTPException, exc:
            # TODO: Handle exceptions
            # e.g. BadStatusError, which might happen when crossing HTTPS and HTTP
#            pdebug("Caught HTTPException: %s: %s" % (type(exc).__name__,
#                                                     str(exc)))
            pass
        
    return thunk
