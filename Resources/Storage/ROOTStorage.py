########################################################################
# $HeadURL $
# File: ROOTStorage.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/06/27 12:42:13
########################################################################

""" :mod: ROOTStorage 
    =======================
 
    .. module: ROOTStorage
    :synopsis: xroot protocol storage element
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    xroot/scalla protocol storage element interface
"""

__RCSID__ = "$Id $"

##
# @file ROOTStorage.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/06/27 12:42:27
# @brief Definition of ROOTStorage class.

## imports 
import os
import os.path
import sys
import re
from types import StringTypes, ListType, DictType 
## from DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Resources.Storage.StorageBase import StorageBase
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.Utilities.File import getSize
from stat import *

########################################################################
class ROOTStorage( StorageBase ):
  """
  .. class:: ROOTStorage
  
  """
  def __init__( self, storageName, protocol, path, host, port, spaceToken, wspath ):
    """c'tor

    :param self: self reference
    :param str storageName: SE name
    :param str protocol: protocol to use
    :param str path: SE root directory
    :param str host: SE host
    :param int port: port
    :param str spaceToken: space token
    :param str wspath: URI chunk on :host: 
    """
    ## private logger 
    self.log = gLogger.getSubLogger( "ROOTStorage", True )

    ## a priori good
    self.isok = True
    ## save c'tor args
    self.protocolName = "ROOT"
    self.name = storageName
    self.protocol = protocol
    self.path = path
    self.host = host
    self.port = port
    self.wspath = wspath
    self.spaceToken = spaceToken
    self.cwd = self.path

    ## base class init
    StorageBase.__init__( self, self.name, self.wspath )

    ## xrd timeout
    self.xrdTimeout = gConfig.getValue( "/Resources/StorageElements/XRDTimeout", 300 )
    self.xrdRetry = gConfig.getValue( "/Resources/StorageElements/XRDRetry", 3 )

  
  def isPfnForProtocol( self, pfn ):
    """ check if supplied pfn is valid for XROOT protocol

    :param self: self reference
    :param str pfn: PFN
    """
    res = pfnparse( pfn )
    return S_OK( res if not res["OK"] else res["Value"]["Protocol"] == self.protocol ) 

  def changeDirectory( self, directory ):
    """ cd dir

    :param self: self reference
    :param str directory:
    """
    self.cwd = os.path.normpath( os.path.join( self.cwd, directory.lstrip("/") ) )

  def createDirectory( self, path ):
    """ mkdir path

    :param self: self reference
    :param str path: path to create
    """
    
    pass

  def getCurrentURL( self ):
    """ get full SE URL including fileName

    :param self: self reference
    """
    

    pass

  def getDirectory( self, path ):
    """ cp -r "SE path" . 

    :param self: self reference
    """
    pass


  def getDirectoryMetadata( self, path ):
    """ ls -l, but not really 

    :param self: self reference

    """
    pass

  def getDirectorySize( self , path ):
    """ du -s dir

    :param self: self reference
    """
    pass

  def getFile( self, path ):
    """ cp SE localFS

    :param self: self reference
    """

    pass

  def getFileMetadata( self, path  ):
    """ ls -l file

    :param self: self reference
    """

    pass
  
  def getFileSize( self, path ):
    """ du file

    :param self: self reference
    """

    pass

  def getParameteres( self ):
    """ get the original parameters dict 
 
    :param self: self reference
    """
    return S_OK( { "StorageName" : self.name,
                   "ProtocolName" : self.protocolName,
                   "Protocol" : self.protocol,
                   "Host" : self.host,
                   "Path" : self.path,
                   "Port" : self.port,
                   "SpaceToken" : self.spaceToken,
                   "WSUrl" : self.wspath } )

  def exists( self, urls ):
    """ test -d or test -f

    :param self: self reference
    :param mixed urls: urls to check 
    """
    urls = self.__checkArgumentFormat( urls )
    if not urls["OK"]:
      return urls
    
    successful = []
    failed = {}
    isDirectory = self.isDirectory( urls )
    if not isDirectory["OK"]:
      return isDirectory

    isFile = self.isFile( urls["Value"]["Failed"].keys() )
    if not isFile["OK"]:
      return isFile
    isDirectory = isDirectory["Value"]
    isFile = isFile["Value"]
    successful = isDirectory["Successful"].update( isFile["Successful"] )
    return S_OK( { "Successful" : dict.fromkeys( successful.keys(), True), 
                   "Failed" : dict.fromkeys( [ url for url in urls if url not in successful.keys() ], False ) } )
    
  def getProtocolPfn( self ):
    """

    :param self: self reference
    """

    pass

  def getTransportURL( self ):
    """

    :param self: self reference
    """
    pass

  def isDirectory( self, urls ):
    """ test -d dir

    :param self: self reference
    :param mixed urls: urls to check
    """
    urls = self.__checkArgumentFormat( urls )
    if not urls["OK"]:
      return urls
    successful = []
    failed = []
    for url in urls["Value"]:
      res = self.__xrd_wrapper( "existdir", url )
      if not res["OK"]:
        failed.append( ( url, res["Message"] ) )
      exitCode, stdOut, stdErr = res["Value"]
      if exitCode != 0:
        failed.append( ( url, stdErr ) )
      if "the directory exists" in stdOut.lower():
        successful.append( url )
        continue
      failed.append( ( url, False ) )
    return S_OK( { "Successful" : dict.fromkeys( successful, True ), "Failed" : dict(failed) } )

  def isFile( self, urls ):
    """ test -f file

    :param self: self reference
    :param mixed urls: paths to check
    """
    urls = self.__checkArgumentFormat( urls )
    if not urls["OK"]:
      return urls
    successful = []
    failed = []
    for url in urls["Value"]:
      res = self.__xrd_wrapper( "existfile", url )
      self.log.always( res )
      if not res["OK"]:
        failed.append( ( url, res["Message"] ) )
      exitCode, stdOut, stdErr = res["Value"]
      if exitCode != 0:
        failed.append( ( url, stdErr ) )
      if "the file exists" in stdOut.lower():
        successful.append(  url )
        continue
      failed.append( ( url, False ) ) 

    return S_OK( { "Successful": dict.fromkeys( successful, True ), "Failed" : dict(failed) } )  

  def listDirectory( self, urls ):
    """ ls dir
    
    :param self: self reference
    :param mixed urls: folders to check
    """
    urls = self.__checkArgumentFormat( urls )
    if not urls["OK"]:
      return urls
    isDirectory = self.isDirectory( urls["Value"] )
    if not isDirectory["OK"]:
      return isDirectory
    urls = isDirectory["Value"]["Successful"].keys()
    successful = {} 
    failed = isDirectory["Value"]["Failed"]
    for url in sorted(urls):
      res = self.__xrd_wrapper( "dirlist", url )
      if not res["OK"]:
        failed[url] = res["Message"]
      exitCode, stdOut, stdErr = res["Value"]
      if exitCode != 0:
        failed[url] = stdErr
        continue
      subDirs = {}
      files = {}
      for line in stdOut.split("\n"):
        if not line:
          continue
        permission, size, cDate, cTime, path  = line.split()
        if permission.startswith("d"):
          subDirs[path] = True 
        else:
          files[path] = { "Size" : int(size) }
      successful[url] = { "SubDirs" : subDirs, "Files" : files }
    return S_OK( { "Successful" : successful, "Failed" : failed } )
         
  def pinFile( self ):
    """ pin file 

    :param self: self reference
    """
    pass

  def prestageFile( self ):
    """ prestage file

    :param self: self reference
    """
    pass

  def prestageFileStatus( self ):
    pass

  def putFile( self ):
    """ cp localFS SE
 
    :param self: self reference
    """
    pass

  def releaseFile( self ):
    """ un-pin file

    :param self: self reference
    """
    pass

  def removeDirectory( self ):
    """ rm or even rm -rf 

    :param self: self referece
    """

    pass

  def removeFile( self ):
    """ rm file

    :param self: self reference
    """
    pass

  
  def __checkArgumentFormat( self, path ):
    """ check and convert :path: to list of paths 

    :param self: self reference
    :param mixed path: arg to check  
    """
    if type( path ) in StringTypes:
      urls = [ path ]
    elif type( path ) == ListType:
      urls = path
    elif type( path ) == DictType:
      urls = path.keys()
    else:
      return S_ERROR( "__checkArgumentFormat: Supplied path is not of the correct format." )
    return S_OK( urls )
  
  def __xrd_wrapper( self, operation, url, timeout=None, callback=None ):
    """ xrd wrapper calling :operation: on :url:

    :param self: self reference
    :param str operation: xdm command
    :param mixed url: pfn undergoing :operation: 
    """
    timeout = timeout if timeout else self.xrdTimeout
    retry = self.xrdRetry if self.xrdRetry else 1
    
    self.log.info("entering xrd wrapper timeout=%s retry=%s operation=%s url=%s" % ( timeout, retry, operation, url ) )
 
    ## TODO: not sure if it is right
    if not url.startswith( self.path ):
      url = "%s/%s" % ( self.path, url )
      self.log.debug( "new path = %s" % url )
      
    command = " ".join( [ "xrd", self.host, operation, url ] )
    self.log.debug( command )
    while retry:
      retry -= 1
      res = shellCall( timeout, command, callback )
      self.log.debug( res )
      if not res["OK"] and res["Message"].startswith("Timeout"):
        timeout *= 2
        continue
      else: 
        return res
    return res  

  def __xrdcp_wrapper( self, source, destination, timeout=None, callback=None ):
    """ xrdcp wrapper

    :param self: self reference
    :param str source: source pfn
    :param str destination: destination pfn
    :param int timeout: xrdcp timeout in seconds
    :param callable callback: callback fcn 
    """
    timeout = timeout if timeout else self.xrdTimeout
    retry = self.xrdRetry if self.xrdRetry else 1
    self.log.info("entering xrdcp wrapper timeout=%s retry=%s source=%s destination=%s" % ( timeout, retry, source, destination ) )
   
    command = " ".join( [ "xrdcp", self.host, source, destination ] )
    self.log.debug( command )
    while retry:
      retry -= 1
      res = shellCall( timeout, command, callback )
      self.log.debug( res )
      if not res["OK"] and res["Message"].startswith("Timeout"):
        timeout *= 2
        continue
      else: 
        return res
    return res  


# TODO remove when ready
if __name__ == "__main__":
  from DIRAC.Core.Base.Script import parseCommandLine
  parseCommandLine()
  from DIRAC.Resources.Storage.StorageFactory import StorageFactory
  sf = StorageFactory()
  rs = sf.getStorages( "CERN-USER" )["Value"]["StorageObjects"][0]
  gLogger.always( rs.getParameteres() )
  res = rs.isDirectory( [ "/user/c/cibak", "user/c/cibak/a", "user/c/cibak/b"] )
  res = rs.isFile( [ "user/c/cibak/cert-test2"] )

  res = rs.listDirectory( "user/c/cibak/" )

  gLogger.always( res )
