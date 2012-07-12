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

#from DIRAC.Core.Base.Script import parseCommandLine
#parseCommandLine()

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

def urlsAsList( fcn ):
  """ checks urls args type and return it as a list of str

  :param self: fcn to call
  """
  def wrapper( self, *args, **kwargs ):
    """ inside worker
    
    :warn: arg urls has to be defined as 2nd one, just after self 
    
    :param self: instance reference
    :param args: non-keyword args
    :param kwargs: keyword args
    """
    urls = kwargs["urls"] if "urls" in kwargs else None 
    if not urls:
      urls = args[0] if len(args) == 1 else None
    
    urlsList = None
    if type( urls ) in ( str, unicode ):
      urlsList = [ urls ]
      #return fcn( self, urls = [ urls ] )
    elif type( urls ) in ( dict, list ):
      urlsList = [ key for key in urls if type(key) in ( unicode, str ) ]
      if len( urlsList ) != len( urls ):
        return S_ERROR("%s wrong args type for urls!" % self.fcn.__name__ )  
    else:
      return S_ERROR("%s wrong args type for urls!" % self.fcn.__name__ )  
    ## update args & kwargs
    if "urls" not in kwargs:
      args = list( args )
      del args[0] 
      args = tuple( args )
    kwargs["urls"] = urlsList 
    return fcn( self, *args, **kwargs  )        
    
  return wrapper


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
      

  ###
  ### SURL/PFN/protocol/cwd/params manipulation
  ###

  def resetWorkingDirectory( self ):
    """ cd self.path

    :param self: self reference
    """
    self.cwd = self.path

  def getCurrentDirectory( self ):
    """ get cwd
    
    :param self: self reference
    """
    return S_OK( self.cwd )

  def getName( self ):
    """ get SE name
    
    :param self: self reference
    """
    return S_OK( self.name )

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

  def isPfnForProtocol( self, pfn ):
    """ check if supplied pfn is valid for XROOT protocol

    :param self: self reference
    :param str pfn: PFN
    """
    res = pfnparse( pfn )
    return res if not res["OK"] else S_OK( res["Value"]["Protocol"] == self.protocol ) 

  
  def getCurrentURL( self, fileName ):
    """ get full SE URL including fileName

    :param self: self reference
    :param str fileName: fine name
    """
    return S_OK( "%s/%s" % ( self.cwd, fileName.lstrip("/") if fileName else "" ) )  

  def getProtocolPfn( self, pfnDict, withPort ):
    """ contruct SURL to be used from :pfnDict:
    
    :param self: self reference
    :param dict pfnDict: pfn dict
    :param bool withPort: include port information
    """
    pfnDict["Protocol"] = self.protocol
    pfnDict["Host"] = self.host    
    pfnDict["Port"] = self.port if withPort else ""
    pfnDict["WSUrl"] = self.wspath if withPort else ""
    return  pfnunparse( pfnDict )

  def getTransportURL( self ):
    """

    :param self: self reference
    """
    pass

  ###
  ### directory manipulation
  ###

  def changeDirectory( self, directory ):
    """ cd dir 

    :param self: self reference
    :param str directory:
    """
    self.cwd = os.path.normpath( os.path.join( self.cwd, directory.lstrip("/") ) )

  @urlsAsList  
  def createDirectory( self, urls ):
    """ mkdir path

    TODO: need to use putFile with temp dir

    :param self: self reference
    :param str urls: path to create
    """
    successful = []
    failed = {}
    for url in urls:
      url2create = self.path
      for path in [ path.strip() for path in url.rstrip( self.path ).split("/") if path.strip() ]:
        url2create += "/%s" % path 
        self.log.always( "mkdir %s " % url2create )
        #res = { "OK" : True, "Value" : (0, "", "" ) }
        res = self.__xrd_wrapper( "mkdir", url2create )
        if not res["OK"]:
          self.log.error( "createDirectory: Failed to create directory on storage.", url2create )
          failed[url] = res['Message']
          continue
        exitCode, stdOut, stdErr = res["Value"]
        if exitCode:
          self.log.error( "createDirectory: Failed to create directory on storage.", url )
          failed[url] = stdErr
          continue
      self.log.debug( "createDirectory: Successfully created directory on storage: %s" % url )
      successful.append(url)
    return S_OK( { "Successful" : dict.fromkeys( successful, True ), 
                   "Failed" : failed } ) 

  @urlsAsList
  def getDirectory( self, urls, localPath=None ):
    """ cp -r "SE path" . 

    :param self: self reference
    :param mixed urls: source
    :param str localPath: cp destination (if omitted, cwd will be used)
    """
    pass

  @urlsAsList
  def getDirectoryMetadata( self, urls ):
    """ ls -l, but not really 

    :param self: self reference
    """
    pass

  @urlsAsList
  def getDirectorySize( self, urls ):
    """ du -s dir

    :param self: self reference
    :param mixed urls: paths to check
    """
    pass

  @urlsAsList
  def getFile( self, urls ):
    """ cp SE localFS

    :param self: self reference
    """

    pass

  @urlsAsList
  def getFileMetadata( self, urls ):
    """ ls -l file

    :param self: self reference
    """
    pass

  @urlsAsList
  def getFileSize( self, urls ):
    """ du file

    :param self: self reference
    :param mixed urls: paths to check
    """
    isFile = self.isFile( urls )
    if not isFile["OK"]:
      return isFile
    isFile = isFile["Value"]

    failed = isFile["Failed"]
    successful = {}
    
    for url in isFile["Successful"]:
      res = self.__xrd_wrapper( "stat", url )
      if not res["OK"]:
        failed[url] = res["Message"]
        continue
      exitCode, stdOut, stdErr = res["Value"]
      if exitCode:
        failed["url"] = stdErr
        continue
      try:
        size = int(stdOut.strip().split()[3].strip())
      except ( TypeError, ValueError ), error:
        failed[url] = str(error)
        continue
      successful[url] = size

    return S_OK( { "Successful" : successful, "Failed" : failed } )
      
  @urlsAsList
  def exists( self, urls ):
    """ test -d or test -f

    :param self: self reference
    :param mixed urls: urls to check 
    """    
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
    
  @urlsAsList
  def isDirectory( self, urls ):
    """ test -d dir

    :param self: self reference
    :param mixed urls: urls to check
    """
    successful = []
    failed = []
    for url in urls:
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

  @urlsAsList
  def isFile( self, urls ):
    """ test -f file

    :param self: self reference
    :param mixed urls: paths to check
    """
    successful = []
    failed = []
    for url in urls:
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

  @urlsAsList
  def listDirectory( self, urls ):
    """ ls dir
    
    :param self: self reference
    :param mixed urls: folders to check
    """
    isDirectory = self.isDirectory( urls )
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
          files[path] = { "Size" : int(size), 
                          "Permission" : permission, 
                          "CTime" : "%s %s" % ( cDate, cTime ) }
      successful[url] = { "SubDirs" : subDirs, "Files" : files }
    return S_OK( { "Successful" : successful, "Failed" : failed } )
         
  @urlsAsList
  def pinFile( self, urls ):
    """ pin file 

    :param self: self reference
    """
    pass
  @urlsAsList
  def prestageFile( self, urls ):
    """ prestage file

    :param self: self reference
    """
    pass

  @urlsAsList
  def prestageFileStatus( self, urls ):
    pass

  @urlsAsList
  def putFile( self, urls ):
    """ cp localFS SE
 
    :param self: self reference
    """
    pass

  @urlsAsList
  def releaseFile( self, urls ):
    """ un-pin file

    :param self: self reference
    """
    pass

  @urlsAsList
  def removeDirectory( self, urls ):
    """ rm or even rm -rf 

    :param self: self referece
    """
    successful = {}
    failed = {}
    pass

  @urlsAsList
  def removeFile( self, urls ):
    """ rm file

    :param self: self reference
    """
    pass
  
  ###
  ### xrd, xrdcp, xprep wrappers + __executeOperation
  ### 

  def __executeOperation( self, url, method ):
    """ execute the :method: with the supplied :urls:
    
    :param self: self reference
    :param mixed url: urls to use
    :param str method: member fcn name 
    """
    fcn = None
    if hasattr(self, method) and callable( getattr(self, method) ):
      fcn = getattr( self, method )
    if not fcn:
      return S_ERROR("Unable to invoke %s, it isn't a member funtion of ROOTStorage" % method )
    res = fcn( url )
    if not res["OK"]:
      return res
    elif url not in res["Value"]["Successful"]:
        return S_ERROR( res["Value"]["Failed"][url] )
    return S_OK( res["Value"]["Successful"][url] )


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
 
  from DIRAC.Resources.Storage.StorageFactory import StorageFactory
  sf = StorageFactory()
  rs = sf.getStorages( "CERN-USER" )["Value"]["StorageObjects"][0]
  #gLogger.always( rs.getParameteres() )
  #res = rs.isDirectory( [ "/user/c/cibak", "user/c/cibak/a", "user/c/cibak/b"] )
  #res = rs.isFile( [ "user/c/cibak/cert-test2"] )
  res = rs.listDirectory( "user/c/cibak/" )
  #res = rs.getFileSize( [ "user/c/cibak/cert-test2", "user/c/cibak/a" ] )
  #res = rs.createDirectory( "/user/c/cibak/a/b/c/d/e/f/g" )
  gLogger.always( res )
