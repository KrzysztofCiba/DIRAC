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

    xroot protocol storage element interface
"""

__RCSID__ = "$Id $"

##
# @file ROOTStorage.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/06/27 12:42:27
# @brief Definition of ROOTStorage class.

## imports 
import os
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
    
    pass

  def exists( self, path ):
    """ 

    :param self: self reference
    """
    pass

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

  def isDirectory( self ):
    """ test -d dir

    :param self: self reference
    """

    pass

  def isFile( self ):
    """ test -f file

    """

    pass

  def listDirectory( self ):
    """ ls dir


    :param self: self reference
    """

    pass

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

  def prestageFIleStatus( self ):
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

  
  def __xrd_wrapper( self, operation, url ):
    """ xrd wrapper calling :operation: on :url:

    :param self: self reference
    :param str operation: xdm command
    :param mixed url: pfns undergoing :operation: 
    """
    pass
