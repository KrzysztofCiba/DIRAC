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
    StorageBase.__init__( self )
  
    
