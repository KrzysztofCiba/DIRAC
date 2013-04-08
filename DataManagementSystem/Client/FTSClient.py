########################################################################
# $HeadURL $
# File: FTSClient.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/08 14:29:43
########################################################################

""" :mod: FTSClient
    ===============

    .. module: FTSClient
    :synopsis: FTS client
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    FTS client
"""

__RCSID__ = "$Id $"

# #
# @file FTSClient.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/08 14:29:47
# @brief Definition of FTSClient class.

# # imports
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Base.Client import Client
# # from RMS
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
# # from DMS
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.Client.FTSJobFile import FTSJobFile
from DIRAC.DataManagementSystem.Client.FTSLfn import FTSLfn
from DIRAC.DataManagementSystem.private.FTSValidator import FTSValidator

########################################################################
class FTSClient( Client ):
  """
  .. class:: FTSClient

  """
  # # placeholder for FTSValidator
  __ftsValidator = None
  # # placeholder for FTSManager
  __ftsManager = None
  # # placeholder for request manager
  __requestClient = None

  def __init__( self, useCertificates = False ):
    """c'tor

    :param self: self reference
    :param bool useCertificates: flag to enable/disable certificates
    """
    Client.__init__( self )
    self.log = gLogger.getSubLogger( "DataManagement/FTSClient" )
    self.setServer( "DataManagement/FTSManager" )

  @classmethod
  def ftsValidator( cls ):
    """ get FTSValidator instance """
    if not cls.__ftsValidator:
      cls.__ftsValidator = FTSValidator()
    return cls.__ftsValidator

  @classmethod
  def ftsManager( cls, timeout = 120 ):
    """ get FTSManager instance """
    if not cls.__ftsManager:
      url = PathFinder.getServiceURL( "DataManagement/FTSManager" )
      if not url:
        raise RuntimeError( "CS option DataManagement/FTSManager URL is not set!" )
      cls.__ftsManager = RPCClient( url, timeout = timeout )
    return cls.__ftsManager

  @classmethod
  def requestClient( cls ):
    """ request client getter """
    if not cls.__requestClient:
      cls.__requestClient = RequestClient()
    return cls.__requestClient

