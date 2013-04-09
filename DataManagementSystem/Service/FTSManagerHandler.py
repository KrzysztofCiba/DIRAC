########################################################################
# $HeadURL $
# File: FTSManagerHandler.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/08 14:24:08
########################################################################

""" :mod: FTSManagerHandler 
    =======================
 
    .. module: FTSManagerHandler
    :synopsis: handler for FTSDB using DISET
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    service handler for FTSDB using DISET
"""

__RCSID__ = "$Id $"

##
# @file FTSManagerHandler.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/08 14:24:30
# @brief Definition of FTSManagerHandler class.

## imports 
## imports
from types import DictType, IntType, ListType, StringTypes
## from DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
## from DMS
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.Client.FTSJobFile import FTSJobFile
from DIRAC.DataManagementSystem.Client.FTSLfn import FTSLfn
from DIRAC.DataManagementSystem.private.FTSValidator import FTSValidator

## global instance of FTSDB
gFTSDB = None

def initializeRequestManagerHandler( serviceInfo ):
  """ initialise handler """
  global gFTSDB
  from DIRAC.DataManagementSystem.DB.FTSDB import FTSDB
  gFTSDB = FTSDB()
  return S_OK()

########################################################################
class FTSManagerHandler(RequestHandler):
  """
  .. class:: FTSManagerHandler
  
  """
  __ftsValidator = None

  @classmethod
  def ftsValidator( cls ):
    """ FTSValidator instance getter """
    if not cls.__ftsValidator:
      cls.__ftsValidator = FTSValidator()
    return cls.__ftsValidator


