########################################################################
# $HeadURL $
# File: FTSSchedule.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/28 09:31:13
########################################################################

""" :mod: FTSSchedule 
    =======================
 
    .. module: FTSSchedule
    :synopsis: FTS schedule operation handler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    FTS schedule operation handler
"""

__RCSID__ = "$Id $"

##
# @file FTSSchedule.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/28 09:31:23
# @brief Definition of FTSSchedule class.

## imports 
from DIRAC import S_OK, S_ERROR, gMonitor, gConfig
from DIRAC.RequestManagementSystem.private.BaseOperation import BaseOperation

########################################################################
class FTSSchedule(BaseOperation):
  """
  .. class:: FTSSchedule
  
  """

  def __init__( self, operation=None ):
    """c'tor

    :param self: self reference
    """
    BaseOperation.__init__( self, operation )
    
  def __call__( self ):
    """ execute """
    self.log.always( "called " % self.__class__.__name__ )
    return S_OK()

