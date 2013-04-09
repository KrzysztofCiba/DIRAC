########################################################################
# $HeadURL $
# File: FTSSchedule.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/28 09:31:13
########################################################################

""" :mod: FTSSchedule
    =================

    .. module: FTSSchedule
    :synopsis: FTS schedule operation handler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    FTS schedule operation handler


    This one should be executed outside of ProcessPool.
"""

__RCSID__ = "$Id $"

# #
# @file FTSSchedule.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/28 09:31:23
# @brief Definition of FTSSchedule class.

# # imports
from DIRAC import S_OK, S_ERROR, gMonitor, gConfig
from DIRAC.RequestManagementSystem.private.BaseOperation import BaseOperation
from DIRAC.DataManagementSystem.private.StrategyHandler import StrategyHandler
from DIRAC.DataManagementSystem.DB.FTSDB import FTSDB


########################################################################
class FTSSchedule( BaseOperation ):
  """
  .. class:: FTSSchedule

  """

  def __init__( self, operation = None ):
    """c'tor

    :param self: self reference
    """
    BaseOperation.__init__( self, operation )
    # # gMonitor stuff
    gMonitor.registerActivity( "FileScheduleAtt", "Files schedule attempted",
                               "FTSSchedule", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "FileScheduleOK", "File schedule successful",
                               "FTSSchedule", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "FileScheduleFail", "File schedule failed",
                               "FTSSchedule", "Files/min", gMonitor.OP_SUM )

  def __call__( self ):
    """ execute """
    self.log.always( "called " % self.__class__.__name__ )

    return S_OK()

  def checkFiles( self ):
    """ """
    pass

