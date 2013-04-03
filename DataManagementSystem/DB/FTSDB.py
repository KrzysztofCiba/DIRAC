########################################################################
# $HeadURL $
# File: FTSDB.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/02 15:13:51
########################################################################
""" :mod: FTSDB
    ===========

    .. module: FTSDB
    :synopsis: FTS DB
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    FTS DB
"""

__RCSID__ = "$Id $"

# #
# @file FTSDB.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/02 15:13:57
# @brief Definition of FTSDB class.

# # imports
# # from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities.LockRing import LockRing
from DIRAC.DataManagementSystem.Client.FTSReq import FTSReq
from DIRAC.DataManagementSystem.Client.FTSFile import FTSFile

########################################################################
class FTSDB( DB ):
  """
  .. class:: FTSDB

  """

  def __init__( self, systemInstance = "Default", maxQueueSize = 10 ):
    """c'tor

    :param self: self reference
    :param str systemInstance: ???
    :param int maxQueueSize: size of queries queue
    """
    DB.__init__( self, "FTSDB", "DataManagement/FTSDB", maxQueueSize )
    # # private lock
    self.getIdLock = LockRing.getLock()
    # # max attmprt for reschedule
    self.maxAttempt = 100
    # # check tables
    self._checkTables( False )

  @staticmethod
  def getTableMeta():
    """ get db schema in a dict format """
    return dict( [ ( classDef.__name__, classDef.tableDesc() )
                   for classDef in ( FTSReq, FTSFile ) ] )

  def _checkTables( self, force = False ):
    """ create tables if not exisiting """
    return self._createTables( self.getTableMeta(), force = force )
