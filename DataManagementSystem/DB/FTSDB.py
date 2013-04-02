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
    self.getIdLock = LockRing.getLock()
    # # max attmprt for reschedule
    self.maxAttempt = 100


