########################################################################
# $HeadURL $
# File: FTSHistoryView.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/19 12:26:30
########################################################################

""" :mod: FTSHistoryView
    ====================

    .. module: FTSHistoryView
    :synopsis: last hour history of FTS transfers as read from FTSHistoryView
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    last hour history of FTS transfers as read from FTSHistoryView
    this one is read-only!!!
"""

__RCSID__ = "$Id $"

# #
# @file FTSHistoryView.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/19 12:26:55
# @brief Definition of FTSHistoryView class.

# # imports
from DIRAC import S_OK

########################################################################
class FTSHistoryView( object ):
  """
  .. class:: FTSHistoryView

  helper class for FTSManagerHandler to keep 1h history of FTS transfers
  """

  def __init__( self, fromDict = None ):
    """c'tor

    :param self: self reference
    """
    fromDict = fromDict if fromDict else {}
    self.__data__ = dict.fromkeys( self.viewDesc()["Fields"].keys(), None )
    for key, value in fromDict.items():
      if key not in self.__data__:
        raise AttributeError( "Unknown FTSHistoryView attribute '%s'" % key )
      if value:
        setattr( self, key, value )

  @staticmethod
  def viewDesc():
    """ view description """
    return { "Fields": { "`SourceSE`": "`FTSJob`.`SourceSE`",
                          "`TargetSE`": "`FTSJob`.`TargetSE`",
                          "`FTSJobs`": "COUNT(DISTINCT `FTSJob`.`FTSJobID`)",
                          "`FTSServer`": "`FTSJob`.`FTSServer`",
                          "`Status`": "`FTSJob`.`Status`",
                          "`Files`": "SUM(`FTSJob`.`Files`)",
                          "`Size`": "SUM(`FTSJob`.`Size`)",
                          "`FailedFiles`": "SUM(`FTSJob`.`FailedFiles`)",
                          "`FailedSize`": "SUM(`FTSJob`.`FailedSize`)" },
             "SelectFrom" : "`FTSJob`",
             "Clauses": [ "`FTSJob`.`LastUpdate` > ( UTC_TIMESTAMP() - INTERVAL 3600 SECOND )" ],
             "GroupBy": [ "`SourceSE`", "`TargetSE`", "`Status`" ] }

  def __setattr__( self, name, value ):
    """ bweare of tpyos!!! """
    if not name.startswith( "_" ) and name not in dir( self ):
      raise AttributeError( "'%s' has no attribute '%s'" % ( self.__class__.__name__, name ) )
    try:
      object.__setattr__( self, name, value )
    except AttributeError, error:
      print name, value, error

  @property
  def SourceSE( self ):
    """ source se getter """
    return self.__data__["SourceSE"]

  @property
  def TargetSE( self ):
    """ target se getter """
    return self.__data__["TargetSE"]

  @property
  def FTSServer( self ):
    """ FTS server getter """
    return self.__data__["FTSServer"]

  @property
  def Status( self ):
    """ status getter """
    return self.__data__["Status"]

  @property
  def Files( self ):
    """ files getter """
    return self.__data__["Files"]

  @property
  def Size( self ):
    """ size getter """
    return self.__data__["Size"]

  @property
  def FailedFiles( self ):
    """ failed files getter """
    return self.__data__["FailedFiles"]

  @property
  def FailedSize( self ):
    """ failed files size getter """
    return self.__data__["FailedSize"]

  def toJSON( self ):
    """ serialize to JSON format """
    return S_OK( self.__data__ )

