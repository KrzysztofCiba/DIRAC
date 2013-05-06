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
import MySQLdb.cursors
from MySQLdb import Error as MySQLdbError
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities.LockRing import LockRing
from DIRAC.Core.Utilities.List import stringListToString
# # ORMs
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.Client.FTSFile import FTSFile
from DIRAC.DataManagementSystem.private.FTSHistoryView import FTSHistoryView

########################################################################
class FTSDB( DB ):
  """
  .. class:: FTSDB

  database holding FTS jobs and their files
  """

  def __init__( self, systemInstance = "Default", maxQueueSize = 10 ):
    """c'tor

    :param self: self reference
    :param str systemInstance: ???
    :param int maxQueueSize: size of queries queue
    """
    DB.__init__( self, "FTSDB", "DataManagement/FTSDB", maxQueueSize )
    self.log = gLogger.getSubLogger( "DataManagement/FTSDB" )
    # # private lock
    self.getIdLock = LockRing().getLock( "FTSDBLock" )
    # # max attempt for reschedule
    self.maxAttempt = 100
    # # check tables
    self._checkTables( False )
    self._checkViews( False )

  @staticmethod
  def getTableMeta():
    """ get db schema in a dict format """
    return dict( [ ( classDef.__name__, classDef.tableDesc() )
                   for classDef in ( FTSJob, FTSFile ) ] )

  @staticmethod
  def getViewMeta():
    """ return db views in dict format

    at the moment only one view - FTSHistoryView
    """
    return { FTSHistoryView.__name__: FTSHistoryView.viewDesc() }

  def _checkViews( self, force = False ):
    """ create views """
    return self._createViews( self.getViewMeta(), force )

  def _checkTables( self, force = False ):
    """ create tables if not existing

    :param bool force: flag to trigger recreation of db schema
    """
    return self._createTables( self.getTableMeta(), force = force )

  def dictCursor( self, conn = None ):
    """ get dict cursor for connection :conn:

    :return: S_OK( { "cursor": MySQLdb.cursors.DictCursor, "connection" : connection  } ) or S_ERROR
    """
    if not conn:
      retDict = self._getConnection()
      if not retDict["OK"]:
        return retDict
      conn = retDict["Value"]
    cursor = conn.cursor( cursorclass = MySQLdb.cursors.DictCursor )
    return S_OK( { "cursor" : cursor, "connection" : conn  } )

  def _transaction( self, queries, connection = None ):
    """ execute transaction """
    queries = [ queries ] if type( queries ) == str else queries
    # # get cursor and connection
    getCursorAndConnection = self.dictCursor( connection )
    if not getCursorAndConnection["OK"]:
      return getCursorAndConnection
    cursor = getCursorAndConnection["Value"]["cursor"]
    connection = getCursorAndConnection["Value"]["connection"]

    # # this iwll be returned as query result
    ret = { "OK" : True,
            "connection" : connection }
    queryRes = { }
    # # switch off autocommit
    connection.autocommit( False )
    try:
      # # execute queries
      for query in queries:
        cursor.execute( query )
        queryRes[query] = list( cursor.fetchall() )
      # # commit
      connection.commit()
      # # save last row ID
      lastrowid = cursor.lastrowid
      # # close cursor
      cursor.close()
      ret["Value"] = queryRes
      ret["lastrowid"] = lastrowid
      return ret
    except MySQLdbError, error:
      self.log.exception( error )
      # # roll back
      connection.rollback()
      # # revert auto commit
      connection.autocommit( True )
      # # close cursor
      cursor.close()
      return S_ERROR( str( error ) )

  def putFTSFile( self, ftsFile ):
    """ put FTSFile into fts db """
    ftsFileSQL = ftsFile.toSQL()
    if not ftsFileSQL["OK"]:
      self.log.error( ftsFileSQL["Message"] )
      return ftsFileSQL
    ftsFileSQL = ftsFileSQL["Value"]
    putFTSFile = self._transaction( ftsFileSQL )
    if not putFTSFile["OK"]:
      self.log.error( putFTSFile["Message"] )
    return putFTSFile

  def getFTSFile( self, fileID = None, lfn = None ):
    """ read FTSFile from db """
    if not any( fileID, lfn ):
      return S_ERROR( "Missing fileID of lfn argument" )


  def deleteFTSFile( self, ftsFileID ):
    """ delete FTSFile given FTSFileID """
    pass

  def putFTSJob( self, ftsJob ):
    """ put FTSJob to the db

    :param FTSJob ftsJob: FTSJob instance
    """
    ftsJobSQL = ftsJob.toSQL()
    if not ftsJobSQL["OK"]:
      return ftsJobSQL
    putJob = [ ftsJobSQL["Value"] ]

    gLogger.always( putJob )

    for ftsFile in [ ftsFile.toSQL() for ftsFile in ftsJob ]:
      if not ftsFile["OK"]:
        return ftsFile
      putJob.append( ftsFile["Value"] )

    putJob = self._transaction( putJob )
    if not putJob["OK"]:
      self.log.error( putJob["Message"] )
    return putJob

  def getFTSJob( self, ftsJobID = None, readOnly = False ):
    """ get FTSJob given FTSJobID """
    getFTSJob = self._transaction( self._getFTSJobProperties( ftsJobID ) )
    if not getFTSJob["OK"]:
      self.log.error( getFTSJob["Message"] )
      return getFTSJob
    getFTSJob = getFTSJob["Value"]
    connection = getFTSJob["connection"]

  def peekFTSJob( self, ftsJobID = None ):
    """ read FTSJob given FTSJobID """
    return self.getFTSJob( ftsJobID, readOnly = True )

  def deleteFTSJob( self, ftsJobID ):
    """ delete FTSJob given ftsJobID """
    pass

  def getFTSJobIDs( self, statusList = [ "Submitted", "Active", "Ready" ] ):
    """ get FTSJobIDs for  a given status list """
    query = "SELECT `FTSJobID` FROM `FTSJob` WHERE `Status` IN (%s);" % stringListToString( statusList )
    query = self._query( query )
    gLogger.always( query )



  def getFTSFiles( self, status = "Waiting" ):
    """ select FTSFiles for submit """
    selectFiles = "SELECT * FROM `FTSFile` WHERE `Status` = '%s';" % status;
    selectFiles = self._transaction( selectFiles )
    if not selectFiles["OK"]:
      self.log.error( selectFiles["Message"] )
      return selectFiles

  def getFTSHistory( self ):
    """ query FTSHistoryView, return list of FTSHistoryViews """
    query = self._transaction( [ "SELECT * FROM `FTSHistoryView`;" ] )
    if not query["OK"]:
      return query
    if not query["Value"]:
      return S_OK()
    return S_OK( [ FTSHistoryView( fromDict ) for fromDict in query["Value"].values()[0] ] )

  def getDBSummary( self ):
    """ get DB summary """
    # # this will be returned
    retDict = { "FTSJob": {}, "FTSFile": {}, "FTSHistory": {} }
    transQueries = { "SELECT `Status`, COUNT(`Status`) FROM `FTSJob` GROUP BY `Status`;" : "FTSJob",
                "SELECT `Status`, COUNT(`Status`) FROM `FTSFile` GROUP BY `Status`;" : "FTSFile",
                "SELECT * FROM `FTSHistoryView`;" : "FTSHistory" }
    ret = self._transaction( transQueries.keys() )
    if not ret["OK"]:
      self.log.error( "getDBSummary: %s" % ret["Message"] )
      return ret
    ret = ret["Value"]
    for k, v in ret.items():
      if transQueries[k] == "FTSJob":
        for aDict in v:
          status = aDict.get( "Status" )
          count = aDict.get( "COUNT(`Status`)" )
          if status not in retDict["FTSJob"]:
            retDict["FTSJob"][status] = 0
          retDict["FTSJob"][status] += count
      elif transQueries[k] == "FTSFile":
        for aDict in v:
          status = aDict.get( "Status" )
          count = aDict.get( "COUNT(`Status`)" )
          if status not in retDict["FTSFile"]:
            retDict["FTSFile"][status] = 0
          retDict["FTSFile"][status] += count
      else:  # # FTSHistory
        retDict["FTSHistory"] = v
    return S_OK( retDict )

  def _getFTSJobProperties( self, ftsJobID, columnNames = None ):
    """ select :columnNames: from FTSJob table  """
    columnNames = columnNames if columnNames else FTSJob.tableDesc()["Fields"].keys()
    columnNames = ",".join( [ '`%s`' % str( columnName ) for columnName in columnNames ] )
    return "SELECT %s FROM `FTSJob` WHERE `FTSJobID` = %s;" % ( columnNames, int( ftsJobID ) )

  def _getFTSFileProperties( self, ftsFileID, columnNames = None ):
    """ select :columnNames: from FTSJobFile table  """
    columnNames = columnNames if columnNames else FTSFile.tableDesc()["Fields"].keys()
    columnNames = ",".join( [ '`%s`' % str( columnName ) for columnName in columnNames ] )
    return "SELECT %s FROM `FTSFile` WHERE `FTSFileID` = %s;" % ( columnNames, int( ftsFileID ) )

  def _getFTSHistoryProperties( self, columnNames = None ):
    """ select :columnNames: from FTSHistory view """
    columnNames = columnNames if columnNames else FTSHistoryView.viewDesc()["Fields"].keys()
    return "SELECT %s FROM `FTSHistoryView`;" % ",".join( columnNames )
