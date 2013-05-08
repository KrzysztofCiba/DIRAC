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
from DIRAC.DataManagementSystem.Client.FTSSite import FTSSite
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
                   for classDef in ( FTSSite, FTSJob, FTSFile ) ] )
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

  def putFTSSite( self, ftsSite ):
    """ put FTS site into DB """
    ftsSiteSQL = ftsSite.toSQL()
    if not ftsSiteSQL["OK"]:
      self.log.error( "putFTSSite: %s" % ftsSiteSQL["Message"] )
      return ftsSiteSQL
    ftsSiteSQL = ftsSiteSQL["Value"]
    putFTSSite = self._transaction( ftsSiteSQL )
    if not putFTSSite["OK"]:
      self.log.error( putFTSSite["Message"] )
    return putFTSSite

  def getFTSSite( self, ftsSiteID ):
    """ read FTSSite given FTSSiteID """
    getFTSSiteQuery = "SELECT * FROM `FTSSite` WHERE `FTSSiteID`=%s" % int( ftsSiteID )
    getFTSSite = self._transaction( [ getFTSSiteQuery ] )
    if not getFTSSite["OK"]:
      self.log.error( "getFTSSite: %s" % getFTSSite["Message"] )
      return getFTSSite
    getFTSSite = getFTSSite["Value"]
    if getFTSSiteQuery in getFTSSite and getFTSSite[getFTSSiteQuery]:
      getFTSSite = FTSSite( getFTSSite[getFTSSiteQuery][0] )
      return S_OK( getFTSSite )
    # # if we land here FTSSite does nor exist
    return S_OK()

  def getFTSSitesList( self ):
    """ bulk read of FTS sites """
    ftsSites = self._transaction( "SELECT * FROM `FTSSites`;" )
    if not ftsSites["OK"]:
      self.log.error( "getFTSSites: %s" % ftsSites["Message"] )
      return ftsSites
    ftsSites = ftsSites["Value"]
    self.log.always( ftsSites )
    return S_OK()

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

  def getFTSFile( self, ftsFileID ):
    """ read FTSFile from db given FTSFileID """
    select = "SELECT * FROM `FTSFile` WHERE `FTSFileID` = %s;" % ftsFileID
    select = self._transaction( [ select ] )
    if not select["OK"]:
      self.log.error( select["Message"] )
      return select
    select = select["Value"]
    if not select.values()[0]:
      return S_OK()
    ftsFile = FTSFile( select.values()[0][0] )
    return S_OK( ftsFile )

  def peekFTSFile( self, ftsFileID ):
    """ peek FTSFile given FTSFileID """
    return self.getFTSFile( ftsFileID )

  def deleteFTSFile( self, ftsFileID ):
    """ delete FTSFile given FTSFileID """
    delete = "DELETE FROM `FTSFile` WHERE `FTSFileID` = %s;" % ftsFileID
    delete = self._transaction( [ delete ] )
    if not delete["OK"]:
      self.log.error( delete["Message"] )
    return delete

  def putFTSJob( self, ftsJob ):
    """ put FTSJob to the db (INSERT or UPDATE)

    :param FTSJob ftsJob: FTSJob instance
    """
    ftsJobSQL = ftsJob.toSQL()
    if not ftsJobSQL["OK"]:
      return ftsJobSQL
    putJob = [ ftsJobSQL["Value"] ]

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

    getJob = [ "SELECT * FROM `FTSJob` WHERE `FTSJobID` = %s;" % ftsJobID ]
    getJob = self._transaction( getJob )
    if not getJob["OK"]:
      self.log.error( getJob["Message"] )
      return getJob
    getJob = getJob["Value"]
    if not getJob:
      return S_OK()
    ftsJob = FTSJob( getJob.values()[0][0] )
    selectFiles = self._transaction( [ "SELECT * FROM `FTSFile` WHERE `FTSGUID` = '%s';" % ftsJob.FTSGUID ] )
    if not selectFiles["OK"]:
      self.log.error( selectFiles["Message"] )
      return selectFiles
    selectFiles = selectFiles["Value"]
    ftsFiles = [ FTSFile( item ) for item in selectFiles.values()[0] ]
    for ftsFile in ftsFiles:
      ftsJob.addFile( ftsFile )

    # # TODO: re-think if we need this one
    # if not readOnly:
    #  setAssigned = "UPDATE `FTSJob` SET `Status`='Assigned' WHERE `FTSJobID` = %s;" % ftsJobID
    #  setAssigned = self._query( setAssigned )
    #  if not setAssigned["OK"]:
    #    self.log.error( setAssigned["Message"] )
    #    return setAssigned

    return S_OK( ftsJob )

  def peekFTSJob( self, ftsJobID = None ):
    """ read FTSJob given FTSJobID """
    return self.getFTSJob( ftsJobID, readOnly = True )

  def deleteFTSJob( self, ftsJobID ):
    """ delete FTSJob given ftsJobID """
    delete = "DELETE FROM `FTSJob` WHERE `FTSJobID` = %s;" % ftsJobID
    delete = self._transaction( [ delete ] )
    if not delete["OK"]:
      self.log.error( delete["Message"] )
    return delete

  def getFTSJobIDs( self, statusList = [ "Submitted", "Active", "Ready" ] ):
    """ get FTSJobIDs for  a given status list """
    query = "SELECT `FTSJobID` FROM `FTSJob` WHERE `Status` IN (%s);" % stringListToString( statusList )
    query = self._query( query )
    if not query["OK"]:
      self.log.error( query["Message"] )
      return query
    # # convert to list of longs
    return S_OK( [ item[0] for item in query["Value"] ] )

  def getFTSFileIDs( self, statusList = [ "Waiting" ] ):
    """ select FTSFileIDs for a given status list """
    query = "SELECT * FROM `FTSFile` WHERE `Status` IN (%s);" % stringListToString( statusList );
    query = self._query( query )
    if not query["OK"]:
      self.log.error( query["Message"] )
      return query
    return S_OK( [ item[0] for item in query["Value"] ] )

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
