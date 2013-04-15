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
from DIRAC.DataManagementSystem.Client.FTSSite import FTSSite
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.Client.FTSFile import FTSFile

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
    self.getIdLock = LockRing.getLock()
    # # max attmprt for reschedule
    self.maxAttempt = 100
    # # check tables
    self._checkTables( False )

  @staticmethod
  def getTableMeta():
    """ get db schema in a dict format """
    return dict( [ ( classDef.__name__, classDef.tableDesc() )
                   for classDef in ( FTSSite, FTSJob, FTSFile ) ] )

  def _checkTables( self, force = False ):
    """ create tables if not exisiting

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
    """ put FTSSite into fts db """
    putFTSSite = self._transaction( ftsSite.toSQL() )
    if not putFTSSite["OK"]:
      self.log.error( putFTSSite["Message"] )
    return putFTSSite

  def getFTSSite( self, ftsSiteID ):
    """ get FTSSite from fts db """
    getFTSSite = self._transaction( self._getFTSSiteProperties( ftsSiteID ) )
    if not getFTSSite["OK"]:
      self.log.error( getFTSSite["Message"] )
    getFTSSite = getFTSSite["Value"]
    return getFTSSite

  def putFTSFile( self, ftsFile ):
    """ put FTSFile into fts db """
    putFTSFile = self._transaction( ftsFile.toSQL() )
    if not putFTSFile["OK"]:
      self.log.error( putFTSFile["Message"] )

    return putFTSFile

  def getFTSFile( self, fileID = None, lfn = None ):
    """ read FTSFile from db """
    if not any( fileID, lfn ):
      return S_ERROR( "Missing fileID of lfn argument" )

  def putFTSJob( self, ftsJob ):
    """ put FTSJob to the db

    :param FTSJob ftsJob: FTSJob instance
    """
    putJob = [ ftsJob.toSQL() ] + [ ftsFile.toSQL() for ftsFile in ftsJob ]
    putJob = self._transaction( putJob )
    if not putJob["OK"]:
      self.log.error( putJob["Message"] )
    return putJob

  def getFTSJob( self, ftsJobID ):
    """ get FTSJob given FTSJobID """
    getFTSJob = self._transaction( self._getFTSJobProperties( ftsJobID ) )
    if not getFTSJob["OK"]:
      self.log.error( getFTSJob["Message"] )
      return getFTSJob
    getFTSJob = getFTSJob["Value"]
    connection = getFTSJob["connection"]



  def selectFTSFiles( self, status = "Waiting" ):
    """ select FTSJobFiles for submit """
    selectFiles = "SELECT * FROM `FTSFile` WHERE `Status` = '%s'" % status;
    selectFiles = self._query( selectFiles )
    if not selectFiles["OK"]:
      self.log.error( selectFiles["Message"] )
      return selectFiles

  def _getFTSSiteProperties( self, ftsSiteID, columnNames = None ):
    """ select :columnNames: from FTSSite table  """
    columnNames = columnNames if columnNames else FTSSite.tableDesc()["Fields"].keys()
    columnNames = ",".join( [ '`%s`' % str( columnName ) for columnName in columnNames ] )
    return "SELECT %s FROM `FTSSite` WHERE `FTSSiteID` = %s;" % ( columnNames, int( ftsSiteID ) )

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
