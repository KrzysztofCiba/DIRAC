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
from DIRAC.DataManagementSystem.Client.FTSLfn import FTSLfn
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.Client.FTSJobFile import FTSJobFile

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
                   for classDef in ( FTSLfn, FTSJob, FTSJobFile ) ] )

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

  def putFTSLfn( self, lfnFile ):
    """ put FTSLfn to fts db """
    addFTSLfn = self._query( lfnFile.toSQL() )
    if not addFTSLfn["OK"]:
      self.log.error( addFTSLfn["Message"] )
    return addFTSLfn

  def getFTSLfn( self, fileID = None, lfn = None ):
    """ read FTSLfn from db """
    pass

  def putFTSJob( self, ftsJob ):
    """ put FTSJob to the db """
    pass

  def getFTSJob( self, status = "Submitted" ):

    pass

  def selectFTSJobFiles( self, status = "Waiting" ):
    """ select FTSJobFiles for submit """
    selectFiles = "SELECT * FROM `FTSJobFiles` WHERE `Status` = '%s'" % status;
    selectFiles = self._query( selectFiles )
    if not selectFiles["OK"]:
      self.log.error( selectFiles["Message"] )
      return selectFiles

  def _getFTSLfnProperties( self, ftsLfnID, columnNames = None ):
    """ select :columnNames: from FTSLfn table  """
    columnNames = columnNames if columnNames else [ col for col in FTSLfn.tableDesc()["Fields"] if col != "FTSLfnID" ]
    columnNames = ",".join( [ '`%s`' % str( columnName ) for columnName in columnNames ] )
    return "SELECT %s FROM `FTSLfn` WHERE `FTSLfnID` = %s;" % ( columnNames, int( ftsLfnID ) )

  def _getFTSJobProperties( self, ftsJobID, columnNames = None ):
    """ select :columnNames: from FTSJob table  """
    columnNames = columnNames if columnNames else [ col for col in FTSJob.tableDesc()["Fields"] if col != "FTSJobID" ]
    columnNames = ",".join( [ '`%s`' % str( columnName ) for columnName in columnNames ] )
    return "SELECT %s FROM `FTSJob` WHERE `FTSJobID` = %s;" % ( columnNames, int( ftsJobID ) )

  def _getFTSJobFileProperties( self, ftsJobFileID, columnNames = None ):
    """ select :columnNames: from FTSJobFile table  """
    columnNames = columnNames if columnNames else [ col for col in FTSJobFile.tableDesc()["Fields"] if col != "FTSJobFileID" ]
    columnNames = ",".join( [ '`%s`' % str( columnName ) for columnName in columnNames ] )
    return "SELECT %s FROM `FTSJobFile` WHERE `FTSJobFileID` = %s;" % ( columnNames, int( ftsJobFileID ) )
