########################################################################
# $HeadURL $
# File: RequestDB.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/12/04 08:06:30
########################################################################
""" :mod: RequestDB 
    =======================
 
    .. module: RequestDB
    :synopsis: db holding Requests 
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    db holding Requests 
"""
__RCSID__ = "$Id $"
##
# @file RequestDB.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/12/04 08:06:51
# @brief Definition of RequestDB class.

## imports 
import random
import MySQLdb.cursors
from MySQLdb import Error as MySQLdbError
## from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File

########################################################################
class RequestDB(DB):
  """
  .. class:: RequestDB

  persistency storage for requests
  """
  tableDict = { "Request" : { "Fields" : { "RequestID" : "INTEGER NOT NULL AUTO_INCREMENT",
                                           "RequestName" : "VARCHAR(255) NOT NULL",
                                           "OwnerDN" : "VARCHAR(255)",
                                           "OwnerGroup" : "VARCHAR(32)",
                                           "Status" : "ENUM('Waiting', 'Assigned', 'Done', 'Failed', 'Cancelled') DEFAULT 'Waiting'",
                                           "Error" : "VARCHAR(255)",
                                           "DIRACSetup" : "VARCHAR(32)",
                                           "JobID" : "INTEGER DEFAULT 0",
                                           "CreationTime" : "DATETIME",
                                           "SubmitTime" : "DATETIME",
                                           "LastUpdate" : "DATETIME"  },
                              "PrimaryKey" : "RequestID",
                              "Indexes" : { "RequestName" : [ "RequestName"] } },
                "Operation" : { "Fields" : { "OperationID" : "INTEGER NOT NULL AUTO_INCREMENT",
                                             "RequestID" : "INTEGER NOT NULL",
                                             "Type" : "VARCHAR(64) NOT NULL",
                                             "Status" : "ENUM('Waiting', 'Assigned', 'Done', 'Failed', 'Cancelled') DEFAULT 'Waiting'",
                                             "Arguments" : "BLOB",
                                             "Order" : "INTEGER NOT NULL",
                                             "SourceSE" : "VARCHAR(255)",
                                             "TargetSE" : "VARCHAR(255)",
                                             "Catalogue" : "VARCHAR(255)",
                                             "CreationTime" : "DATETIME",
                                             "SubmitTime" : "DATETIME",
                                             "LastUpdate" : "DATETIME" },
                                "PrimaryKey" : "OperationID" },
                "File" : { "Fields" : { "FileID" : "INTEGER NOT NULL AUTO_INCREMENT",
                                        "OperationID" : "INTEGER NOT NULL",
                                        "Status" : "ENUM('Waiting', 'Done', 'Failed', 'Scheduled', 'Cancelled')",
                                        "LFN" : "VARCHAR(255)",
                                        "PFN" : "VARCHAR(255)",
                                        "ChecksumType" : "ENUM('adler32', 'md5', 'sha1', 'none') DEFAULT 'adler32'",
                                        "Checksum" : "VARCHAR(255)",
                                        "GUID" : "VARCHAR(26)",
                                        "Error" : "VARCHAR(255)" },
                           "PrimaryKey" : "FileID",
                           "Indexes" : { "LFN" : [ "LFN" ] } } } 

  def __init__( self, systemInstance = 'Default', maxQueueSize = 10 ):
    """c'tor

    :param self: self reference

    """
    DB.__init__( self, "RequestDB", "RequestManagement/RequestDB", maxQueueSize )
    self.getIdLock = threading.Lock()
    self._checkTables()

  def _checkTables( self, force = False ):
    """ create tables if not exisiting """
    return self._createTables( self.tableDict, force = force )

  def dictCursor( self, conn=None ):
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


  def setRequest( self, request ):
    """ update or insert request into db """      
    cursor = self.dictCursor()
    if not cursor["OK"]:
      self.log.error("setRequest: %s" % cursor["Message"] )
    cursor = cursor["Value"]["cursor"]
    connection = cursor["Value"]["connection"]
    connection.autocommit( False )
    try:
      cursor.execute( request.toSQL() )
      if not request.requestID:
        request.requestID = cursor.lastrowid
        for operation in request:
          cursor.execute( operation.toSQL() )
          if not operation.operationID:
            operation.operationID = cursor.lastrowid
            for opFile in operation:
              cursor.execute( opFile.toSQL() )
      connection.commit()
      cursor.close()
    except MySQLdbError, error:
      connection.rollback()
      connection.autocommit(True)
      cursor.close()
      self.log.error( "setRequest: unable to put request: %s" % str(error) )
      return S_ERROR( "setRequest: %s" % str(error) )
    
    return S_OK()
      
    def getRequest( self ):
      """ read request for execution """
      cursor = self.dictCursor()
      if not cursor["OK"]:
        self.log.error("putRequest: %s" % cursor["Message"] )
      cursor = cursor["Value"]["cursor"]
      connection = cursor["Value"]["connection"]
      connection.autocommit( False )
      try:
        cursor.execute( "SELECT RequestID FROM Requests WHERE Status = 'Waiting' ORDER BY LastUpdate ASC LIMIT 100;" ) 
        requestIDs = cursor.fetchall()
        
      except MySQLdbError, error:
        connection.rollback()
        connection.autocommit(True)
        cursor.close()
        self.log.error( "getRequest: unable to get request: %s" % str(error) )
        return S_ERROR( "getRequest: %s" % str(error) )
      return S_OK()

        
    def deleteRequest( self, request ):

      pass


    def getDBSummary( self ):
      
      pass

    def readRequests( self, jobIDs=None ):

      pass

    



  
