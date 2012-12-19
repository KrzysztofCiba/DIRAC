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

    db holding Requests, Operations and their Files 
"""
__RCSID__ = "$Id $"
##
# @file RequestDB.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/12/04 08:06:51
# @brief Definition of RequestDB class.

## imports 
import random
import threading
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
  tableDict = { "Request" : 
                { "Fields" : 
                  { "RequestID" : "INTEGER NOT NULL AUTO_INCREMENT",
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
                "Operation" : 
                { "Fields" : 
                  { "OperationID" : "INTEGER NOT NULL AUTO_INCREMENT",
                    "RequestID" : "INTEGER NOT NULL",
                    "Type" : "VARCHAR(64) NOT NULL",
                    "Status" : "ENUM('Waiting', 'Assigned', 'Queued', 'Done', 'Failed', 'Cancelled') "\
                      "DEFAULT 'Queued'",
                    "Arguments" : "BLOB",
                    "Order" : "INTEGER NOT NULL",
                    "SourceSE" : "VARCHAR(255)",
                    "TargetSE" : "VARCHAR(255)",
                    "Catalogue" : "VARCHAR(255)",
                    "CreationTime" : "DATETIME",
                    "SubmitTime" : "DATETIME",
                    "LastUpdate" : "DATETIME" },
                  "PrimaryKey" : "OperationID" },
                "File" : 
                { "Fields" : { "FileID" : "INTEGER NOT NULL AUTO_INCREMENT",
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
    self.getIdLock = threading.Lock() 
    DB.__init__( self, "ReqDB", "RequestManagement/ReqDB", maxQueueSize )
    self._checkTables( False )
    
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

  def putRequest( self, request ):
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
      cursor.execute( "SELECT `RequestID` FROM `Request` WHERE `Status` = 'Waiting' ORDER BY `LastUpdate` ASC LIMIT 100;" ) 
      requestIDs = [ record["RequestID"] for record in cursor.fetchall() ] 
      ## no waiting requests found
      if not requestIDs:
        return S_OK()
      random.shuffle( requestIDs )
      requestID = requestIDs[0]
      
      select = cursor.execute( "SELECT * FROM `Request` WHERE `RequestID` = %s;" % requestID )
      
      update = cursor.execute( "UPDATE `Request` SET `Status` = 'Assigned' WHERE `RequestID` = %s;" % reuqestID )


      
      
    except MySQLdbError, error:
      connection.rollback()
      connection.autocommit(True)
      cursor.close()
      self.log.error( "getRequest: unable to get request: %s" % str(error) )
      return S_ERROR( "getRequest: %s" % str(error) )
    return S_OK()

  def deleteRequest( self, requestName ):
    """ delete request """
    

    pass

  def getRequestProperties( self, requestName, columnNames ):
    """ select :columnNames: from Request table  """
    columnNames = ",".join( [ '`%s`' % str(columnName) for columnName in columnNames ] )
    query = "SELECT %s FROM `Request` WHERE RequestName = `%s`;" % ( columnNames, requestName )
    

  def getOperationProperties( self, operationID, columnNames ):
    """ select :columnNames: from Operation table  """
    columnNames = ",".join( [ '`%s`' % str(columnName) for columnName in columnNames ] )
    query = "SELECT %s FROM `Operation` WHERE OperationID = %s;" % ( columnNames, int(operationID) )

  def getFileProperties( self, fileID, columnNames ):
    """ select :columnNames: from File table  """
    columnNames = ",".join( [ '`%s`' % str(columnName) for columnName in columnNames ] )
    query = "SELECT %s FROM `File` WHERE FileID = %s;" % ( columnNames, int(fileID) )

  def getDBSummary( self ):
    """ get db summary """
    pass

  def getRequestSummaryWeb( self, selectDict, sortList, startItem, maxItems ):
    """ get db summary for web """
    pass

  def getRequestForJobs( self, jobIDs ):
    """ read request """
    pass

  def readRequestsForJobs( self, jobIDs=None ):
    """ read request for jobs """
    pass

    



  
