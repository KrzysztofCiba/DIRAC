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

  def __init__( self, systemInstance = 'Default', maxQueueSize = 10 ):
    """c'tor

    :param self: self reference

    """
    self.getIdLock = threading.Lock() 
    DB.__init__( self, "ReqDB", "RequestManagement/ReqDB", maxQueueSize )
    self._checkTables( False )
    
  def _checkTables( self, force = False ):
    """ create tables if not exisiting """
    self.tableDict = dict.( [ ( classDef.__class__.__name__,  classDef.tableDesc() )
                              for classDef in ( Request, Operation, File ) ] )
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


  def _transaction( self, queries, connection=None ):
    """ execute transaction """
    queries = [ queries ] if type(queries) = str else queries
    ## get cursor and connection
    cursor = None
    if not connection:
      getCursorAndConnection = self.dictCursor( connection )
      if not getCursorAndConnection["OK"]:
        return getCursorAndConnection
      cursor = getCursorAndConnection["Value"]["cursor"]
      connection = getCursorAndConnection["Value"]["connection"]

    ## this we will return back
    ret = { "OK" : True,
            "Value" : { "connection" : connection, 
                        "lastrowid" : None } }
    ## switch off autocommit
    connection.autocommit( False )
    try:
      ## execute queries
      for query in queries:
        cursor.execute( query )
      ## commit 
      connection.commit()
      ## save last row ID
      lastrowid = cursor.lastrowid
      ## close cursor
      cursor.close()
      return S_OK( { "connection" : connection, 
                     "lastrowid" : lastrowid } )
    except MySQLdbError, error:
      ## rollback
      connection.rollback()
      ## rever autocommit
      connection.autocommit( True )
      ## close cursor
      cursor.close()
      return S_ERROR( str(error) )


  def putRequest( self, request, connection=None ):
    """ update or insert request into db 

    :param Request request: Request instance
    """      
    putRequest = self._transaction( request.toSQL(), connection=connection )
    if not putRequest["OK"]:
      self.log.error("putRequest: %s" % putRequest["Message"] )
      return putRequest
    putRequest = putRequest["Value"]
    connection = putRequest["connection"]  
    ## set RequestID when necessary
    if not request.requestID:
      request.requestID = putRequest["lastrowid"]

    for operation in request:
      putOperation = self._transaction( operation.toSQL(), connection=connection )
      if not putOperation["OK"]:
        self.log.error("putRequest: unable to put operation %d: %s" % ( request.indexOf( operation ), 
                                                                        putOperation["Message"] ) )
        deleteRequest = self.deleteRequest( requestID = request.RequestID, connection=connection )
        self.__putConnection( connection )
        return putOperation

      putOperation = putOperation["Value"]
      if not operation.operationID:
        operation.operationID = putOperation["lastrowid"]
      filesToSQL = [ opFile.toSQL() for opFile in operation ]
      if filesToSQL:
        putFiles = self._transaction( filesToSQL, connection=connection )
        if not putFiles["OK"]:
          self.log.error("putRequest: unable to put files for operation %d: %s" % ( request.indexOf( operation ),
                                                                                    putFiles["Message"] ) )
          deleteRequest = self.deleteRequest( requestID=request.RequestID, connection=connection )
          self.__putConnection( connection )
          return putFiles

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

  def deleteRequest( self, requestName=None, requestID=None, connection=None ):
    """ delete request """
    if requestName:
      

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

    



  
