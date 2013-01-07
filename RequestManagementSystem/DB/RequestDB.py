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

  @staticmethod
  def getTableMeta():
    return dict( [ ( classDef.__name__,  classDef.tableDesc() )
                   for classDef in ( Request, Operation, File ) ] ) 
    
  def _checkTables( self, force = False ):
    """ create tables if not exisiting """
    return self._createTables( self.getTableMeta(), force = force )

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
    queries = [ queries ] if type(queries) == str else queries
    ## get cursor and connection
    getCursorAndConnection = self.dictCursor( connection )
    if not getCursorAndConnection["OK"]:
      return getCursorAndConnection
    cursor = getCursorAndConnection["Value"]["cursor"]
    connection = getCursorAndConnection["Value"]["connection"]

    ## this iwll be returned as query result
    ret = { "OK" : True,
            "connection" : connection }
    queryRes = { }
    ## switch off autocommit
    connection.autocommit( False )
    try:
      ## execute queries
      for query in queries:
        cursor.execute( query )
        queryRes[query] = list( cursor.fetchall() )
      ## commit 
      connection.commit()
      ## save last row ID
      lastrowid = cursor.lastrowid
      ## close cursor
      cursor.close()
      ret["Value"] = queryRes
      ret["lastrowid"] = lastrowid
      return ret      
    except MySQLdbError, error:
      self.log.exception( error )
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
    query = "SELECT `RequestID` from `Request` WHERE `RequestName` = '%s'" % request.RequestName
    exists = self._transaction( query, connection=connection )
    if not exists["OK"]:
      self.log.error("putRequest: %s" % exists["Message"] )
      return exists

    ## save connection for furhter use
    connection = exists["connection"]
    exists = exists["Value"]

    if exists[query] and exists[query][0]["RequestID"] != request.RequestID:
      return S_ERROR("putRequest: request if '%s' already exists in the db (RequestID=%s)" % ( request.RequestName, 
                                                                                               exists[query][0]["RequestID"] ) )
    putRequest = self._transaction( request.toSQL(), connection=connection )
    if not putRequest["OK"]:
      self.log.error("putRequest: %s" % putRequest["Message"] )
      return putRequest
    lastrowid = putRequest["lastrowid"]
    putRequest = putRequest["Value"]

    ## set RequestID when necessary
    if request.RequestID == 0:
      request.RequestID = lastrowid

    for operation in request:
      putOperation = self._transaction( operation.toSQL(), connection=connection )
      if not putOperation["OK"]:
        self.log.error("putRequest: unable to put operation %d: %s" % ( request.indexOf( operation ), 
                                                                        putOperation["Message"] ) )
        deleteRequest = self.deleteRequest( request.requestName, connection=connection )
        if not deleteRequest["OK"]:
          self.log.error("putRequest: unable to delete request '%s': %s" % ( request.requestName, deleteRequest["Message"] ) )
        return putOperation
      lastrowid = putOperation["lastrowid"]
      putOperation = putOperation["Value"]
      if operation.OperationID == 0:
        operation.OperationID = lastrowid
      filesToSQL = [ opFile.toSQL() for opFile in operation ]
      if filesToSQL:
        putFiles = self._transaction( filesToSQL, connection=connection )
        if not putFiles["OK"]:
          self.log.error("putRequest: unable to put files for operation %d: %s" % ( request.indexOf( operation ),
                                                                                    putFiles["Message"] ) )
          deleteRequest = self.deleteRequest( request.requestName, connection=connection )
          return putFiles

    return S_OK()
      
  def getRequest( self, requestName=None ):
    """ read request for execution

    :param str requestName: request's name (default None)
    """
    requestID = None
    if requestName:
      self.log.info("getRequest: selecting request '%s'" % requestName )
      reqIDQuery =  "SELECT `RequestID`, `Status` FROM `Request` WHERE `RequestName` = '%s';" % str(requestName)
      reqID = self._transaction( reqIDQuery )
      if not reqID["OK"]:
        self.log.error("getRequest: %s" % reqID["Message"] )
        return reqID
      requestID = reqID["Value"][reqIDQuery][0]["RequestID"] if "RequestID" in reqID["Value"][reqIDQuery][0] else None
      status = reqID["Value"][reqIDQuery][0]["Status"] if "Status" in reqID["Value"][reqIDQuery][0] else None
      if not all(requestID, status ):
        return S_ERROR("getRequest: request '%s' not exists" % requestName )
      if requestID and status and status == "Assigned":
        return S_ERROR("getRequest: status of request '%s' is 'Assigned', request cannot be selected" % requestName )
    else:
      reqIDsQuery = "SELECT `RequestID` FROM `Request` WHERE `Status` = 'Waiting' ORDER BY `LastUpdate` ASC LIMIT 100;"
      reqIDs = self._transaction( reqIDsQuery )
      if not reqIDs["OK"]:
        self.log.error( "getRequest: %s" % reqIDs["Message"] )
        return reqIDs
      reqIDs = reqIDs["Value"][reqIDsQuery]
      
      reqIDs = [ reqID["RequestID"] for reqID in reqIDs ]
      if not reqIDs:
        return S_OK()
      random.shuffle( reqIDs )
      requestID = reqIDs[0]

    selectQuery = [ "SELECT * FROM `Request` WHERE `RequestID` = %s;" % requestID,
                    "SELECT * FROM `Operation` WHERE `RequestID` = %s;" % requestID ]
    selectReq = self._transaction( selectQuery )
    if not selectReq["OK"]:
      self.log.error("getRequest: %s" % selectReq )
    selectReq = selectReq["Value"]
    
    request = Request( selectReq[selectQuery[0]][0] )
    for records in sorted( selectReq[selectQuery[1]], key=lambda k: k["Order"]):
      ## order is ro, remove
      del records["Order"]
      operation = Operation( records )
      getFilesQuery = "SELECT * FROM `File` WHERE `OperationID` = %s;" % operation.OperationID
      getFiles = self._transaction( getFilesQuery )
      if not getFiles["OK"]:
        self.log.error("getRequest: %s" % getFiles["Message"] )
        return getFiles
      getFiles = getFiles["Value"][getFilesQuery]
      for getFile in getFiles:
        getFileDict = dict( [ (key, value ) for key, value in getFile.items() if value != None ] )
        operation.addFile( File( getFileDict ) )
      request.addOperation( operation )

    setAssigned = self._transaction( "UPDATE `Request` SET `Status` = 'Assigned' WHERE RequestID = %s;" % requestID )
    if not setAssigned["OK"]:
      self.log.error("getRequest: %s" % setAssigned["Message"] )
      return setAssigned

    return S_OK( request )  

  def deleteRequest( self, requestName, connection=None ):
    """ delete request given its name
    
    :param str requestName: request.RequestName
    :param mixed connection: connection to use if any
    """
    requestIDs = self._transaction( 
      "SELECT r.RequestID, o.OperationID FROM `Request` r LEFT JOIN `Operation` o "\
        "ON r.RequestID = o.RequestID WHERE `RequestName` = '%s'" % requestName, connection )

    if not requestIDs["OK"]:
      self.log.error("deleteRequest: unable to read RequestID and OperationIDs: %s" % requestIDs["Message"] )
      return requestIDs
    ## save connection for further use
    connection = requestIDs["connection"]
    requestIDs = requestIDs["Value"]
    trans = []
    requestID = None
    for records in requestIDs.values():
      for record in records:
        requestID = record["RequestID"] if record["RequestID"] else None
        operationID = record["OperationID"] if record["OperationID"] else None
        if operationID and requestID:
          trans.append( "DELETE FROM `File` WHERE `OperationID` = %s;" % operationID )
          trans.append( "DELETE FROM `Operation` WHERE `RequestID` = %s AND `OperationID` = %s;" % ( requestID, operationID ) )
    ## last bit: request itself
    if requestID:
      trans.append( "DELETE FROM `Request` WHERE `RequestID` = %s;" % requestID )

    delete = self._transaction( trans, connection )  
    if not delete["OK"]:
      self.log.error("deleteRequest: unable to delete request '%s': %s" % ( requestName, delete["Message"] ) )
      return delete
    return S_OK()

  def _getRequestProperties( self, requestName, columnNames ):
    """ select :columnNames: from Request table  """
    columnNames = ",".join( [ '`%s`' % str(columnName) for columnName in columnNames ] )
    query = "SELECT %s FROM `Request` WHERE RequestName = `%s`;" % ( columnNames, requestName )
    
  def _getOperationProperties( self, operationID, columnNames ):
    """ select :columnNames: from Operation table  """
    columnNames = ",".join( [ '`%s`' % str(columnName) for columnName in columnNames ] )
    query = "SELECT %s FROM `Operation` WHERE OperationID = %s;" % ( columnNames, int(operationID) )

  def _getFileProperties( self, fileID, columnNames ):
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

    



  
