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
import MySQLdb.cursors
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
                                           "LastUpdate" : "DATETIME" },
                              "PrimaryKey" : "RequestID",
                              "Indexes" : "RequestName" : [ "RequestName"]  },
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
                                        "Checksum" : "VARCGAR(255)",
                                        "GUID" : "VARCHAR(26)",
                                        "Error" : "VARCHAR(255)" },
                           "PrimaryKey" : "FileID",
                           "Indexes" : "LFN" : [ "LFN" ] } } 

  def __init__( self, systemInstance = 'Default', maxQueueSize = 10 ):
    """c'tor

    :param self: self reference

    """
    DB.__init__( self, 'RequestDB', 'RequestManagement/RequestDB', maxQueueSize )
    self.getIdLock = threading.Lock()
    self._checkTables()

  def _checkTables( self, force = False ):
    """ create tables if not exisiting """
    return self._createTables( self.tableDict, force = force )

  def dictCursor( self, conn=None ):
    """ get dict cursor for connection :conn:

    :return: S_OK( { "cursor": MySQLdb.cursors.DictCursor, "conn" : connection  } ) or S_ERROR
    """
    if not conn:
      retDict = self._getConnection()
      if not retDict["OK"]:
        return retDict
      conn = retDict["Value"]
    cursor = conn.cursor( cursorclass = MySQLdb.cursors.DictCursor )
    return S_OK( { "cursor" : cursor, "conn" : conn  } )

  def transaction( self, cmdList, conn=None, cursorType=MySQLdb.cursors.DictCursor, dropConnection=False ):
    """ transaction using dict cursor """

    if type( cmdList ) != ListType:
      return S_ERROR( "transaction: wrong type (%s) for cmdList" % type( cmdList ) )

    ## get connection 
    if not conn:
      retDict = self._getConnection()
      if not retDict["OK"]:
        return retDict
      conn = retDict["Value"]

    ## list with cmds and their results   
    cmdRet = []
    try:
      cursor = conn.cursor( cursorclass=cursorType )
      for cmd in cmdList:
        cmdRet.append( ( cmd, cursor.execute( cmd ) ) )
      conn.commit()
      cursor.close()
      ret = S_OK( cmdRet )
      if dropConnection:
        self.__putConnection( conn )
      else:
        ret["conn"] = conn
      return ret
    except Exception, error:
      self.log.execption( error )
      ## rollback, put back connection to the pool 
      conn.rollback()
      self.__putConnection( conn )
      return S_ERROR( error )


    def putRequest( self, request ):
      """ update or insert request into db """      
      cursor = self.dictCursor()
      if not cursor["OK"]:
        self.log.error("putRequest: %s" % cursor["Message"] )
      cursor = cursor["Value"]["cursor"]

      transaction = [ request.toSQL() ]
      

    def getRequest( self ):
      pass

    def deleteRequest( self, request ):
      pass

    

    def getDBSummary( self ):
      pass

    def readRequests( self ):
      pass

    



  
