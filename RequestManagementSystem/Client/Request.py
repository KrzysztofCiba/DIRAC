########################################################################
# $HeadURL $
# File: Request.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/07/16 13:43:45
########################################################################
""" :mod: Request
    =============

    .. module: Request
    :synopsis: request implementation
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    request implementation
"""
# for properties
# pylint: disable=E0211,W0612,W0142
__RCSID__ = "$Id$"
# #
# @file Request.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/07/16 13:44:00
# @brief Definition of Request class.

# # imports
import datetime
try:
  import xml.etree.cElementTree as ElementTree
except ImportError:
  import xml.etree.ElementTree as ElementTree
from xml.parsers.expat import ExpatError
# # from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.TypedList import TypedList
from DIRAC.RequestManagementSystem.Client.Operation import Operation

########################################################################
class Request( object ):
  """
  .. class:: Request

  :param int RequestID: requestID
  :param str Name: request' name
  :param str OwnerDN: request's owner DN
  :param str OwnerGroup: request owner group
  :param str Setup: DIRAC setup
  :param str SourceComponent: whatever
  :param int JobID: jobID
  :param datetime.datetime CreationTime: UTC datetime
  :param datetime.datetime SubmissionTime: UTC datetime
  :param datetime.datetime LastUpdate: UTC datetime
  :param str Status: request's status
  :param TypedList operations: list of operations
  """

  def __init__( self, fromDict = None ):
    """c'tor

    :param self: self reference
    """
    self.__waiting = None
    self.__data__ = dict.fromkeys( self.tableDesc()["Fields"].keys(), None )
    now = datetime.datetime.utcnow().replace( microsecond = 0 )
    self.__data__["CreationTime"] = now
    self.__data__["SubmitTime"] = now
    self.__data__["LastUpdate"] = now
    self.__data__["Status"] = "Waiting"
    self.__data__["JobID"] = 0
    self.__data__["RequestID"] = 0
    self.__operations__ = TypedList( allowedTypes = Operation )
    fromDict = fromDict if fromDict else {}
    for opDict in fromDict.get( "Operations", [] ):
      self.addOperation( opDict )
    if "Operations" in fromDict: del fromDict["Operations"]
    for key, value in fromDict.items():
      if key not in self.__data__:
        raise AttributeError( "Unknown Request attribute '%s'" % key )
      if value:
        setattr( self, key, value )

  @staticmethod
  def tableDesc():
    """ get table desc """
    return { "Fields" :
             { "RequestID" : "INTEGER NOT NULL AUTO_INCREMENT",
               "RequestName" : "VARCHAR(255) NOT NULL",
               "OwnerDN" : "VARCHAR(255)",
               "OwnerGroup" : "VARCHAR(32)",
               "Status" : "ENUM('Waiting', 'Assigned', 'Done', 'Failed', 'Cancelled') DEFAULT 'Waiting'",
               "Error" : "VARCHAR(255)",
               "DIRACSetup" : "VARCHAR(32)",
               "SourceComponent" : "BLOB",
               "JobID" : "INTEGER DEFAULT 0",
               "CreationTime" : "DATETIME",
               "SubmitTime" : "DATETIME",
               "LastUpdate" : "DATETIME"  },
             "PrimaryKey" : [ "RequestID", "RequestName" ],
             "Indexes" : { "RequestName" : [ "RequestName"] } }

  def _notify( self ):
    """ simple state machine for sub request statuses """
    self.__waiting = None
    # # update operations statuses
    for operation in self:
      status = operation.Status
      if status in ( "Done", "Failed" ):
        continue
      elif status == "Queued" and not self.__waiting:
        operation._setWaiting( self )  # Status = "Waiting" ## this is 1st queued, flip to waiting
        self.__waiting = operation
      elif status == "Waiting":
        if self.__waiting != None:
          operation._setQueued( self )  #  Status = "Queued" ## flip to queued, another one is waiting
        else:
          self.__waiting = operation

    # now update self status
    if "Queued" in self.subStatusList() or "Waiting" in self.subStatusList():
      if self.Status != "Waiting":
        self.Status = "Waiting"
    # elif "Scheduled" in self.subStatusList():
    #  self.Status = "Scheduled"
    else:
      self.Status = "Done"

  def getWaiting( self ):
    """ get waiting operation if any """
    # # update states
    self._notify()
    return S_OK( self.__waiting )

  # # Operation aritmetics
  def __contains__( self, operation ):
    """ in operator

    :param self: self reference
    :param Operation subRequest: a subRequest
    """
    return bool( operation in self.__operations__ )

  def __add__( self, operation ):
    """ + operator for subRequest

    :param self: self reference
    :param Operation operation: sub-request to add
    """
    if operation not in self:
      self.__operations__.append( operation )
      operation._parent = self
      self._notify()
    return S_OK()

  def insertBefore( self, newOperation, existingOperation ):
    """ insert :newOperation: just before :existingOperation:

    :param self: self reference
    :param Operation newOperation: Operation to be inserted
    :param Operation existingOperation: previous Operation sibling
    """
    if existingOperation not in self:
      return S_ERROR( "%s is not in" % existingOperation )
    if newOperation in self:
      return S_ERROR( "%s is already in" % newOperation )
    self.__operations__.insert( self.__operations__.index( existingOperation ), newOperation )
    newOperation._parent = self
    self._notify()
    return S_OK()

  def insertAfter( self, newOperation, existingOperation ):
    """ insert :newOperation: just after :existingOperation:

    :param self: self reference
    :param Operation newOperation: Operation to be inserted
    :param Operation existingOperation: next Operation sibling
    """
    if existingOperation not in self:
      return S_ERROR( "%s is not in" % existingOperation )
    if newOperation in self:
      return S_ERROR( "%s is already in" % newOperation )
    self.__operations__.insert( self.__operations__.index( existingOperation ) + 1, newOperation )
    newOperation._parent = self
    self._notify()
    return S_OK()

  def addOperation( self, operation ):
    """ add :operation: to list of Operations

    :param self: self reference
    :param Operation operation: Operation to be inserted
    """
    if operation not in self:
      self +operation
    return S_OK()

  def __iter__( self ):
    """ iterator for sub-request """
    return self.__operations__.__iter__()

  def __getitem__( self, i ):
    """ [] op for sub requests """
    return self.__operations__.__getitem__( i )

  def indexOf( self, subReq ):
    """ return index of subReq (execution order) """
    return self.__operations__.index( subReq ) if subReq in self else -1

  def __len__( self ):
    """ nb of subRequests """
    return len( self.__operations__ )

  def subStatusList( self ):
    """ list of statuses for all subRequest """
    return [ subReq.Status for subReq in self ]

  # # properties

  @property
  def RequestID( self ):
    """ request ID getter """
    return self.__data__["RequestID"]

  @RequestID.setter
  def RequestID( self, value ):
    """ requestID setter (shouldn't be RO???) """
    self.__data__["RequestID"] = long( value ) if value else 0

  @property
  def RequestName( self ):
    """ request's name getter """
    return self.__data__["RequestName"]

  @RequestName.setter
  def RequestName( self, value ):
    """ request name setter """
    if type( value ) != str:
      raise TypeError( "RequestName should be a string" )
    self.__data__["RequestName"] = value[:128]

  @property
  def OwnerDN( self ):
    """ request owner DN getter """
    return self.__data__["OwnerDN"]

  @OwnerDN.setter
  def OwnerDN( self, value ):
    """ request owner DN setter """
    if type( value ) != str:
      raise TypeError( "OwnerDN should be a string!" )
    self.__data__["OwnerDN"] = value

  @property
  def OwnerGroup( self ):
    """ request owner group getter  """
    return self.__data__["OwnerGroup"]

  @OwnerGroup.setter
  def OwnerGroup( self, value ):
    """ request owner group setter """
    if type( value ) != str:
      raise TypeError( "OwnerGroup should be a string!" )
    self.__data__["OwnerGroup"] = value

  @property
  def DIRACSetup( self ):
    """ DIRAC setup getter  """
    return self.__data__["DIRACSetup"]

  @DIRACSetup.setter
  def DIRACSetup( self, value ):
    """ DIRAC setup setter """
    if type( value ) != str:
      raise TypeError( "setup should be a string!" )
    self.__data__["DIRACSetup"] = value

  @property
  def SourceComponent( self ):
    """ source component getter  """
    return self.__data__["SourceComponent"]

  @SourceComponent.setter
  def SourceComponent( self, value ):
    """ source component setter """
    if type( value ) != str:
      raise TypeError( "Setup should be a string!" )
    self.__data__["SourceComponent"] = value

  @property
  def JobID( self ):
    """ jobID getter """
    return self.__data__["JobID"]

  @JobID.setter
  def JobID( self, value = 0 ):
    """ jobID setter """
    self.__data__["JobID"] = long( value )

  @property
  def CreationTime( self ):
    """ creation time getter """
    return self.__data__["CreationTime"]

  @CreationTime.setter
  def CreationTime( self, value = None ):
    """ creation time setter """
    if type( value ) not in ( datetime.datetime, str ) :
      raise TypeError( "CreationTime should be a datetime.datetime!" )
    if type( value ) == str:
      value = datetime.datetime.strptime( value.split( "." )[0], '%Y-%m-%d %H:%M:%S' )
    self.__data__["CreationTime"] = value

  @property
  def SubmitTime( self ):
    """ request's submission time getter """
    return self.__data__["SubmitTime"]

  @SubmitTime.setter
  def SubmitTime( self, value = None ):
    """ submission time setter """
    if type( value ) not in ( datetime.datetime, str ):
      raise TypeError( "SubmitTime should be a datetime.datetime!" )
    if type( value ) == str:
      value = datetime.datetime.strptime( value.split( "." )[0], '%Y-%m-%d %H:%M:%S' )
    self.__data__["SubmitTime"] = value

  @property
  def LastUpdate( self ):
    """ last update getter """
    return self.__data__["LastUpdate"]

  @LastUpdate.setter
  def LastUpdate( self, value = None ):
    """ last update setter """
    if type( value ) not in  ( datetime.datetime, str ):
      raise TypeError( "LastUpdate should be a datetime.datetime!" )
    if type( value ) == str:
      value = datetime.datetime.strptime( value.split( "." )[0], '%Y-%m-%d %H:%M:%S' )
    self.__data__["LastUpdate"] = value

  @property
  def Status( self ):
    """ status getter """
    opStatuses = list( set( [ op.Status for op in self.__operations__ ] ) )
    status = "Waiting"
    if "Done" in opStatuses:
      status = "Done"
    if "Assigned" in opStatuses:
      status = "Assigned"
    if "Waiting" in opStatuses:
      status = "Waiting"
    if "Scheduled" in opStatuses:
      status = "Scheduled"
    self.__data__["Status"] = status
    return self.__data__["Status"]

  @Status.setter
  def Status( self, value ):
    """ status setter """
    if value not in ( "Done", "Waiting", "Failed", "Assigned", "Scheduled" ):
      raise ValueError( "Unknown status: %s" % str( value ) )
    self.__data__["Status"] = value

  @property
  def Order( self ):
    """ ro execution order getter """
    self._notify()
    opStatuses = [ op.Status for op in self.__operations__ ]
    return opStatuses.index( "Waiting" ) if "Waiting" in opStatuses else len( opStatuses )

  @property
  def Error( self ):
    """ error getter """
    return self.__data__["Error"]

  @Error.setter
  def Error( self, error ):
    """ error setter """
    self.__data__["Error"] = str( error )[255:]

  @classmethod
  def fromXML( cls, element ):
    """ create Request object from xmlString or xml.ElementTree.Element """
    if type( element ) == str:
      try:
        element = ElementTree.fromstring( element )
      except ExpatError, error:
        return S_ERROR( str( error ) )
    if element.tag != "request":
      return S_ERROR( "unable to de-serialize request, xml root element is not a 'request' " )
    request = Request( element.attrib )
    for operationElement in element.findall( "operation" ):
      operation = Operation.fromXML( element = operationElement )
      if not operation["OK"]:
        return operation
      request.addOperation( operation["Value"] )
    return S_OK( request )

  def toXML( self, dumpToStr = False ):
    """ dump request to XML

    :param self: self reference
    :return: S_OK( xmlString )
    """
    dumpToStr = bool( dumpToStr )
    root = ElementTree.Element( "request" )
    root.attrib["RequestName"] = str( self.RequestName ) if self.RequestName else ""
    root.attrib["RequestID"] = str( self.RequestID ) if self.RequestID else ""
    root.attrib["OwnerDN"] = str( self.OwnerDN ) if self.OwnerDN else ""
    root.attrib["OwnerGroup"] = str( self.OwnerGroup ) if self.OwnerGroup else ""
    root.attrib["DIRACSetup"] = str( self.DIRACSetup ) if self.DIRACSetup else ""
    root.attrib["JobID"] = str( self.JobID ) if self.JobID else "0"
    root.attrib["SourceComponent"] = str( self.SourceComponent ) if self.SourceComponent else ""
    root.attrib["Error"] = str( self.Error ) if self.Error else ""
    # # always calculate status, never set
    root.attrib["Status"] = str( self.Status )
    # # datetime up to seconds
    root.attrib["CreationTime"] = self.CreationTime.isoformat( " " ).split( "." )[0] if self.CreationTime else ""
    root.attrib["SubmitTime"] = self.SubmitTime.isoformat( " " ).split( "." )[0] if self.SubmitTime else ""
    root.attrib["LastUpdate"] = self.LastUpdate.isoformat( " " ).split( "." )[0] if self.LastUpdate else ""
    # # trigger xml dump of a whole operations and their files tree
    for operation in self.__operations__:
      opXML = operation.toXML()
      if not opXML["OK"]:
        return opXML
      root.append( opXML["Value"] )
    return S_OK( { False: root,
                    True: ElementTree.tostring( root ) }[dumpToStr] )

  def toSQL( self ):
    """ prepare SQL INSERT or UPDATE statement """
    colVals = [ ( "`%s`" % column, "'%s'" % value if type( value ) in ( str, datetime.datetime ) else str( value ) )
                for column, value in self.__data__.items()
                if value and column not in  ( "RequestID", "LastUpdate" ) ]
    colVals.append( ( "`LastUpdate`", "UTC_TIMESTAMP()" ) )
    query = []
    if self.RequestID:
      query.append( "UPDATE `Request` SET " )
      query.append( ", ".join( [ "%s=%s" % item for item in colVals  ] ) )
      query.append( " WHERE `RequestID`=%d;\n" % self.RequestID )
    else:
      query.append( "INSERT INTO `Request` " )
      columns = "(%s)" % ",".join( [ column for column, value in colVals ] )
      values = "(%s)" % ",".join( [ value for column, value in colVals ] )
      query.append( columns )
      query.append( " VALUES %s;" % values )
      # query.append( "WHERE NOT EXISTS (SELECT `RequestName` FROM `Request` WHERE `RequestName` = '%s');\n" % self.RequestName )
    return S_OK( "".join( query ) )

  # # digest
  def toJSON( self ):
    """ get digest for a web """
    digest = dict( zip( self.__data__.keys(),
                        [ str( val ) if val else "" for val in self.__data__.values() ] ) )
    digest["Operations"] = []
    for op in self:
      opJSON = op.toJSON()
      if not opJSON["OK"]:
        return opJSON
      digest["Operations"].append( opJSON["Value"] )
    return S_OK( digest )
