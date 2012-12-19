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

##
# @file Request.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/07/16 13:44:00
# @brief Definition of Request class.

## imports 
import os
import datetime
try:
  import xml.etree.cElementTree as ElementTree
except ImportError:
  import xml.etree.ElementTree as ElementTree
from xml.parsers.expat import ExpatError
  
## from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.TypedList import TypedList 
from DIRAC.RequestManagementSystem.Client.Operation import Operation
  
########################################################################
class Request(object):
  """
  .. class:: Request
 
  :param int requestID: requestID
  :param str name: request' name
  :param str ownerDN: request's owner DN
  :param str ownerGroup: request owner group
  :param str setup: DIRAC setup
  :param str sourceComponent: ??? 
  :param int jobID: jobID 
  :param datetime.datetime creationTime: UTC datetime 
  :param datetime.datetime submissionTime: UTC datetime 
  :param datetime.datetime lastUpdate: UTC datetime 
  :param str status: request's status
  :param TypedList operations: list of operations 
  """

  def __init__( self, fromDict=None ):
    """c'tor

    :param self: self reference
    """
    self.__waiting = None 
    self.__data__ = dict.fromkeys( ( "RequestID", "RequestName", "OwnerDN", "OwnerGroup", "DIRACSetup", "Status", 
                                     "SourceComponent", "JobID", "CreationTime", "SubmitTime", "LastUpdate", "Error" ), None )
    now = datetime.datetime.utcnow().replace( microsecond = 0 )
    self.__data__["CreationTime"] = now 
    self.__data__["SubmitTime"] = now
    self.__data__["LastUpdate"] = now
    self.__data__["Status"] = "Waiting"
    self.__data__["JobID"] = 0
    self.__operations__ = TypedList( allowedTypes=Operation )
    fromDict = fromDict if fromDict else {}
    for key, value in fromDict.items():
      if key not in self.__data__:
        raise AttributeError("Unknown Request attribute '%s'" % key )
      if value:
        setattr( self, key, value )

  def _notify( self ):
    """ simple state machine for sub request statuses """

    self.__waiting = None 
    ## update sub-requets statuses
    for subReq in self:
      
      status = subReq.Status
  
      if status in ( "Done", "Failed" ):
        continue
      elif status == "Queued" and not self.__waiting:
        subReq._setWaiting( self ) # Status = "Waiting" ## this is 1st queued, flip to waiting
        self.__waiting = subReq 
      elif status == "Waiting":
        if self.__waiting:
          subReq._setQueued( self ) #  Status = "Queued" ## flip to queued, another one is waiting
        else:
          self.__waiting = subReq
          
    # now update self status
    if "Queued" in self.subStatusList() or "Waiting" in self.subStatusList():
      if self.Status != "Waiting":
        self.Status = "Waiting"
    else:
      self.Status = "Done"

  def getWaiting( self ):
    """ get waiting subrequets if any """
    ## update states
    self._notify()
    return S_OK(self.__waiting) 

  ## Operation aritmetics
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
    return S_OK()
    
  def insertAfter( self, newOperation, existingOperation ):
    """ insert :newOperation: just after :existingOperation: 
    
    :param self: self reference
    :param Operation newOperation: Operation to be insterted
    :param Operation existingOperation: next Operation sibling
    """
    if existingOperation not in self:
      return S_ERROR( "%s is not in" % existingOperation )
    if newOperation in self:
      return S_ERROR( "%s is already in" % newOperation )
    self.__operations__.insert( self.__operations__.index( existingOperation )+1, newOperation )
    newOperation._parent = self 
    return S_OK()

  def addOperation( self, operation ):
    """ add :operation: to list of Operations

    :param self: self reference
    :param Operation operation: Operation to be insterted
    """
    if operation not in self:
      added = self + operation
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

  ## properties

  @property
  def RequestID( self ):
    """ request ID getter """
    return self.__data__["RequestID"]

  @RequestID.setter
  def RequestID( self, value ):
    """ requestID setter (shouldn't be RO???) """
    self.__data__["RequestID"] = long(value) if value else 0

  @property
  def RequestName( self ):
    """ request's name getter """
    return self.__data__["RequestName"]
    
  @RequestName.setter
  def RequestName( self, value ):
    """ request name setter """
    if type(value) != str:
      raise TypeError("RequestName should be a string")
    self.__data__["RequestName"] = value[:128]

  @property
  def OwnerDN( self ):
    """ request owner DN getter """
    return self.__data__["OwnerDN"]
  
  @OwnerDN.setter
  def OwnerDN( self, value ):
    """ request owner DN setter """
    if type(value) != str:
      raise TypeError("ownerDN should be a string!")
    self.__data__["OwnerDN"] = value
    
  @property
  def OwnerGroup( self ):
    """ request owner group getter  """
    return self.__data__["OwnerGroup"]

  @OwnerGroup.setter 
  def OwnerGroup( self, value ):
    """ request owner group setter """
    if type(value) != str:
      raise TypeError("ownerGroup should be a string!")
    self.__data__["OwnerGroup"] = value

  @property
  def DIRACSetup( self ):
    """ DIRAC setup getter  """
    return self.__data__["DIRACSetup"]

  @DIRACSetup.setter
  def DIRACSetup( self, value ):
    """ DIRAC setup setter """
    if type(value) != str:
      raise TypeError("setup should be a string!")
    self.__data__["DIRACSetup"] = value

  @property 
  def SourceComponent( self ):
    """ source component getter  """
    return self.__data__["SourceComponent"]

  @SourceComponent.setter
  def SourceComponent( self, value ):
    """ source component setter """
    if type(value) != str:
      raise TypeError("Setup should be a string!")
    self.__data__["SourceComponent"] = value

  @property
  def JobID( self ):
    """ jobID getter """
    return self.__data__["JobID"]

  @JobID.setter
  def JobID( self, value=0 ):
    """ jobID setter """
    self.__data__["JobID"] = long(value)
    
  @property
  def CreationTime( self ):
    """ creattion time getter """
    return self.__data__["CreationTime"]

  @CreationTime.setter 
  def CreationTime( self, value = None ):
    """ creation time setter """
    if type(value) not in ( datetime.datetime, str ) :
      raise TypeError("CreationTime should be a datetime.datetime!")
    if type(value) == str:
      value = datetime.datetime.strptime( value.split(".")[0], '%Y-%m-%d %H:%M:%S' )
    self.__data__["CreationTime"] = value

  @property
  def SubmitTime( self ):
    """ request's submission time getter """
    return self.__data__["SubmitTime"]

  @SubmitTime.setter 
  def SubmitTime( self, value = None ):
    """ submission time setter """
    if type(value) not in ( datetime.datetime, str ):
      raise TypeError("SubmitTime should be a datetime.datetime!")
    if type(value) == str:
      value = datetime.datetime.strptime( value.split(".")[0], '%Y-%m-%d %H:%M:%S' )
    self.__data__["SubmitTime"] = value
    
  @property 
  def LastUpdate( self ):
    """ last update getter """ 
    return self.__data__["LastUpdate"]

  @LastUpdate.setter
  def LastUpdate( self, value = None ):
    """ last update setter """
    if type( value ) not in  ( datetime.datetime, str ):
      raise TypeError("LastUpdate should be a datetime.datetime!")
    if type(value) == str:
      value = datetime.datetime.strptime( value.split(".")[0], '%Y-%m-%d %H:%M:%S' )
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
    self.__data__["Status"] = status
    return self.__data__["Status"]
  
  @Status.setter
  def Status( self, value ):
    """ status setter """
    if value not in ( "Done", "Waiting", "Failed" ):
      raise ValueError( "Unknown status: %s" % str(value) )
    self.__data__["Status"] = value      
  
  @property
  def Order( self ):
    """ ro execution order getter """
    self._notify()
    opStatuses = [ op.Status for op in self.__operations__ ]
    return opStatuses.index("Waiting") if "Waiting" in opStatuses else len(opStatuses) 
    
  @classmethod
  def fromXML( cls, xmlString ):
    """ create Request object from xmlString or xml.ElementTree.Element """
    try:
      root = ElementTree.fromstring( xmlString )
    except ExpatError, error:
      return S_ERROR( "unable to deserialise request from xml: %s" % str(error) )
    if root.tag != "request":
      return S_ERROR( "unable to deserialise request, xml root element is not a 'request' " )
    request = Request( root.attrib )
    for subReqElement in root.findall( "operation" ):
      request += Operation.fromXML( element=subReqElement )
    return S_OK( request )

  def toXML( self ):
    """ dump request to XML 

    :param self: self reference
    :return: S_OK( xmlString ) 
    """
    root = ElementTree.Element( "request" )
    root.attrib["RequestName"] = str(self.RequestName) if self.RequestName else ""
    root.attrib["RequestID"] = str(self.RequestID) if self.RequestID else ""
    root.attrib["OwnerDN"] = str(self.OwnerDN) if self.OwnerDN else "" 
    root.attrib["OwnerGroup"] = str(self.OwnerGroup) if self.OwnerGroup else "" 
    root.attrib["DIRACSetup"] = str(self.DIRACSetup) if self.DIRACSetup else ""
    root.attrib["JobID"] = str(self.JobID) if self.JobID else "0"
    root.attrib["SourceComponent"] = str(self.SourceComponent) if self.SourceComponent else "" 
    ## always calculate status, never set
    root.attrib["Status"] = str(self.Status)
    ## datetime up to seconds
    root.attrib["CreationTime"] = self.CreationTime.isoformat(" ").split(".")[0] if self.CreationTime else ""
    root.attrib["SubmitTime"] = self.SubmitTime.isoformat(" ").split(".")[0] if self.SubmitTime else ""
    root.attrib["LastUpdate"] = self.LastUpdate.isoformat(" ").split(".")[0] if self.LastUpdate else "" 
    ## trigger xml dump of a whole operations and their files tree
    for operation in self.__operations__:
      root.append( operation.toXML() )
    xmlStr = ElementTree.tostring( root )
    return S_OK( xmlStr )

  def toSQL( self ):
    """ prepare SQL INSERT or UDPATE statement """
    colVals = [ ( "`%s`" % column, "'%s'" % value if type(value) in ( str, datetime.datetime ) else str(value) ) 
                for column, value in self.__data__.items()
                if value and column not in  ( "RequestID", "LastUpdate" ) ] 
    colVals.append( ("`LastUpdate`", "UTC_TIMESTAMP()" ) )
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
      query.append(" VALUES %s;\n" % values )
    return "".join( query )
    
  ## digest
  def getDigest( self ):
    """ get digest for a web """
    digestString = []
    for op in self:
      digestList = [ str(op.Type),  
                     str(op.Status), 
                     str(op.Order), 
                     str(op.TargetSE) if op.TargetSE else "", 
                     str(op.Catalogue) if op.Catalogue else "" ]
      if len(op):
        subFile = op[0]
        digestList.append( "%s,...<%d files>" % ( os.path.basename( subFile.LFN ), len(op) ) )
      digestString.append( ":".join( digestList ) )
    return S_OK( "\n".join( digestString ) ) 

