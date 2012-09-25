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
from DIRAC.RequestManagementSystem.Client.SubRequest import SubRequest
  
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
  :param TypedList subRequests: list of subrequests 
  """

  def __init__( self, fromDict=None ):
    """c'tor

    :param self: self reference
    """
    self.__data__ = dict.fromkeys( ( "RequestID", "RequestName", "OwnerDN", "OwnerGroup", "DIRACSetup", "Status", 
                                     "SourceComponent", "JobID", "CreationTime", "SubmissionTime", "LastUpdate"), None )
    now = datetime.datetime.utcnow().replace( microsecond = 0 )
    self.__data__["CreationTime"] = now 
    self.__data__["SubmissionTime"] = now
    self.__data__["LastUpdate"] = now
    self.__data__["Status"] = "Queued"
    self.__data__["JobID"] = 0
    self.__subReqs__ = TypedList( allowedTypes=SubRequest )
    fromDict = fromDict if fromDict else {}
    for key, value in fromDict.items():
      setattr( self, key, value )

  def _notify( self ):
    """ simple state machine for sub request statuses """

    waitingFound = False
    ## update sub-requets statuses
    for subReq in self:
      
      subReq.ExecutionOrder = self.indexOf( subReq )
      status = subReq.Status
  
      if status in ( "Done", "Failed" ):
        continue
      elif status == "Queued" and not waitingFound:
        subReq._setWaiting( self ) # Status = "Waiting" ## this is 1st queued, flip to waiting
        waitingFound = True 
      elif status == "Waiting":
        if waitingFound:
          subReq._setQueued( self ) #  Status = "Queued" ## flip to queued, another one is waiting
        else:
          waitingFound = True 

    # now update self status
    if "Queued" in self.subStatusList() or "Waiting" in self.subStatusList():
      if self.Status != "Waiting":
        self.Status = "Waiting"
    else:
      self.Status = "Done"

  ## SubRequest aritmetics
  def __contains__( self, subRequest ):
    """ in operator 
    
    :param self: self reference
    :param SubRequest subRequest: a subRequest 
    """
    return bool( subRequest in self.__subReqs__ ) 

  def __add__( self, subRequest ):
    """ + operator for subRequest

    :param self: self reference
    :param SubRequest subRequest: sub-request to add
    """
    if subRequest not in self:
      self.__subReqs__.append( subRequest )
      subRequest._parent = self
      subRequest.ExecutionOrder = self.indexOf( subRequest )
    return S_OK()
   
  def insertBefore( self, newSubRequest, existingSubRequest ):
    """ insert :newSubRequest: just before :existingSubRequest:

    :param self: self reference
    :param SubRequest newSubRequest: SubRequest to be inserted 
    :param SubRequest existingSubRequest: previous SubRequest sibling  
    """
    if existingSubRequest not in self:
      return S_ERROR( "%s is not in" % existingSubRequest )
    if newSubRequest in self:
      return S_ERROR( "%s is already in" % newSubRequest )
    self.__subReqs__.insert( self.__subReqs__.index( existingSubRequest ), newSubRequest )
    newSubRequest._parent = self
    newSubRequest.ExecutionOrder = self.indexOf( newSubRequest )
    return S_OK()
    
  def insertAfter( self, newSubRequest, existingSubRequest ):
    """ insert :newSubRequest: just after :existingSubRequest: 
    
    :param self: self reference
    :param SubRequest newSubRequest: SubRequest to be insterted
    :param SubRequest existingSubRequest: next SubRequest sibling
    """
    if existingSubRequest not in self:
      return S_ERROR( "%s is not in" % existingSubRequest )
    if newSubRequest in self:
      return S_ERROR( "%s is already in" % newSubRequest )
    self.__subReqs__.insert( self.__subReqs__.index( existingSubRequest )+1, newSubRequest )
    newSubRequest._parent = self 
    newSubRequest.ExecutionOrder = self.indexOf( newSubRequest )
    return S_OK()

  def addSubRequest( self, subRequest ):
    """ add :subRequest: to list of SubRequests

    :param self: self reference
    :param SubRequest subRequest: SubRequest to be insterted
    """
    if subRequest not in self:
      added = self + subRequest
    return S_OK()

  def __iter__( self ):
    """ iterator for sub-request """
    return self.__subReqs__.__iter__()

  def __getitem__( self, i ):
    """ [] op for sub requests """
    return self.__subReqs__.__getitem__( i )

  def indexOf( self, subReq ):
    """ return index of subReq (execution order) """
    return self.__subReqs__.index( subReq ) if subReq in self else -1

  def __len__( self ):
    """ nb of subRequests """
    return len( self.__subReqs__ )

  def subStatusList( self ):
    """ list of statuses for all subRequest """
    return [ subReq.Status for subReq in self ]

  ## props
  def __requestID():
    """ request ID prop """
    doc = "request ID"
    def fset( self, value ):
      """ requestID setter """
      self.__data__["RequestID"] = long(value) if value else 0
    def fget( self ):
      """ request ID getter """
      return self.__data__["RequestID"]
    return locals()
  RequestID = property( **__requestID() )

  def __ownerDN():
    """ request owner DN prop """
    doc = "request owner DN"
    def fset( self, value ):
      """ request owner DN setter """
      if type(value) != str:
        raise TypeError("ownerDN should be a string!")
      self.__data__["OwnerDN"] = value
    def fget( self ):
      """ request owner DN getter """
      return self.__data__["OwnerDN"]
    return locals()
  OwnerDN = property( **__ownerDN() )

  def __ownerGroup():
    """ request owner group prop """
    doc = "request owner group "
    def fset( self, value ):
      """ request owner group setter """
      if type(value) != str:
        raise TypeError("ownerGroup should be a string!")
      self.__data__["OwnerGroup"] = value
    def fget( self ):
      """ request owner group getter """
      return self.__data__["OwnerGroup"]
    return locals()
  OwnerGroup = property( **__ownerGroup() )

  def __setup():
    """ DIRAC setup prop """
    doc = "DIRAC setup"
    def fset( self, value ):
      """ DIRAC setup setter """
      if type(value) != str:
        raise TypeError("setup should be a string!")
      self.__data__["DIRACSetup"] = value
    def fget( self ):
      """ DIRAC setup getter """
      return self.__data__["DIRACSetup"]
    return locals() 
  DIRACSetup = property( **__setup() )

  def __sourceComponent():
    """ source component prop """
    doc = "source component "
    def fset( self, value ):
      """ source component setter """
      if type(value) != str:
        raise TypeError("Setup should be a string!")
      self.__data__["SourceComponent"] = value
    def fget( self ):
      """ source component getter """
      return self.__data__["SourceComponent"]
    return locals() 
  SourceComponent = property( **__sourceComponent() )

  def __name():
    """ request's name prop """
    doc = "request's name"
    def fset( self, value ):
      """ request name setter """
      if type(value) != str:
        raise TypeError("RequestName should be a string")
      self.__data__["RequestName"] = value[:128]
    def fget( self ):
      """ request name getter """
      return self.__data__["RequestName"]
    return locals()
  RequestName = property( **__name() )

  def __jobID():
    """ jobID prop """
    doc = "jobID"
    def fset( self, value=0 ):
      """ jobID setter """
      self.__data__["JobID"] = long(value)
    def fget( self ):
      """ jobID getter """
      return self.__data__["JobID"]
    return locals()
  JobID = property( **__jobID() )

  def __creationTime():
    """ request's creation time prop """
    doc = "request's creation time"
    def fset( self, value = None ):
      """ creation time setter """
      if type(value) not in ( datetime.datetime, str ) :
        raise TypeError("CreationTime should be a datetime.datetime!")
      if type(value) == str:
        value = datetime.datetime.strptime( value.split(".")[0], '%Y-%m-%d %H:%M:%S' )
        self.__data__["CreationTime"] = value
    def fget( self ):
      """ creation time getter """
      return self.__data__["CreationTime"]
    return locals()
  CreationTime = property( **__creationTime() )

  def __submissionTime():
    """ request's submission time prop """
    doc = "request's submisssion time"
    def fset( self, value = None ):
      """ submission time setter """
      if type(value) not in ( datetime.datetime, str ):
        raise TypeError("SubmissionTime should be a datetime.datetime!")
      if type(value) == str:
        value = datetime.datetime.strptime( value.split(".")[0], '%Y-%m-%d %H:%M:%S' )
      self.__data__["SubmissionTime"] = value
    def fget( self ):
      """ submission time getter """
      return self.__data__["SubmissionTime"]
    return locals()
  SubmissionTime = property( **__submissionTime() )

  def __lastUpdate():
    """ last update prop """ 
    doc = "request's last update"
    def fset( self, value = None ):
      """ last update setter """
      if type( value ) not in  ( datetime.datetime, str ):
        raise TypeError("LastUpdate should be a datetime.datetime!")
      if type(value) == str:
        value = datetime.datetime.strptime( value.split(".")[0], '%Y-%m-%d %H:%M:%S' )
      self.__data__["LastUpdate"] = value
    def fget( self ):
      """ last update time getter """
      return self.__data__["LastUpdate"]
    return locals()
  LastUpdate = property( **__lastUpdate() )

  ## status
  def __status():
    """ status prop """
    doc = "request status"
    def fset( self, value ):
      """ status setter """
      if value not in ( "Done", "Waiting", "Failed" ):
        raise ValueError( "Unknown status: %s" % str(value) )
      self.__status = value      
    def fget( self ):
      """ status getter """
      subStatuses = list( set( [ subRequest.Status for subRequest in self.__subReqs__ ] ) ) 
      status = "Waiting"
      if "Done" in subStatuses:
        status = "Done"
      if "Assigned" in subStatuses:
        status = "Assigned"
      if "Waiting" in subStatuses:
        status = "Waiting"
      self.__data__["Status"] = status
      return self.__data__["Status"]
    return locals()
  Status = property( **__status() )

  def currentExecutionOrder( self ):
    """ get execution order """
    self._notify()
    subStatuses = [ subRequest.Status for subRequest in self.__subReqs__ ]
    return S_OK( subStatuses.index("Waiting") if "Waiting" in subStatuses else len(subStatuses) )
    
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
    for subReqElement in root.findall( "subrequest" ):
      request += SubRequest.fromXML( element=subReqElement )
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
    root.attrib["SubmissionTime"] = self.SubmissionTime.isoformat(" ").split(".")[0] if self.SubmissionTime else ""
    root.attrib["LastUpdate"] = self.LastUpdate.isoformat(" ").split(".")[0] if self.LastUpdate else "" 
    ## trigger xml dump of a whole subrequests and their files tree
    for subRequest in self.__subReqs__:
      root.append( subRequest.toXML() )
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
      query.append( "UPDATE `Requests` SET " )
      query.append( ", ".join( [ "%s=%s" % item for item in colVals  ] ) )
      query.append( " WHERE `RequestID`=%d;\n" % self.RequestID )
    else:
      query.append( "INSERT INTO `Requests` " )
      columns = "(%s)" % ",".join( [ column for column, value in colVals ] )
      values = "(%s)" % ",".join( [ value for column, value in colVals ] )
      query.append( columns )
      query.append(" VALUES %s;\n" % values )
    return "".join( query )
    
  ## digest
  def getDigest( self ):
    """ get digest for a web """
    digestString = []
    for subReq in self:
      digestList = [ str(subReq.RequestType), 
                     str(subReq.Operation), 
                     str(subReq.Status), 
                     str(subReq.ExecutionOrder), 
                     str(subReq.TargetSE) if subReq.TargetSE else "", 
                     str(subReq.Catalogue) if subReq.Catalogue else "" ]
      if len(subReq):
        subFile = subReq[0]
        digestList.append( "%s,...<%d files>" % ( os.path.basename( subFile.LFN ), len(subReq) ) )
      digestString.append( ":".join(digestList ) )
    return S_OK( "\n".join( digestString ) ) 

