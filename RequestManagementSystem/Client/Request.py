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

__RCSID__ = "$Id $"

##
# @file Request.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/07/16 13:44:00
# @brief Definition of Request class.

## imports 
from types import LongType, IntType
import datetime
try:
  import xml.etree.cElementTree as ElementTree
except ImportError:
  import xml.etree.ElementTree as ElementTree
from xml.parsers.expat import ExpatError
  
## from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.TypedList import TypedList
##  
from DIRAC.RequestManagementSystem.Client.SubRequest import SubRequest
from DIRAC.RequestManagementSystem.Client.RequestFile import RequestFile
  
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
  ## requets's id
  __requestID = None
  ## request's name
  __name = None
  ## request's owner DN
  __ownerDN = None
  ## request's owner group
  __ownerGroup = None
  ## DIRAC setup
  __setup = None
  ## source component
  __sourceComponent = None
  ## jobID
  __jobID = 0
  ## creation time
  __creationTime = None 
  ## submission time
  __submissionTime = None 
  ## last update 
  __lastUpdate = None
  ## status
  __status = "Waiting"
  ## list of sub-requests
  __subRequests = TypedList( allowedTypes=SubRequest )

  def __init__( self ):
    """c'tor

    :param self: self reference
    """
    self.__name = ""

  ## SubRequest aritmetics
  def __contains__( self, subRequest ):
    """ in operator 
    
    :param self: self reference
    :param SubRequest subRequest: a subRequest 
    """
    return bool( subRequest in self.__subRequests ) 

  def __iadd__( self, subRequest ):
    """ += operator for subRequest

    :param self: self reference
    :param SubRequest subRequest: sub-request to add
    """
    if subRequest not in self:
      self.__subRequests.append( subRequest )
      subRequest.parent = self
    return self

  def __isub__( self, subRequest ):
    """ -= operator for subRequest

    :param self: self reference
    :param SubRequest subRequest: sub-request to add
    """
    if subRequest in self:
      self.__subRequests.remove( subRequest )
      subRequest.parent = None
    return self
   
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
    self.__subRequests.insert( self.__subRequests.index( existingSubRequest ), newSubRequest )
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
    self.__subRequests.insert( self.__subRequests.index( existingSubRequest )+1, newSubRequest )
    return S_OK()

  def addSubRequest( self, subRequest ):
    """ add :subRequest: to list of SubRequests

    :param self: self reference
    :param SubRequest subRequest: SubRequest to be insterted
    """
    if subRequest in self:
      return S_ERROR( "%s is already in" % subRequest )
    self += subRequest
    return S_OK()

  def removeSubRequest( self, subRequest ):
    """ delete :subRequest: from request 

    :param self: self reference
    :param SubRequest subRequest: SubRequest to be removed 
    """
    self -= subRequest 
    return S_OK()

  def __iter__( self ):
    return self.__subRequests.__iter__()
  
  ## props
  def requestID():
    """ request ID prop """
    doc = "request ID"
    def fset( self, value ):
      """ requestID setter """
      if type(value) not in (LongType, IntType, StringType):
        raise TypeError("requestID should be an integer!")
      self.__requestID = long(value)
    def fget( self ):
      """ request ID getter """
      return self.__requestID
    return locals()
  requestID = property( **requestID() )

  def ownerDN():
    """ request owner DN prop """
    doc = "request owner DN"
    def fset( self, value ):
      """ request owner DN setter """
      if type(value) != str:
        raise TypeError("ownerDN should be a string!")
      self.__ownerDN = value
    def fget( self ):
      """ request owner DN getter """
      return self.__ownerDN
    return locals()
  ownerDN = property( **ownerDN() )

  def ownerGroup():
    """ request owner group prop """
    doc = "request owner group "
    def fset( self, value ):
      """ request owner group setter """
      if type(value) != str:
        raise TypeError("ownerGroup should be a string!")
      self.__ownerGroup = value
    def fget( self ):
      """ request owner group getter """
      return self.__ownerGroup
    return locals()
  ownerGroup = property( **ownerGroup() )

  def setup():
    """ DIRAC setup prop """
    doc = "DIRAC setup"
    def fset( self, value ):
      """ DIRAC setup setter """
      if type(value) != str:
        raise TypeError("setu should be a string!")
      self.__setup = value
    def fget( self ):
      """ DIRAC setup getter """
      return self.__setup
    return locals() 
  setup = property( **setup() )

  def sourceComponent():
    """ source component prop """
    doc = "source component "
    def fset( self, value ):
      """ source component setter """
      if type(value) != str:
        raise TypeError("setu should be a string!")
      self.__sourceComponent = value
    def fget( self ):
      """ source component getter """
      return self.__sourceComponent
    return locals() 
  sourceComponent = property( **sourceComponent() )

  def name():
    """ request's name prop """
    doc = "request's name"
    def fset( self, value ):
      """ request name setter """
      if type(value) != str:
        raise TypeError("name should be a string")
      self.__name = value
    def fget( self ):
      """ request name getter """
      return self.__name
    return locals()
  name = property( **name() )

  def jobID():
    """ jobID prop """
    doc = "jobID"
    def fset( self, value=0 ):
      """ jobID setter """
      if type(value) not in ( long, int, str ):
        raise TypeError( "jobID as to be an integer" )
      self.__jobID = long(value)
    def fget( self ):
      """ jobID getter """
      return self.__jobID
    return locals()
  jobID = property( **jobID() )

  def creationTime():
    """ request's creation time prop """
    doc = "request's creation time"
    def fset( self, value = None ):
      """ creation time setter """
      if type(value) not in ( datetime.datetime, str ) :
        raise TypeError("creationTime should be a datetime.datetime!")
      if type(value) == str:
        value = datetime.datetime.strptime( value.split(".")[0], '%Y-%m-%d %H:%M:%S' )
        self.__creationTime = value
    def fget( self ):
      """ creation time getter """
      return self.__creationTime
    return locals()
  creationTime = property( **creationTime() )

  def submissionTime():
    """ request's submission time prop """
    doc = "request's submisssion time"
    def fset( self, value = None ):
      """ submission time setter """
      if type(value) not in ( datetime.datetime, str ):
        raise TypeError("submissionTime should be a datetime.datetime!")
      if type(value) == str:
        value = datetime.datetime.strptime( value.split(".")[0], '%Y-%m-%d %H:%M:%S' )
      self.__submissionTime = value
    def fget( self ):
      """ submisssion time getter """
      return self.__submissionTime
    return locals()
  submissionTime = property( **submissionTime() )

  def lastUpdate():
    """ last update prop """ 
    doc = "request's last update"
    def fset( self, value = None ):
      """ last update setter """
      if type( value != type(datetime.datetime) ):
        raise TypeError("lastUpdate should be a datetime.datetime!")
      if type(value) == str:
        value = datetime.datetime.strptime( value.split(".")[0], '%Y-%m-%d %H:%M:%S' )
      self.__lastUpdate = value
    def fget( self ):
      """ submisssion time getter """
      return self.__lastUpdate
    return locals()
  lastUpdate = property( **lastUpdate() )

  
  ## status
  def status( self ):
    """ status prop
    
    TODO: add more logic here
    """
    def fget( self ):
      subStatuses = list( set( [ subRequest.status() for subRequest in self.__subRequests ] ) ) 
      self.__status = "New"
      if "Done" in subStatuses:
        self.__status = "Done"
      if "Assigned" in subStatuses:
        self.__status = "Assigned"
      if "Waiting" in subStatuses:
        self.__status = "Waiting"
      return self.__status  
      
  def executionOrder( self ):
    """ get execution order """
    subStatuses = [ subRequest.status() for subRequest in self.__subRequests ]
    return S_OK( subStatuses["Waiting"] if "Waiting" in subStatuses else len(subStatuses) )
    
  @classmethod
  def fromXML( cls, xmlString ):
    try:
      doc = ElementTree.parse( xmlString )
    except ExpatError, error:
      self.log.exception("unable to deserialize request from xml string", error )
      return S_ERROR( )
    root = doc.getroot()
    if root.tag != "request":
      return S_ERROR( "unable to deserialise request, xml root element is not a 'request' " )
    request = Request()
    for attrName, attrValue in root.attrib.items():
      setattr( request, attrName, attrValue )
  
  def toXML( self ):
    """ dump request to XML 

    :param self: self reference
    :return: S_OK( xmlString ) 
    """
    root = ElementTree.Element( "request" )
    root.attrib["RequestName"] = str(self.name) if self.name else ""
    root.attrib["RequestID"] = str(self.requestID) if self.requestID else ""
    root.attrib["OwnerDN"] = str(self.ownerDN) if self.ownerDN else "" 
    root.attrib["OwnerGroup"] = str(self.ownerGroup) if self.ownerGroup else "" 
    root.attrib["DIRACSetup"] = str(self.setup) if self.setup else ""
    root.attrib["JobID"] = str(self.jobID) if self.jobID else "0"
    root.attrib["SourceComponent"] = self.sourceComponent.isoformat(" ").split(".")[0] if self.sourceComponent else ""
    ## always calculate status, never set
    root.attrib["Status"] = self.status()
    ## datetime up to seconds
    root.attrib["CreationTime"] = self.creationTime.isoformat(" ").split(".")[0] is self.creationTime else ""
    root.attrib["SubmissionTime"] = self.submissionTime.isoformat(" ").split(".")[0] if self.submissionTime else ""
    root.attrib["LastUpdate"] = str(self.lastUpdate) if self.lastUpdate else ""
    for subRequest in self.__subRequests:
      root.insert( subRequest.toXML() )
    doc = ElementTree.ElementTree( root )
    xmlStr = ElementTree.tostring( doc, "utf-8", "xml" )
    return S_OK( xmlStr )

  @classmethod
  def fromSQL( cls, record ):
    pass

  
  def toSQL( self ):
    pass

  
