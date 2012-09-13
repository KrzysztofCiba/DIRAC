########################################################################
# $HeadURL $
# File: SubRequest.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/07/24 12:12:05
########################################################################

""" :mod: SubRequest 
    =======================
 
    .. module: SubRequest
    :synopsis: SubRequest implementation
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    SubRequest implementation
"""
# for properties 
# pylint: disable=E0211,W0612,W0142 

__RCSID__ = "$Id $"

##
# @file SubRequest.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/07/24 12:12:18
# @brief Definition of SubRequest class.

## imports 
try:
  import xml.etree.cElementTree as ElementTree
except ImportError:
  import xml.etree.ElementTree
from xml.parsers.expat import ExpatError
import datetime
import itertools
## from DIRAC
from DIRAC.Core.Utilities.TypedList import TypedList
from DIRAC.RequestManagementSystem.Client.SubReqFile import SubReqFile

########################################################################
class SubRequest(object):
  """
  .. class:: SubRequest
 
  :param long SubRequestID: SubRequestID as read from DB backend
  :param long RequestID: parent RequestID 
  :param str Status: execution status
  :param str RequestType: one of ( "diset", "logupload", "register", "removal", "transfer" )
  :param str Operation: operation to perform
  :param str Arguments: additional arguments
  :param str SourceSE: source SE name
  :param str TargetSE: target SE names as comma separated list
  :param str Catalogue: catalogue to use as comma separated list
  :param str Error: error string if any
  :param Request parent: parent Request instance
  """
 
  def __init__( self, fromDict=None ):
    """ c'tor

    :param self: self reference
    :param dict fromDict: attributes dictionary
    """
    self._parent = None
    ## sub-request attributes
    self.__data__ = dict.fromkeys( ( "RequestID",  "SubRequestID", "Status", "RequestType", "Operation", 
                                     "Argument", "SourceSE", "TargetSE", "Catalogue", "Error",
                                     "CreationTime", "SubmissionTime", "LastUpdate" ) )

    now = datetime.datetime.utcnow().replace( microsecond=0 )
    self.__data__["CreationTime"] = now
    self.__data__["SubmissionTime"] = now
    self.__data__["LastUpdate"] = now
    self.__data__["SubRequestID"] = 0
    self.__data__["RequestID"] = 0
    self.__data__["Status"] = "Queued"

    ## sub-request files
    self.__files__ = TypedList( allowedTypes = SubReqFile )
    ## initilise
    fromDict = fromDict if fromDict else {}
    for key, value in fromDict.items():
      if value != None:
        setattr( self, key, value )

  def _notify( self ):
    """ notify self about file status change """
    if "Scheduled" in self.fileStatusList() or "Waiting" in self.fileStatusList() and self.Status not in ( "Waiting", "Queued" ):
      self.Status = "Waiting"

  def _setQueued( self ):
    """ don't touch """
    self.__data__["Status"] = "Queued"
    
  def _setWaiting( self ):
    """ don't touch as well """
    self.__data__["Status"] = "Waiting"

  ## SubReqFiles aritmetics 
  def __contains__( self, subFile ):
    """ in operator """
    return subFile in self.__files__

  def __iadd__( self, subFile ):
    """ += operator """
    if subFile not in self:
      self.__files__.append( subFile )
      subFile._parent = self 
      self._notify()
    return self

  def __add__( self, subFile ):
    """ + operator """
    self += subFile
      
  def addFile( self, subFile ):
    """ add :subFile: to subrequest """
    self += subFile

  def __iter__( self ):
    """ subrequest files iterator """
    return self.__files__.__iter__()

  def fileStatusList( self ):
    """ get list of files statuses """
    return [ subFile.Status for subFile in self ] 

  def __len__( self ):
    """ nb of subFiles """
    return len( self.__files__ )

  ## props 
  def __requestID():
    """ RequestID prop """
    doc = "RequestID"
    def fset( self, value ):
      """ RequestID setter """
      value = long(value) if value else None
      if self._parent and self._parent.RequestID and self._parent.RequestID != value:
        raise ValueError("Parent RequestID mismatch (%s != %s)" % ( self._parent.RequestID, value ) )
      self.__data__["RequestID"] = value
    def fget( self ):
      """ RequestID getter """
      if self._parent:
        self.__data__["RequestID"] = self._parent.RequestID
      return self.__data__["RequestID"]
    return locals()
  RequestID = property( **__requestID() )

  def __subRequestID():
    """ SubRequestID prop"""
    doc = "SubRequestID"
    def fset( self, value ):
      """ SubRequestID setter """
      self.__data__["SubRequestID"] = long(value) if value else 0
    def fget( self ):
      """ SubRequestID getter """
      return self.__data__["SubRequestID"]
    return locals()
  SubRequestID = property( **__subRequestID() )

  def __requestType():
    """ request type prop """
    doc = "request type"
    def fset( self, value ):
      """ request type setter """
      if value not in ( "diset", "logupload", "register", "removal", "transfer" ):
        raise ValueError( "%s is not a valid request type!" % str(value) )
      if self.Operation and value != { "commitRegisters" : "diset",
                                       "setFileStatusForTransformation" : "diset",
                                       "setJobStatusBulk" : "diset",
                                       "sendXMLBookkeepingReport" : "diset",
                                       "setJobParameters" : "diset",
                                       "uploadLogFiles" : "loguplad",
                                       "registerFile" : "register",
                                       "reTransfer" : "register",
                                       "replicaRemoval" : "removal",
                                       "removeFile" : "removal",
                                       "physicalRemoval" : "removal",
                                       "putAndRegister" : "transfer",
                                       "replicateAndRegister" : "transfer" }[self.Operation]:
        raise ValueError("RequestType '%s' is not valid for Operation '%s'" % ( str(value), self.Operation) ) 
      self.__data__["RequestType"] = value
    def fget( self ):
      """ request type getter """
      return self.__data__["RequestType"]
    return locals()
  RequestType = property( **__requestType() )

  def __operation():
    """ operation prop """
    doc = "operation"
    def fset( self, value ):
      """ operation setter """
      operationDict = { "diset" : ( "commitRegisters", "setFileStatusForTransformation", "setJobStatusBulk",
                                    "sendXMLBookkeepingReport", "setJobParameters" ),
                        "logupload" : ( "uploadLogFiles", ),
                        "register" : ( "registeFile", "reTransfer" ),
                        "removal" : ( "replicaRemoval", "removeFile", "physicalRemoval" ),
                        "transfer" : ( "replicateAndRegister", "putAndRegister" ) } 
      if value not in tuple( itertools.chain( *operationDict.values() ) ):
        raise ValueError( "'%s' in not valid Operation!" % value )
      if self.RequestType and value not in operationDict[ self.RequestType ]:
        raise ValueError("Operation '%s' is not valid for '%s' request type!" % ( str(value),  self.RequestType ) )
      self.__data__["Operation"] = value
    def fget( self ):
      """ operation getter """
      return  self.__data__["Operation"]
    return locals()
  Operation = property( **__operation() )
          
  def __arguments():
    """ arguments prop """
    doc = "sub-request arguments"
    def fset( self, value ):
      """ arguments setter """
      self.__data__["Arguments"] = value
    def fget( self ):
      """ arguments getter """
      return self.__data__["Arguments"]
    return locals()
  Arguments = property( **__arguments() )
  
  def __sourceSE():
    """ source SE prop """
    doc = "source SE"
    def fset( self, value ):
      """ source SE setter """
      self.__data__["SourceSE"] = str(value) if value else ""
    def fget( self ):
      """ source SE getter """
      return self.__data__["SourceSE"] 
    return locals()
  SourceSE = property( **__sourceSE() )
  
  def __targetSE():
    """ target SE prop """
    doc = "source SE"
    def fset( self, value ):
      """ target SE setter """
      self.__data__["TargetSE"] = value
    def fget( self ):
      """ target SE getter """
      return self.__data__["TargetSE"]
    return locals()
  TargetSE = property( **__targetSE() )
  
  def __catalogue():
    """ catalogue prop """
    doc = "catalogue"
    def fset( self, value ):
      """ catalogue setter """
      # TODO check type == list or comma separated str 
      self.__data__["Catalogue"] = value
    def fget( self ):
      """ catalogue getter """
      return self.__data__["Catalogue"]
    return locals()
  Catalogue = property( **__catalogue() )

  def __error():
    """ error prop """
    doc = "error"
    def fset( self, value ):
      """ error setter """
      self.__data__["Error"] = value[:255] if value else ""
    def fget( self ):
      """ error getter """
      return self.__data__["Error"]
    return locals()
  Error = property( **__error() )

  def __status():
    """ Status prop """
    doc = "Status"
    def fset( self, value ):
      """ Status setter """
      if value not in ( "Waiting", "Assigned", "Queued", "Failed", "Done" ):
        raise ValueError("unknown Status '%s'" % str(value) )
      if value in ( "Failed", "Done" ) and self.__files__:
        if "Waiting" in self.fileStatusList() or "Scheduled" in self.fileStatusList():
          return 
          #raise ValueError("unable to set status to '%s', there are waiting files" % value )
      ## update? notify parent
      if value != self.Status and self._parent:       
        self._parent._notify()
      self.__data__["Status"] = value
    def fget( self ):
      """ Status getter """
      return self.__data__["Status"]
    return locals()
  Status = property( **__status() )

  
  def __executionOrder():
    """ ExecutionOrder prop """
    doc = "ExecutionOrder"
    def fset( self, value ):
      """ ExecutionOrder setter """
      ## if parent present
      pass
    def fget( self ):
      """ ExecutionOrder getter """
      return self.__data__["ExecutionOrder"]
    return locals()
  ExecutionOrder = property( **__executionOrder() )

  def __creationTime():
    """ CreationTime prop """
    pass

  def __submissionTime():
    """ CreationTime prop """
    pass

  def __LastUpdate():
    """ CreationTime prop """
    pass


  def toXML( self ):
    """ dump subrequest to XML """
    data = dict( [ ( key, str(val) ) for key, val in self.__data__.items() ] )
    element = ElementTree.Element( "subrequest", data ) 
    for subFile in self.__files__:
      element.append( subFile.toXML() )
    return element
  
  @classmethod
  def fromXML( cls, element ):
    """ generate SubRequest instance from :element: 
    
    :param ElementTree.Element element: subrequest element
    """
    if not isinstance( element, type(ElementTree.Element("subrequest")) ):
      raise TypeError("wrong argument type %s, excpected ElementTree.Element" % type(element) )
    if element.tag != "subrequest":
      raise ValueError("wrong tag <%s>, expected <subrequest>!" % element.tag )
    subRequest = SubRequest( element.attrib )
    for fileElement in element.findall( "file" ):
      subRequest += SubReqFile.fromXML( fileElement )
    return subRequest

  @classmethod
  def fromSQL( cls, dictRec ):
    """ TODO """
    subRequest = SubRequest()
    return subRequest
    
  def __str__( self ):
    """ str operator """
    return ElementTree.tostring( self.toXML() )

  def toSQL( self ):
    """ get SQL INSTERT or UPDATE statement """
    colVals = [ ( "`%s`" % column, "'%s'" % value if type(value) in ( str, datetime.datetime ) else str(value) ) 
                for column, value in self.__data__.items()
                if value and column not in  ( "SubRequestID", "LastUpdate" ) ] 
    colVals.append( ("`LastUpdate`", "UTC_TIMESTAMP()" ) )
    query = []
    if self.SubRequestID:
      query.append( "UPDATE `SubRequests` SET " )
      query.append( ", ".join( [ "%s=%s" % item for item in colVals  ] ) )
      query.append( " WHERE `SubRequestID`=%d;\n" % self.SubRequestID )
    else:
      query.append( "INSERT INTO `SubRequests` " )
      columns = "(%s)" % ",".join( [ column for column, value in colVals ] )
      values = "(%s)" % ",".join( [ value for column, value in colVals ] )
      query.append( columns )
      query.append(" VALUES %s;\n" % values )
    #for subFile in self:
    #  query.append( subFile.toSQL() )
    return "".join( query )
