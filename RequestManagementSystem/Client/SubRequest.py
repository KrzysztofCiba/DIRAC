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
import itertools
## from DIRAC
from DIRAC import S_OK
from DIRAC.Core.Utilities.TypedList import TypedList
from DIRAC.RequestManagementSystem.Client.SubReqFile import SubReqFile

########################################################################
class SubRequest(object):
  """
  .. class:: SubRequest
  
  """
  ## sub-request files
  __files = TypedList( allowedTypes = SubReqFile )

  ## sub-request attributes
  __data__ = dict.fromkeys( ( "RequestType", "Operation", "Arguments", "RequestID",
                             "SourceSE", "TargetSE", "Catalogue", "Error" ), None )

  def __init__( self, fromDict=None ):
    """c'tor

    :param self: self reference
    """
    fromDict = fromDict if fromDict else {}
    for key, value in fromDict:
      setattr( self, key, value )

  ## SubReqFiles aritmetics   
  def __contains__( self, subFile ):
    """ in operator """
    return subFile in self.__files

  def __iadd__( self, subFile ):
    """ += operator """
    if subFile not in self:
      self.__files.append( subFile )
    return self

  def __add__( self, subFile ):
    """ + operator """
    if subFile not in self:
      self.__files.append( subFile )
      
  def addFile( self, subFile ):
    """ add :subFile: to subrequest """
    return self + subFile

  def __isub__( self, subFile ):
    """ -= operator """
    if subFile in self:
      self.__files.remove( subFile )
    return self

  def __sub__( self, subFile ):
    """ - operator """
    if subFile in self:
      self.__files.remove( subFile )

  def removeFile( self, subFile ):
    """ remove :subFile: from sub-request """
    return self - subFile

  ## props 
  # pylint: disable=E0211,W0612 
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
        raise ValueError( "%s in not valid Operation!" % value )
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
      self.__data__["SourceSE"] = value
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
      if type(value) != str:
        raise ValueError("error has to be a string!")
      self.__data__["Error"] = value[255:]
    def fget( self ):
      """ error getter """
      return self.__data__["Error"]
    return locals()
  Error = property( **__error() )

  def toXML( self ):
    """ dump subrequest to XML """
    element = ElementTree.Element( "subrequest", self.__data__ ) 
    for subFile in self.__files:
      element.append( subFile.toXML() )
    return element
  
  @classmethod
  def fromXML( cls, element ):
    """ generate SubRequest instance from :element: 
    
    :param ElementTree.Element element: subrequest element
    """
    if not isinstance( element, ElementTree.Element):
      raise TypeError("wrong argument type %s, excpected ElementTree.Element" % type(element) )
    if element.tag != "subrequest":
      raise ValueError("wrong tag <%s>, expected <subrequest>!" % element.tag )
    subRequest = SubRequest( element.attrib )
    for fileElement in element.findall( "file" ):
      subRequest += SubReqFile.fromXML( fileElement )
    return subRequest
    
  def __str__( self ):
    """ str operator """
    return ElementTree.tostring( self.toXML() )
