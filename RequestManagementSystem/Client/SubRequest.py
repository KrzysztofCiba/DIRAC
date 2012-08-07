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
## from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.TypedList import TypedList
from DIRAC.RequestManagementSystem.Client.SubReqFile import SubReqFile

########################################################################
class SubRequest(object):
  """
  .. class:: SubRequest
  

  
  """
  ## sub-request files
  __files = TypedList( allowedTypes = SubReqFile )
  
  __attrs = dict.fromkeys( ( "RequestType", "Operation", "Arguments", 
                             "SourceSE", "TargetSE", "Catalogue", "Error" ), None )

  def __init__( self, fromDict=None ):
    """c'tor

    :param self: self reference
    """
    pass

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
  def __requestType():
    """ request type prop """
    doc = "request type"
    def fset( self, value ):
      """ request type setter """
      if value not in ( "diset", "logupload", "register", "removal", "transfer" ):
        raise ValueError( "%s is not a valid request type!" % str(value) )
      self.__attrs["RequestType"] = value
    def fget( self ):
      """ request type getter """
      return self.__attrs["RequestType"]
    return locals()
  RequestType = property( **__requestType() )

  def __operation():
    """ operation prop """
    doc = "operation"
    def fset( self, value ):
      """ operation setter """
      if not self.RequestType:
        raise Exception("unable to set operation when request type is not set!")
      if value not in { "diset" : ( "commitRegisters", "setFileStatusForTransformation", "setJobStatusBulk",
                                    "sendXMLBookkeepingReport", "setJobParameters" ),
                        "logupload" : ( "uploadLogFiles", ),
                        "register" : ( "registeFile", ),
                        "removal" : ( "replicaRemoval", "removeFile", "physicalRemoval" ),
                        "transfer" : ( "replicateAndRegister" ) }[ self.RequestType ]:
        raise ValueError("opearion %s is not valid for %s request type!" % ( str(value),  self.RequestType ) )
      self.__attrs["Operation"] = value
    def fget( self ):
      """ operation getter """
      return  self.__attrs["Operation"]
    return locals()
  Operation = property( **__operation() )
          
  def __arguments():
    """ arguments prop """
    doc = "sub-request arguments"
    def fset( self, value ):
      """ arguments setter """
      self.__attrs["Arguments"] = value
    def fget( self ):
      """ arguments getter """
      return self.__attrs["Arguments"]
    return locals()
  Arguments = property( **__arguments() )
  
  def __sourceSE():
    """ source SE prop """
    doc = "source SE"
    def fset( self, value ):
      """ source SE setter """
      self.__attrs["SourceSE"] = value
    def fget( self ):
      """ source SE getter """
      return self.__attrs["SourceSE"] 
    return locals()
  SourceSE = property( **__sourceSE() )
  
  def __targetSE():
    """ target SE prop """
    doc = "source SE"
    def fset( self, value ):
      """ target SE setter """
      self.__attrs["TargetSE"] = value
    def fget( self ):
      """ target SE getter """
      return self.__attrs["TargetSE"]
    return locals()
  TargetSE = property( **__targetSE() )
  
  def __catalogue():
    """ catalogue prop """
    doc = "catalogue"
    def fset( self, value ):
      """ catalogue setter """
      # TODO check type == list or comma separated str 
      self.__attrs["Catalogue"] = value
    def fget( self ):
      """ catalogue getter """
      return self.__attrs["Catalogue"]
    return locals()
  Catalogue = property( **__catalogue() )

  def __error():
    """ error prop """
    doc = "error"
    def fset( self, value ):
      """ error setter """
      if type(value) != str:
        raise ValueError("error has to be a string!")
      self.__attrs["Error"] = value[255:]
    def fget( self ):
      """ error getter """
      return self.__attrs["Error"]
    return locals()
  Error = property( **__error() )

  def toXML( self ):
    """ dump subrequest to XML """
    element = ElementTree.Element( "subrequest", self.__attrs ) 
    for subFile in self.__files:
      element.insert( subFile.toXML() )
    return element
  
  @classmethod
  def fromXML( cls  ):
    
