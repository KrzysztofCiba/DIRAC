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


########################################################################
class SubRequest(object):
  """
  .. class:: SubRequest
  

  
  """
  ## sub-request files
  __files = TypedList( allowedTypes = File )

  def __init__( self ):
    """c'tor

    :param self: self reference
    """
    pass

  ## props 

  def requestType():
    """ request type prop """
    doc = "request type"
    def fset( self, value ):
      """ request type setter """
      if value not in ( "diset", "logupload", "register", "removal", "transfer" ):
        raise ValueError( "%s is not a valid request type!" % str(value) )
      self.__requestType = value
    def fget( self ):
      """ request type getter """
      return self.__requestTypex
    return locals()
  requestType = property( **requestType() )

  def operation():
    """ operation prop """
    doc = "operation"
    def fset( self, value ):
      """ operation setter """
      if not self.__requestType:
        raise Exception("unable to set operation when request type is not set!")
      if value not in  { "diset" ; ( "commitRegisters", "setFileStatusForTransformation", "setJobStatusBulk",
                                     "sendXMLBookkeepingReport", "setJobParameters" ),
                         "logupload" : ( "uploadLogFiles", ),
                         "register" : ( "registeFile", ),
                         "removal" : ( "replicaRemoval", "removeFile", "physicalRemoval" ),
                         "transfer" : ( "replicateAndRegister" ) }[self.__requestType]:
        raise ValueError("opearion %s is not valid for %s request type!" % ( str(value), self.__requestType ) )
      self.__operation = value
    def fget( self ):
      """ operation getter """
      return self.__operation
    return locals()
  operation = property( **operation() )
          
  def arguments():
    """ arguments prop """
    doc = "sub-request arguments"
    def fset( self, value ):
      """ arguments setter """
      self.__arguments = value
    def fget( self ):
      """ arguments getter """
      return self.__arguments
    return locals()
  arguments = property( **arguments() )
  
  def sourceSE():
    """ source SE prop """
    doc = "source SE"
    def fset( self, value ):
      """ source SE setter """
      self.__sourceSE = value
    def fget( self ):
      """ source SE getter """
      return self.__sourceSE 
    return locals()
  sourceSE = property( **sourceSE() )
  
  def targetSE():
    """ target SE prop """
    doc = "source SE"
    def fset( self, value ):
      """ target SE setter """
      self.__targetSE = value
    def fget( self ):
      """ target SE getter """
      return self.__targetSE 
    return locals()
  targetSE = property( **targetSE() )
  
  def catalogue():
    """ catalogue prop """
    doc = "catalogue"
    def fset( self, value ):
      """ catalogue setter """
      # TODO check type == list
      self.__catalogue = value
    def fget( self ):
      """ catalogue getter """
      return self.__catalogue 
    return locals()
  catalogue = property( **catalogue() )

  def error():
    """ error prop """
    doc = "error"
    def fset( self, value ):
      """ error setter """
      if type(value) != str:
        raise ValueError("error has to be a string!")
      self.__error = error
    def fget( self ):
      """ error getter """
      return self.__error
    return locals()
  error = property( **error() )

  

  def toXML( self ):
    pass
