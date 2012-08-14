########################################################################
# $HeadURL $
# File: SubReqFile.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/08/03 15:02:53
########################################################################

""" :mod: SubReqFile 
    ================
 
    .. module: SubReqFile
    :synopsis: sub-request file
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    sub-request file
"""
# for properties 
# pylint: disable=E0211,W0612,W0142 

__RCSID__ = "$Id $"

##
# @file SubReqFile.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/08/03 15:03:03
# @brief Definition of SubReqFile class.

## imports 
import os
import urlparse
try:
  import xml.etree.cElementTree as ElementTree
except ImportError:
  import xml.etree.ElementTree as ElementTree
from xml.parsers.expat import ExpatError
## from DIRAC
from DIRAC.Core.Utilities.File import checkGuid
from DIRAC.Core.Utilities.Traced import Traced

########################################################################
class SubReqFile( object ):
  """
  .. class:: SubReqFile

  A bag object holding sub-request file attributes.

  :param SubRequest parent: sub-request reference
  
  :param SubRequest parent: reference to parent SubRequest
  """
  __metaclass__ = Traced 

  parent = None
  
  ## SQL and XML description 
  __data__ = dict.fromkeys( ( "FileID", "LFN", "PFN", "GUID", "Size", "SubRequestID",
                              "Addler", "Md5", "Status", "Attempt",  "Error"),  
                            None )
  
  def __init__( self, fromDict=None ):
    """c'tor

    :param self: self reference
    """
    fromDict = fromdict if fromDict else {}
    for attrName, attrValue in fromDict.items():
      if attrName not in self.__data__:
        raise AttributeError( "unknown SubReqFile attribute %s" % str(attrName) )
      setattr( self, attrName, attrValue )
   
  def __eq__( self, other ):
    """ == operator, comparing str """
    return str(self) == str(other)

  ## props  
  def __fileID():
    """ file ID """
    doc = "FileID"
    def fset( self, value ):
      """ FileID setter """
      self.__data__["FileID"] = long(value)
    def fget( self ):
      """ FileID getter """
      return self.__data__["FileID"]
    return locals()
  FileID = property( **__fileID() ) 

  def __subRequestID():
    """ sub request ID, this one is ro """
    doc = "sub request ID"
    def fget( self ):
      """ SubRequestID getter """
      if self.parent:
        return self.parent.SubRequestID
    return locals()
  SubRequestID = property( **__subRequestID() )

  def __size():
    """ file size prop """
    doc = "file size in bytes"
    def fset( self, value ):
      """ file size setter """
      self.__data__["Size"] = long(value)
    def fget( self ):
      """ file size getter """
      return self.__data__["Size"]
    return locals()
  Size = property( **__size() )

  def __lfn():
    """ LFN prop """
    doc = "lfn"
    def fset( self, value ):
      """ lfn setter """
      if type(value) != str:
        raise TypeError("lfn has to be a string!")
      if not os.path.isabs( value ):
        raise ValueError("lfn should be an absolute path!")
      self.__data__["LFN"] = value
    def fget( self ):
      """ lfn getter """
      return self.__data__["LFN"]
    return locals()
  LFN = property( **__lfn() )

  def __pfn():
    """ pfn prop """
    doc = "pfn"
    def fset( self, value ):
      """ pfn setter """
      if type(value) != str:
        raise TypeError("pfn has to be a string!")
      if not urlparse.urlparse( value ).scheme:
        raise ValueError("wrongly formatted URI!")
      self.__data__["PFN"] = value
    def fget( self ):
      """ pfn getter """
      return self.__data__["PFN"]
    return locals()
  PFN = property( **__pfn() )

  def __guid():
    """ GUID prop """
    doc = "GUID"
    def fset( self, value ):
      """ GUID setter """
      if not checkGuid( value ):
        raise TypeError("%s is not a GUID" % str(value) )
      self.__data__["GUID"] = value
    def fget( self ):
      """ GUID getter """
      return self.__data__["GUID"]
    return locals()
  GUID = property( **__guid() )

  def __addler():
    """ ADDLER32 checksum prop """
    doc = "ADDLER32 checksum"
    def fset( self, value ):
      """ ADDLER32 setter """
      self.__data__["Addler"] = value
    def fget( self ):
      """ ADDLER32 getter """
      return self.__data__["Addler"]
    return locals()
  Addler = property( **__addler() ) 

  def __md5():
    """ MD5 checksum prop """
    doc = "MD5 checksum"
    def fset( self, value ):
      """ MD5 setter """
      self.__data__["Md5"] = value
    def fget( self ):
      """ MD5 getter """
      return self.__data__["Md5"] 
    return locals()
  Md5 = property( **__md5() )
  
  def __attempt():
    """ attempt prop """
    doc = "attempt"
    def fset( self, value ):
      """ attempt getter """
      if type( value ) not in (int, long):
        raise TypeError("attempt has to ba an integer")
      self.__data__["Attempt"] = value
    def fget( self ):
      """ attempt getter """
      return self.__data__["Attempt"]
    return locals()
  Attempt = property( **__attempt() )

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
    
  def __status():
    """ status prop """
    doc = "file status"
    def fset( self, value ):
      """ status setter """
      if value not in ( "Waiting", "Failed", "Done", "Scheduled" ):
        raise ValueError( "unknown status: %s" % str(value) )
      self.__data__["Status"] = value
    def fget( self ):
      """ status getter """
      return self.__data__["Status"] 
    return locals()
  Status = property( **__status() )

  ## (de)serialisation   
  def toXML( self ):
    """ serialise SubReqFile to XML """
    attrs = dict( [ ( k, str(v) if v else "") for (k, v) in self.__data__.items() ] )
    return ElementTree.Element( "file", attrs )

  @classmethod
  def fromXML( cls, element ):
    """ build SubReqFile form ElementTree.Element :element: """
    if element.tag != "file":
      raise ValueError("wrong tag, excpected 'file', got %s" % element.tag )
    return SubReqFile( element.attrib )
  
  def __str__( self ):
    """ str operator """
    return ElementTree.tostring( self.toXML() )


