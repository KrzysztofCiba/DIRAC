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

  :param SubRequest _parent: reference to parent SubRequest
  :param dict __data__: attrs dict
  """
  __metaclass__ = Traced 
  
  def __init__( self, fromDict=None ):
    """c'tor

    :param self: self reference
    """
    self._parent = None
    self.__data__ = dict.fromkeys( ( "FileID", "SubRequestID", "LFN", "PFN", "GUID", "Size",  
                                     "Addler", "Md5", "Status", "Attempt", "Error" ) ) 
    self.__data__["Status"] = "Waiting"
    self.__data__["Attempt"] = 1
    self.__data__["SubRequestID"] = 0
    self.__data__["FileID"] = 0
    fromDict = fromDict if fromDict else {}
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
      value = long(value) if value else None
      self.__data__["FileID"] = value
    def fget( self ):
      """ FileID getter """
      return self.__data__["FileID"]
    return locals()
  FileID = property( **__fileID() ) 

  def __subRequestID():
    """ sub request ID, this one is ro """
    doc = "sub request ID"
    def fset( self, value ):
      """ SubRequestID setter """
      value = long(value) if value else None
      if self._parent and self._parent.SubRequestID != value:
        raise ValueError("Parent SubRequestID mismatch (%s != %s)" % ( self._parent.SubRequestID, value ) )
      self.__data__["SubRequestID"] = value
    def fget( self ):
      """ SubRequestID getter """
      if self._parent:
        self.__data__["SubRequestID"] = self._parent.SubRequestID
      return self.__data__["SubRequestID"]
    return locals()
  SubRequestID = property( **__subRequestID() )

  def __size():
    """ file size prop """
    doc = "file size in bytes"
    def fset( self, value ):
      """ file size setter """
      value = long(value)
      if value < 0:
        raise ValueError("Size should be a positive integer!")
      self.__data__["Size"] = value
    def fget( self ):
      """ file size getter """
      return self.__data__["Size"]
    return locals()
  Size = property( **__size() )

  def __lfn():
    """ LFN prop """
    doc = "LFN"
    def fset( self, value ):
      """ lfn setter """
      if type(value) != str:
        raise TypeError("LFN has to be a string!")
      if not os.path.isabs( value ):
        raise ValueError("LFN should be an absolute path!")
      self.__data__["LFN"] = value
    def fget( self ):
      """ lfn getter """
      return self.__data__["LFN"]
    return locals()
  LFN = property( **__lfn() )

  def __pfn():
    """ PFN prop """
    doc = "PFN"
    def fset( self, value ):
      """ PFN setter """
      if value:
        if type(value) != str:
          raise TypeError("PFN has to be a string!")
        if not urlparse.urlparse( value ).scheme:
          raise ValueError("Wrongly formatted URI!")
      self.__data__["PFN"] = value
    def fget( self ):
      """ PFN getter """
      return self.__data__["PFN"]
    return locals()
  PFN = property( **__pfn() )

  def __guid():
    """ GUID prop """
    doc = "GUID"
    def fset( self, value ):
      """ GUID setter """
      if value:
        if type(value) not in ( str, unicode ):
          raise TypeError("GUID should be a string!")
        if not checkGuid( value ):
          raise ValueError("'%s' is not a valid GUID!" % str(value) )
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
      self.__data__["Addler"] = str(value)
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
      self.__data__["Md5"] = str(value)
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
      self.__data__["Attempt"] = int(value)
    def fget( self ):
      """ attempt getter """
      if not self.__data__["Attempt"]:
        self.__data__["Attempt"] = 1
      return self.__data__["Attempt"]
    return locals()
  Attempt = property( **__attempt() )

  def __error():
    """ error prop """
    doc = "error"
    def fset( self, value ):
      """ error setter """
      if type(value) != str:
        raise TypeError("Error has to be a string!")
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
        raise ValueError( "Unknown Status: %s!" % str(value) )
      self.__data__["Status"] = value
    def fget( self ):
      """ status getter """
      if not self.__data__["Status"]:
        self.__data__["Status"] = "Waiting"
      return self.__data__["Status"] 
    return locals()
  Status = property( **__status() )

  ## (de)serialisation   
  def toXML( self ):
    """ serialise SubReqFile to XML """
    attrs = dict( [ ( k, str( getattr(self, k) ) if getattr(self, k) else "") for k in self.__data__ ] )
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

  def toSQL( self ):
    """ get SQL INSERT or UPDATE statement """
    colVals = [ ( "`%s`" % column, "'%s'" % value if type(value) == str else str(value) ) 
                for column, value in self.__data__.items()
                if value and column != "FileID" ] 
    query = []
    if self.FileID:
      query.append( "UPDATE `Files` SET " )
      query.append( ", ".join( [ "%s=%s" % item for item in colVals  ] ) )
      query.append( " WHERE `FileID`=%d;\n" % self.FileID )
    else:
      query.append( "INSERT INTO Files " )
      columns = "(%s)" % ",".join( [ column for column, value in colVals ] )
      values = "(%s)" % ",".join( [ value for column, value in colVals ] )
      query.append( columns )
      query.append(" VALUES %s;\n" % values )
    return "".join( query )
