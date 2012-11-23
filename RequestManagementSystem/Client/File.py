########################################################################
# $HeadURL $
# File: File.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/08/03 15:02:53
########################################################################

""" :mod: File 
    ================
 
    .. module: File
    :synopsis: sub-request file
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    sub-request file
"""
# for properties 
# pylint: disable=E0211,W0612,W0142 

__RCSID__ = "$Id $"

##
# @file File.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/08/03 15:03:03
# @brief Definition of File class.

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
class File( object ):
  """
  .. class:: File

  A bag object holding sub-request file attributes.

  :param SubRequest _parent: reference to parent SubRequest
  :param dict __data__: attrs dict
  """  
   
  def __init__( self, fromDict=None ):
    """c'tor

    :param self: self reference
    """
    self._parent = None
    self.__data__ = dict.fromkeys( ( "FileID", "OperationID", "Status", "Error", "LFN", 
                                     "PFN", "Size", "ChecksumType", "Checksum", "GUID" ) ) 
    self.__data__["Status"] = "Waiting"
    self.__data__["OperationID"] = 0
    self.__data__["FileID"] = 0
    fromDict = fromDict if fromDict else {}
    for attrName, attrValue in fromDict.items():
      if attrName not in self.__data__:
        raise AttributeError( "unknown File attribute %s" % str(attrName) )
      setattr( self, attrName, attrValue )

  def __setattr__( self, name, value ):
    if not name.startswith("_") and name not in dir(self):
      raise AttributeError("'%s' has no attribute '%s'" % ( self.__class__.__name__, name ) )
    object.__setattr__( self, name, value )

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

  def __operationID():
    """ operation ID, this one is ro """
    doc = "operation ID"
    def fset( self, value ):
      """ OperationID setter """
      value = long(value) if value else None
      if self._parent and self._parent.OperationID != value:
        raise ValueError("parent OperationID mismatch (%s != %s)" % ( self._parent.OperationID, value ) )
      self.__data__["OperationID"] = value
    def fget( self ):
      """ OperationID getter """
      if self._parent:
        self.__data__["OperationID"] = self._parent.OperationID
      return self.__data__["OperationID"]
    return locals()
  OperationID = property( **__operationID() )

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

  def __checksumType():
    """ checksum type prop """
    doc = "ChecksumType"
    def fset( self, value = "NONE" ):
      """ checksum type setter """
      value = str(value).upper()
      if value not in ( "ADLER", "MD5", "SHA1", "NONE" ):
        raise ValueError("unknown checksum type: %s" % value )
      self.__data__["ChecksumType"] = value
    def fget( self ):
      """ ChecksumType getter """
      return self.__data__["ChecksumType"]
    return locals()
  ChecksumType = property( **__checksumType() )

  def __checksum():
    """ checksum prop """
    doc = "checksum"
    def fset( self, value ):
      """ checksum setter """
      self.__data__["Checksum"] = str(value)
    def fget( self ):
      """ checksum getter """
      return self.__data__["Checksum"]
    return locals()
  Checksum = property( **__checksum() ) 
  
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
    """ serialise File to XML """
    attrs = dict( [ ( k, str( getattr(self, k) ) if getattr(self, k) else "") for k in self.__data__ ] )
    return ElementTree.Element( "file", attrs )

  @classmethod
  def fromXML( cls, element ):
    """ build File form ElementTree.Element :element: """
    if element.tag != "file":
      raise ValueError("wrong tag, excpected 'file', got %s" % element.tag )
    return File( element.attrib )
  
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
      query.append( "UPDATE `File` SET " )
      query.append( ", ".join( [ "%s=%s" % item for item in colVals  ] ) )
      query.append( " WHERE `FileID`=%d;\n" % self.FileID )
    else:
      query.append( "INSERT INTO File " )
      columns = "(%s)" % ",".join( [ column for column, value in colVals ] )
      values = "(%s)" % ",".join( [ value for column, value in colVals ] )
      query.append( columns )
      query.append(" VALUES %s;\n" % values )
    return "".join( query )
