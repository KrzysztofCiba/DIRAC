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
# pylint: disable=E0211,W0612,W0142,E1101,E0102 

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
    :param dict fromDict: property dict 
    """
    self._parent = None
    self.__data__ = dict.fromkeys( ( "FileID", "OperationID", "Status", "Error", "LFN", "Attempt",
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
    """ beawre of tpyos """
    if not name.startswith("_") and name not in dir(self):
      raise AttributeError("'%s' has no attribute '%s'" % ( self.__class__.__name__, name ) )
    #print name, value
    object.__setattr__( self, name, value )

  def __eq__( self, other ):
    """ == operator, comparing str """
    return str(self) == str(other)

  ## props  
  @property
  def FileID( self ):
    """ FileID getter """
    return self.__data__["FileID"]
  
  @FileID.setter
  def FileID( self, value ):
    """ FileID setter """
    value = long(value) if value else None
    self.__data__["FileID"] = value

  @property
  def OperationID( self ):
    """ operation ID (RO) """
    return self._parent.OperationID if self._parent else 0

  @property
  def Attempt( self ):
    """ attempt getter """
    return self.__data__["Attempt"]
  
  @Attempt.setter
  def Attempt( self, value ):
    """ attempt setter """
    value = int(value)
    if value < 0:
      raise ValueError("Attempt should be a positive integer!")
    self.__data__["Attempt"] = int(value)
    
  @property
  def Size( self ):
    """ file size getter """
    return self.__data__["Size"]

  @Size.setter
  def Size( self, value ):
    """ file size setter """
    value = long(value)
    if value < 0:
      raise ValueError("Size should be a positive integer!")
    self.__data__["Size"] = value
   
  @property
  def LFN( self ):
    """ LFN prop """
    return self.__data__["LFN"]
    
  @LFN.setter
  def LFN( self, value ):
    """ lfn setter """
    if type(value) != str:
      raise TypeError("LFN has to be a string!")
    if not os.path.isabs( value ):
      raise ValueError("LFN should be an absolute path!")
    self.__data__["LFN"] = value

  @property
  def PFN( self ):
    """ PFN prop """
    return self.__data__["PFN"]
  
  @PFN.setter
  def PFN( self, value ):
    """ PFN setter """
    if type(value) != str:
      raise TypeError("PFN has to be a string!")
    if not urlparse.urlparse( value ).scheme:
      raise ValueError("Wrongly formatted URI!")
    self.__data__["PFN"] = value

  @property
  def GUID( self ):
    """ GUID prop """
    return self.__data__["GUID"]

  @GUID.setter
  def GUID( self, value ):
    """ GUID setter """
    if value:
      if type(value) not in ( str, unicode ):
        raise TypeError("GUID should be a string!")
      if not checkGuid( value ):
        raise ValueError("'%s' is not a valid GUID!" % str(value) )
    self.__data__["GUID"] = value
    
  @property
  def ChecksumType( self ):
    """ checksum type prop """
    return self.__data__["ChecksumType"]

  @ChecksumType.setter
  def ChecksumType( self, value = "NONE" ):
    """ checksum type setter """
    value = str(value).upper()
    if value not in ( "ADLER", "MD5", "SHA1", "NONE" ):
      raise ValueError("unknown checksum type: %s" % value )
    self.__data__["ChecksumType"] = value

  @property
  def Checksum( self ):
    """ checksum prop """
    return self.__data__["Checksum"]
  
  @Checksum.setter
  def Checksum( self, value ):
    """ checksum setter """
    self.__data__["Checksum"] = str(value)
  
  @property
  def Error( self ):
    """ error prop """
    return self.__data__["Error"]
  
  @Error.setter
  def Error( self, value ):
    """ error setter """
    if type(value) != str:
      raise TypeError("Error has to be a string!")
    self.__data__["Error"] = value[255:]

  @property
  def Status( self ):
    """ status prop """
    if not self.__data__["Status"]:
      self.__data__["Status"] = "Waiting"
    return self.__data__["Status"] 

  @Status.setter
  def Status( self, value ):
    """ status setter """
    if value not in ( "Waiting", "Failed", "Done", "Scheduled" ):
      raise ValueError( "Unknown Status: %s!" % str(value) )
    self.__data__["Status"] = value
  
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
    fromDict = dict( [ (key, value) for key, value in element.attrib.items() if value ] ) 
    return File( fromDict )
  
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
      query.append( "INSERT INTO `File` " )
      columns = "(%s)" % ",".join( [ column for column, value in colVals ] )
      values = "(%s)" % ",".join( [ value for column, value in colVals ] )
      query.append( columns )
      query.append(" VALUES %s;\n" % values )
    return "".join( query )
