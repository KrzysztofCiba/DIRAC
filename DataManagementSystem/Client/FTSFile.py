########################################################################
# $HeadURL $
# File: FTSFile.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/08 09:28:29
########################################################################

""" :mod: FTSFile
    =============

    .. module: FTSFile
    :synopsis: class representing a single file in the FTS job
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    class representing a single file in the FTS job
"""

__RCSID__ = "$Id $"

# #
# @file FTSFile.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/08 09:28:45
# @brief Definition of FTSFile class.

# # imports
import os
try:
  import xml.etree.cElementTree as ElementTree
except ImportError:
  import xml.etree.ElementTree as ElementTree

########################################################################
class FTSFile( object ):
  """
  .. class:: FTSFile

  class representing a single file in the FTS job
  """

  def __init__( self, fromDict = None ):
    """c'tor

    :param self: self reference
    :param dict fromDict: data dict
    """
    self._parent = None
    self.__data__ = dict.fromkeys( self.tableDesc()["Fields"].keys(), None )
    self.__data__["Status"] = "Waiting"
    fromDict = fromDict if fromDict else {}
    for attrName, attrValue in fromDict.items():
      if attrName not in self.__data__:
        raise AttributeError( "unknown File attribute %s" % str( attrName ) )
      setattr( self, attrName, attrValue )

  @staticmethod
  def tableDesc():
    """ get table desc """
    return { "Fields" :
             { "FTSFileID": "INTEGER NOT NULL AUTO_INCREMENT",
               "FTSJobID":  "INTEGER",
               "FileID" : "INTEGER",
               "OperationID" : "INTEGER",
               "LFN" : "VARCHAR(255) NOT NULL",
               "Attempt": "INTEGER NOT NULL DEFAULT 0",
               "Checksum" : "VARCHAR(64)",
               "ChecksumType" : "VARCHAR(32)",
               "Size" : "INTEGER",
               "SourceSE" : "VARCHAR(128)",
               "SourceSURL" : "VARCHAR(255)",
               "TargerSE" : "VARCHAR(128)",
               "TargetSURL" : "VARCHAR(255)",
               "Status" : "VARCHAR(32) DEFAULT 'Waiting'",
               "Error" : "VARCHAR(255)"  },
             "PrimaryKey" : [ "FTSFileID" ],
             "Indexes" : { "FTSJobID" : [ "FTSJobID" ], "FTSFileID" : [ "FTSFileID"] } }

  def __setattr__( self, name, value ):
    """ bweare of tpyos!!! """
    if not name.startswith( "_" ) and name not in dir( self ):
      raise AttributeError( "'%s' has no attribute '%s'" % ( self.__class__.__name__, name ) )
    try:
      object.__setattr__( self, name, value )
    except AttributeError, error:
      print name, value, error

  @property
  def FTSFileID( self ):
    """ FTSFileID getter """
    return self.__data__["FTSFileID"]

  @FTSFileID.setter
  def FTSFileID( self, value ):
    """ FTSFileID setter """
    self.__data__["FTSFileID"] = long( value ) if value else 0

  @property
  def FTSJobID( self ):
    """ FTSJobID getter """
    return self.__data__["FTSJobID"]

  @FTSJobID.setter
  def FTSJobID( self, value ):
    """ FTSJobID setter """
    self.__data__["FTSJobID"] = long( value ) if value else 0

  @property
  def OperationID( self ):
    """ OperationID getter """
    return self.__data__["OperationID"]

  @OperationID.setter
  def OperationID( self, value ):
    """ OperationID setter """
    value = long( value ) if value else None
    self.__data__["OperationID"] = value

  @property
  def FileID( self ):
    """ FileID getter """
    return self.__data__["FileID"]

  @FileID.setter
  def FileID( self, value ):
    """ FileID setter """
    value = long( value ) if value else None
    self.__data__["FileID"] = value

  @property
  def LFN( self ):
    """ LFN prop """
    return self.__data__["LFN"]

  @LFN.setter
  def LFN( self, value ):
    """ lfn setter """
    if type( value ) != str:
      raise TypeError( "LFN has to be a string!" )
    if not os.path.isabs( value ):
      raise ValueError( "LFN should be an absolute path!" )
    self.__data__["LFN"] = value

  @property
  def Attempt( self ):
    """ attempt getter """
    return self.__data__["Attempt"]

  @Attempt.setter
  def Attempt( self, value ):
    """ attempt setter """
    value = int( value )
    if value < 0:
      raise ValueError( "Attempt should be a positive integer!" )
    self.__data__["Attempt"] = int( value )

  @Size.setter
  def Size( self, value ):
    """ file size setter """
    value = long( value )
    if value < 0:
      raise ValueError( "Size should be a positive integer!" )
    self.__data__["Size"] = value

  @property
  def Checksum( self ):
    """ checksum prop """
    return self.__data__["Checksum"]

  @Checksum.setter
  def Checksum( self, value ):
    """ checksum setter """
    self.__data__["Checksum"] = str( value )

  @property
  def ChecksumType( self ):
    """ checksum type prop """
    return self.__data__["ChecksumType"]

  @ChecksumType.setter
  def ChecksumType( self, value ):
    """ checksum type setter """
    if str( value ).upper() not in ( "ADLER32", "MD5", "SHA1", "NONE" ):
      raise ValueError( "unknown checksum type: %s" % value )
    self.__data__["ChecksumType"] = str( value ).upper() if value else None

  @property
  def SourceSE( self ):
    """ source SE prop """
    return self.__data__["SourceSE"] if self.__data__["SourceSE"] else ""

  @SourceSE.setter
  def SourceSE( self, value ):
    """source SE setter """
    self.__data__["SourceSE"] = value[:255] if value else ""

  @property
  def SourceSURL( self ):
    """ source SURL getter """
    return self.__data__["SourceSURL"] if self.__data__["SourceSURL"] else ""

  @SourceSURL.setter
  def SourceSURL( self, value ):
    """ source SURL getter """
    self.__data__["SourceSURL"] = value[:255]

  @property
  def TargetSE( self ):
    """ target SE prop """
    return self.__data__["TargetSE"] if self.__data__["TargetSE"] else ""

  @TargetSE.setter
  def TargetSE( self, value ):
    """ target SE setter """
    self.__data__["TargetSE"] = value[:255] if value else ""

  @property
  def TargetSURL( self ):
    """ target SURL getter """
    return self.__data__["TargetSURL"] if self.__data__["TargetSURL"] else ""

  @TargetSURL.setter
  def TargetSURL( self, value ):
    """ target SURL getter """
    self.__data__["TargetSURL"] = value[:255]

  @property
  def Error( self ):
    """ error prop """
    return self.__data__["Error"]

  @Error.setter
  def Error( self, value ):
    """ error setter """
    if type( value ) != str:
      raise TypeError( "Error has to be a string!" )
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
    if value not in ( "Waiting", 'Submitted', 'Executing', 'Finished', 'FinishedDirty', 'Cancelled' ):
      raise ValueError( "Unknown Status: %s!" % str( value ) )
    self.__data__["Status"] = value

  def toXML( self, dumpToStr = False ):
    """ serialize file to XML

    :param bool dumpToStr: dump to str
    """
    dumpToStr = bool( dumpToStr )
    attrs = dict( [ ( k, str( getattr( self, k ) ) if getattr( self, k ) else "" ) for k in self.__data__ ] )
    el = ElementTree.Element( "FTSFile", attrs )
    return { True : el, False : ElementTree.tostring( el ) }[dumpToStr]

  @classmethod
  def fromXML( cls, element ):
    """ build FTSFile form ElementTree.Element :element: """
    if element.tag != "FTSFile":
      raise ValueError( "wrong tag, expected 'FTSFile', got %s" % element.tag )
    fromDict = dict( [ ( key, value ) for key, value in element.attrib.items() if value ] )
    return FTSFile( fromDict )

  def toSQL( self ):
    """ prepare SQL INSERT or UPDATE statement """
    colVals = [ ( "`%s`" % column, "'%s'" % value if type( value ) == str else str( value ) )
                for column, value in self.__data__.items()
                if value and column != "FTSFileID" ]
    query = []
    if self.FTSFileID:
      query.append( "UPDATE `FTSFile` SET " )
      query.append( ", ".join( [ "%s=%s" % item for item in colVals  ] ) )
      query.append( " WHERE `FTSFileID`=%d;\n" % self.FTSFileID )
    else:
      query.append( "INSERT INTO `FTSFile` " )
      columns = "(%s)" % ",".join( [ column for column, value in colVals ] )
      values = "(%s)" % ",".join( [ value for column, value in colVals ] )
      query.append( columns )
      query.append( " VALUES %s;" % values )
    return "".join( query )
