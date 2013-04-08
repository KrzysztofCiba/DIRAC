########################################################################
# $HeadURL $
# File: FTSJobFile.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/08 09:28:29
########################################################################

""" :mod: FTSJobFile
    ================

    .. module: FTSJobFile
    :synopsis: class representing a single file in the FTS job
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    class representing a single file in the FTS job
"""

__RCSID__ = "$Id $"

# #
# @file FTSJobFile.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/08 09:28:45
# @brief Definition of FTSJobFile class.

# # imports
try:
  import xml.etree.cElementTree as ElementTree
except ImportError:
  import xml.etree.ElementTree as ElementTree

########################################################################
class FTSJobFile( object ):
  """
  .. class:: FTSJobFile

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
             { "FTSJobFileID": "INTEGER NOT NULL AUTO_INCREMENT",
               "FTSLfnID":  "INTEGER NOT NULL",
               "FTSJobID":  "INTEGER",
               "Attempt": "INTEGER NOT NULL DEFAULT 0",
               "Checksum" : "VARCHAR(64)",
               "ChecksumType" : "VARCHAR(32)",
               "Size" : "INTEGER",
               "SourceSE" : "VARCHAR(128)",
               "SourceSURL" : "VARCHAR(255)",
               "TargerSE" : "VARCHAR(128)",
               "TargetSURL" : "VARCHAR(255)",
               "Status" : "ENUM( 'Waiting', 'Submitted', 'Executing', 'Finished', 'FinishedDirty', 'Cancelled' ) DEFAULT 'Waiting'",
               "Error" : "VARCHAR(255)"  },
             "PrimaryKey" : [ "FTSJobFileID" ],
             "Indexes" : { "FTSJobID" : [ "FTSJobID" ], "FTSJobFileID" : [ "FTSJobFileID"] } }

  def __setattr__( self, name, value ):
    """ bweare of tpyos!!! """
    if not name.startswith( "_" ) and name not in dir( self ):
      raise AttributeError( "'%s' has no attribute '%s'" % ( self.__class__.__name__, name ) )
    try:
      object.__setattr__( self, name, value )
    except AttributeError, error:
      print name, value, error

  @property
  def FTSJobFileID( self ):
    """ FTSJobFileID getter """
    return self.__data__["FTSJobFileID"]

  @FTSJobFileID.setter
  def FTSJobFileID( self, value ):
    """ FTSJobFileID setter """
    self.__data__["FTSJobFileID"] = long( value ) if value else 0

  @property
  def FTSJobID( self ):
    """ FTSJobID getter """
    return self.__data__["FTSJobID"]

  @FTSJobID.setter
  def FTSJobID( self, value ):
    """ FTSJobID setter """
    self.__data__["FTSJobID"] = long( value ) if value else 0

  @property
  def FTSLfnID( self ):
    """ FTSLfnID getter """
    return self.__data__["FTSLfnID"]

  @FTSLfnID.setter
  def FTSLfnID( self, value ):
    """ FTSLfnID setter """
    self.__data__["FTSLfnID"] = long( value ) if value else 0

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

  def toXML( self ):
    """ serialize file to XML """
    attrs = dict( [ ( k, str( getattr( self, k ) ) if getattr( self, k ) else "" ) for k in self.__data__ ] )
    return ElementTree.Element( "ftsjobfile", attrs )

  @classmethod
  def fromXML( cls, element ):
    """ build File form ElementTree.Element :element: """
    if element.tag != "ftsjobfile":
      raise ValueError( "wrong tag, expected 'ftsjobfile', got %s" % element.tag )
    fromDict = dict( [ ( key, value ) for key, value in element.attrib.items() if value ] )
    return FTSJobFile( fromDict )

  def toSQL( self ):
    """ prepare SQL INSERT or UPDATE statement """
    colVals = [ ( "`%s`" % column, "'%s'" % value if type( value ) == str else str( value ) )
                for column, value in self.__data__.items()
                if value and column != "FTSJobFileID" ]
    query = []
    if self.FTSJobFileID:
      query.append( "UPDATE `FTSJobFile` SET " )
      query.append( ", ".join( [ "%s=%s" % item for item in colVals  ] ) )
      query.append( " WHERE `FTSJobFileID`=%d;\n" % self.RequestID )
    else:
      query.append( "INSERT INTO `FTSJobFile` " )
      columns = "(%s)" % ",".join( [ column for column, value in colVals ] )
      values = "(%s)" % ",".join( [ value for column, value in colVals ] )
      query.append( columns )
      query.append( " VALUES %s;" % values )
    return "".join( query )
