########################################################################
# $HeadURL $
# File: FTSLfn.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/02 14:03:37
########################################################################
""" :mod: FTSLfn
    =============

    .. module: FTSLfn
    :synopsis: class representing a single file in the FTS
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    class representing a single file in the FTS
"""

__RCSID__ = "$Id $"

# #
# @file FTSLfn.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/02 14:03:54
# @brief Definition of FTSLfn class.

# # imports
import os

########################################################################
class FTSLfn( object ):
  """
  .. class:: FTSLfn

  """

  def __init__( self, fromDict = None ):
    """c'tor

    :param self: self reference
    """
    self.__data__ = dict.fromkeys( self.tableDesc()["Fields"].keys(), None )
    self.__data__["Status"] = "Waiting"
    self.__data__["FTSLfnID"] = 0
    fromDict = fromDict if fromDict else {}
    for key, value in fromDict.items():
      if key not in self.__data__:
        raise AttributeError( "Unknown FTSLfn attribute '%s'" % key )
      if value:
        setattr( self, key, value )

  @staticmethod
  def tableDesc():
    """ get table desc """
    return { "Fields" :
              { "FTSLfnID" : "INTEGER NOT NULL AUTO_INCREMENT",
                "FileID" : "INTEGER NOT NULL",
                "LFN" : "VARCHAR(255)",
                "TargetSE" : "VARCHAR(255)",
                "Checksum" : "VARCHAR(64)",
                "ChecksumType" : "VARCHAR(32)",
                "Status" : "ENUM ('Waiting', 'Failed', 'Done', 'Scheduled') DEFAULT 'Waiting'",
                "Error" : "VARCHAR(255)",
               "PrimaryKey" : [ "FTSLfnID" ],
             "Indexes" : { "FTSLfnID" : [ "FTSLfnID" ], "LFN" : [ "LFN" ], "FileID" : ["FileID"] } } }

  def __setattr__( self, name, value ):
    """ bweare of tpyos!!! """
    if not name.startswith( "_" ) and name not in dir( self ):
      raise AttributeError( "'%s' has no attribute '%s'" % ( self.__class__.__name__, name ) )
    try:
      object.__setattr__( self, name, value )
    except AttributeError, error:
      print name, value, error

  @property
  def FTSLfnID( self ):
    """ FTSLfnID getter """
    return self.__data__["FTSLfnID"]

  @FTSLfnID.setter
  def FTSLfnID( self, value ):
    """ FTSLfnID setter """
    self.__data__["FTSLfnID"] = long( value ) if value else 0

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
  def TargetSE( self ):
    """ target SE prop """
    return self.__data__["TargetSE"] if self.__data__["TargetSE"] else ""

  @TargetSE.setter
  def TargetSE( self, value ):
    """ target SE setter """
    self.__data__["TargetSE"] = value[:255] if value else ""

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
  def Checksum( self ):
    """ checksum prop """
    return self.__data__["Checksum"]

  @Checksum.setter
  def Checksum( self, value ):
    """ checksum setter """
    self.__data__["Checksum"] = str( value )

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
    if value not in ( "Waiting", "Failed", "Done", "Scheduled" ):
      raise ValueError( "Unknown Status: %s!" % str( value ) )
    self.__data__["Status"] = value

  def toSQL( self ):
    """ prepare SQL INSERT or UPDATE statement """
    colVals = [ ( "`%s`" % column, "'%s'" % value if type( value ) == str else str( value ) )
                for column, value in self.__data__.items()
                if value and column != "FTSLfnID" ]
    query = []
    if self.FTSLfnIDID:
      query.append( "UPDATE `FTSLfn` SET " )
      query.append( ", ".join( [ "%s=%s" % item for item in colVals  ] ) )
      query.append( " WHERE `FTSLfnID`=%d;\n" % self.RequestID )
    else:
      query.append( "INSERT INTO `FTSLfn` " )
      columns = "(%s)" % ",".join( [ column for column, value in colVals ] )
      values = "(%s)" % ",".join( [ value for column, value in colVals ] )
      query.append( columns )
      query.append( " VALUES %s;" % values )
    return "".join( query )
