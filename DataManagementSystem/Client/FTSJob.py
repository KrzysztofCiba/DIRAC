########################################################################
# $HeadURL $
# File: FTSJob.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/02 13:41:20
########################################################################
""" :mod: FTSJob
    ============

    .. module: FTSJob
    :synopsis: class representing FTS job
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    class representing single FTS request
"""

__RCSID__ = "$Id $"

# #
# @file FTSJob.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/02 13:41:37
# @brief Definition of FTSJob class.

# # imports
import os
import datetime
import tempfile
try:
  import xml.etree.cElementTree as ElementTree
except ImportError:
  import xml.etree.ElementTree as ElementTree
from xml.parsers.expat import ExpatError
# # from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.Grid import executeGridCommand
from DIRAC.Core.Utilities.File import checkGuid
# from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.Core.Utilities.TypedList import TypedList
from DIRAC.DataManagementSystem.Client.FTSJobFile import FTSJobFile

########################################################################
class FTSJob( object ):
  """
  .. class:: FTSJob

  class describing one FTS job
  """

  def __init__( self, fromDict = None ):
    """c'tor

    :param self: self reference
    :param dict fromDict: data dict
    """
    self.__data__ = dict.fromkeys( self.tableDesc()["Fields"].keys(), None )
    now = datetime.datetime.utcnow().replace( microsecond = 0 )
    self.__data__["CreationTime"] = now
    self.__data__["SubmitTime"] = now
    self.__data__["LastUpdate"] = now
    self.__data__["Status"] = "Submitted"
    self.__data__["FTSJobID"] = 0
    self.__files__ = TypedList( allowedTypes = FTSJobFile )
    fromDict = fromDict if fromDict else {}
    for key, value in fromDict.items():
      if key not in self.__data__:
        raise AttributeError( "Unknown FTSJob attribute '%s'" % key )
      if value:
        setattr( self, key, value )

  @staticmethod
  def tableDesc():
    """ get table desc """
    return { "Fields" :
             { "FTSJobID" : "INTEGER NOT NULL AUTO_INCREMENT",
               "GUID" :  "VARCHAR(64)",
               "SourceSE" : "VARCHAR(128)",
               "TargerSE" : "VARCHAR(128)",
               "FTSServer" : "VARCHAR(255)",
               "TargetToken": "VARCHAR(255)",
               "SourceToken": "VARCHAR(255)",
               "Size": "INTEGER",
               "Status" : "ENUM( 'Submitted', 'Executing', 'Finished', 'FinishedDirty', 'Cancelled' ) DEFAULT 'Submitted'",
               "Error" : "VARCHAR(255)",
               "CreationTime" : "DATETIME",
               "SubmitTime" : "DATETIME",
               "LastUpdate" : "DATETIME"  },
             "PrimaryKey" : [ "FTSJobID" ],
             "Indexes" : { "FTSJobID" : [ "FTSJobID" ] } }

  def __setattr__( self, name, value ):
    """ bweare of tpyos!!! """
    if not name.startswith( "_" ) and name not in dir( self ):
      raise AttributeError( "'%s' has no attribute '%s'" % ( self.__class__.__name__, name ) )
    try:
      object.__setattr__( self, name, value )
    except AttributeError, error:
      print name, value, error

  @property
  def FTSJobID( self ):
    """ FTSJobID getter """
    return self.__data__["FTSJobID"]

  @FTSJobID.setter
  def FTSJobID( self, value ):
    """ FTSJobID setter """
    self.__data__["FTSJobID"] = long( value ) if value else 0

  @property
  def GUID( self ):
    """ GUID prop """
    return self.__data__["GUID"]

  @GUID.setter
  def GUID( self, value ):
    """ GUID setter """
    if value:
      if type( value ) not in ( str, unicode ):
        raise TypeError( "GUID should be a string!" )
      if not checkGuid( value ):
        raise ValueError( "'%s' is not a valid GUID!" % str( value ) )
    self.__data__["GUID"] = value

  @property
  def FTSServer( self ):
    """ FTSServer getter """
    return self.__data__["FTSServer"]

  @FTSServer.setter
  def FTSServer( self, url ):
    """ FTSServer getter """
    self.__data__["FTSServer"] = url

  @property
  def Error( self ):
    """ error getter """
    return self.__data__["Error"]

  @Error.setter
  def Error( self, error ):
    """ error setter """
    self.__data__["Error"] = str( error )[255:]

  @property
  def Size( self ):
    """ size getter """
    if not self.__data__["Size"]:
      self.__data__["Size"] = sum( [ ftsFile.Size for ftsFile in self ] )
    return self.__data__["Size"]

  @Size.setter
  def Size( self, value ):
    """ size setter """
    if value:
      self.__data__["Size"] = value
    else:
      self.__data__["Size"] = sum( [ ftsFile.Size for ftsFile in self ] )

  @property
  def CreationTime( self ):
    """ creation time getter """
    return self.__data__["CreationTime"]

  @CreationTime.setter
  def CreationTime( self, value = None ):
    """ creation time setter """
    if type( value ) not in ( datetime.datetime, str ) :
      raise TypeError( "CreationTime should be a datetime.datetime!" )
    if type( value ) == str:
      value = datetime.datetime.strptime( value.split( "." )[0], '%Y-%m-%d %H:%M:%S' )
    self.__data__["CreationTime"] = value

  @property
  def SubmitTime( self ):
    """ request's submission time getter """
    return self.__data__["SubmitTime"]

  @SubmitTime.setter
  def SubmitTime( self, value = None ):
    """ submission time setter """
    if type( value ) not in ( datetime.datetime, str ):
      raise TypeError( "SubmitTime should be a datetime.datetime!" )
    if type( value ) == str:
      value = datetime.datetime.strptime( value.split( "." )[0], '%Y-%m-%d %H:%M:%S' )
    self.__data__["SubmitTime"] = value

  @property
  def LastUpdate( self ):
    """ last update getter """
    return self.__data__["LastUpdate"]

  @LastUpdate.setter
  def LastUpdate( self, value = None ):
    """ last update setter """
    if type( value ) not in  ( datetime.datetime, str ):
      raise TypeError( "LastUpdate should be a datetime.datetime!" )
    if type( value ) == str:
      value = datetime.datetime.strptime( value.split( "." )[0], '%Y-%m-%d %H:%M:%S' )
    self.__data__["LastUpdate"] = value

  @property
  def TargetSE( self ):
    """ target SE getter """
    return self.__data__["TargetSE"]

  @TargetSE.setter
  def TargetSE( self, targetSE ):
    """ target SE setter """
    self.__data__["TargetSE"] = targetSE

  @property
  def SourceSE( self ):
    """ source SE getter """
    return self.__data__["SourceSE"]

  @SourceSE.setter
  def SourceSE( self, sourceSE ):
    """ source SE setter """
    self.__data__["SourceSE"] = sourceSE

  @property
  def SourceToken( self ):
    """ source token getter """
    return self.__data__["SourceToken"]

  @SourceToken.setter
  def SourceToken( self, sourceToken ):
    """ source SE setter """
    self.__data__["SourceToken"] = sourceToken

  @property
  def TargetToken( self ):
    """ target token getter """
    return self.__data__["TargetToken"]

  @TargetToken.setter
  def TargetToken( self, targetToken ):
    """ target SE setter """
    self.__data__["TargetToken"] = targetToken

  # # FTSJobFiles aritmetics
  def __contains__( self, subFile ):
    """ in operator """
    return subFile in self.__files__

  def __iadd__( self, subFile ):
    """ += operator """
    if subFile not in self:
      self.__files__.append( subFile )
      subFile._parent = self
    return self

  def __add__( self, ftsFile ):
    """ + operator """
    self +=ftsFile

  def addFile( self, ftsFile ):
    """ add :ftsFile: to FTS job """
    self +=ftsFile

  # # helpers for looping
  def __iter__( self ):
    """ files iterator """
    return self.__files__.__iter__()

  def __getitem__( self, i ):
    """ [] op for files """
    return self.__files__.__getitem__( i )

  def fileStatusList( self ):
    """ get list of files statuses """
    return [ ftsFile.Status for ftsFile in self ]

  def __len__( self ):
    """ nb of subFiles """
    return len( self.__files__ )

  def _surlPairs( self ):
    """ create and return SURL pair file """
    surls = []
    for ftsFile in self:
      checksum = "%s:%s" % ( ftsFile.ChecksumType, ftsFile.Checksum )
      if len( checksum ) == 1:
        checksum = ""
      surls.append( "%s %s %s" % ( ftsFile.SourceSURL, ftsFile.TargetSURL, checksum ) )
    return "\n".join( surls )

  def submitFTS2( self ):
    """ submit fts job using FTS2 client """
    if self.GUID:
      return S_ERROR( "FTSJob already has been submitted" )
    surls = self._surlPairs()
    if not surls:
      return S_ERROR( "No files to submit" )
    fd, fileName = tempfile.mkstemp()
    surlFile = os.fdopen( fd, 'w' )
    surlFile.write( surls )
    surlFile.close()
    submitCommand = [ "glite-transfer-submit", "-s", self.FTSServer, "-f", fileName, "-o", "--compare-checksums" ]
    submit = executeGridCommand( "", submitCommand )
    os.remove( fileName )
    if not submit["OK"]:
      return submit
    returnCode, output, errStr = submit["Value"]
    if not returnCode == 0:
      return S_ERROR( errStr )
    self.GUID = output.replace( "\n", "" )
    return S_OK()

  def monitorFTS2( self ):
    """ monitor fts job """
    if not self.GUID:
      return S_ERROR( "GUID not set, FTS job not submitted?" )
    monitorCommand = [ "glite-transfer-status", "--verbose", "-s", self.FTSServer, self.GUID, "-l" ]
    monitor = executeGridCommand( "", monitorCommand )
    if not monitor['OK']:
      return monitor
    returnCode, outputStr, errStr = monitor["Value"]
    # Returns a non zero status if error
    if not returnCode:
      return S_ERROR( errStr )
    return S_OK( outputStr )

  def submitFTS3( self ):
    """ placeholder for FTS3 """
    pass

  def monitorFTS3( self ):
    """ placeholder for FTS3 """
    pass

  def toSQL( self ):
    """ prepare SQL INSERT or UPDATE statement

    :return: str with SQL fragment
    """
    colVals = [ ( "`%s`" % column, "'%s'" % value if type( value ) in ( str, datetime.datetime ) else str( value ) )
                for column, value in self.__data__.items()
                if value and column not in  ( "FTSJobID", "LastUpdate" ) ]
    colVals.append( ( "`LastUpdate`", "UTC_TIMESTAMP()" ) )
    query = []
    if self.FTSJobID:
      query.append( "UPDATE `FTSJob` SET " )
      query.append( ", ".join( [ "%s=%s" % item for item in colVals  ] ) )
      query.append( " WHERE `FTSJobID`=%d;\n" % self.RequestID )
    else:
      query.append( "INSERT INTO `FTSJob` " )
      columns = "(%s)" % ",".join( [ column for column, value in colVals ] )
      values = "(%s)" % ",".join( [ value for column, value in colVals ] )
      query.append( columns )
      query.append( " VALUES %s;" % values )
    return "".join( query )

  @classmethod
  def fromXML( cls, xmlString ):
    """ create FTSJob object from xmlString or xml.ElementTree.Element

    :param str xmlString: XML fragment
    :return: S_OK(FTSJob)/S_ERROR
    """
    try:
      root = ElementTree.fromstring( xmlString )
    except ExpatError, error:
      return S_ERROR( "unable to de-serialize FTSJob from xml: %s" % str( error ) )
    if root.tag != "ftsjob":
      return S_ERROR( "unable to de-serialize FTSJob, xml root element is not a 'ftsjob'" )
    ftsJob = FTSJob( root.attrib )
    for ftsJobFile in root.findall( "ftsjobfile" ):
      ftsJob.addFile( FTSJobFile.fromXML( element = ftsJobFile ) )
    return S_OK( ftsJob )

  def toXML( self ):
    """ dump request to XML

    :param self: self reference
    :return: S_OK( xmlString )/S_ERROR
    """
    root = ElementTree.Element( "ftsjob" )
    # # attributes
    root.attrib["FTSJobID"] = str( self.FTSJobID ) if self.FTSJobID else ""
    root.attrib["Error"] = str( self.Error ) if self.Error else ""
    root.attrib["GUID"] = str( self.GUID ) if self.GUID else ""
    root.attrib["FTSServer"] = str( self.FTSServer ) if self.FTSServer else ""
    root.attrib["SourceSE"] = self.SourceSE
    root.attrib["TargetSE"] = self.TargetSE
    root.attrib["Size"] = str( self.Size )
    # # always calculate status, never set
    root.attrib["Status"] = str( self.Status )
    # # datetime up to seconds
    root.attrib["CreationTime"] = self.CreationTime.isoformat( " " ).split( "." )[0] if self.CreationTime else ""
    root.attrib["SubmitTime"] = self.SubmitTime.isoformat( " " ).split( "." )[0] if self.SubmitTime else ""
    root.attrib["LastUpdate"] = self.LastUpdate.isoformat( " " ).split( "." )[0] if self.LastUpdate else ""
    # # trigger xml dump of a whole operations and their files tree
    for ftsFile in self.__files__:
      root.append( ftsFile.toXML() )
    xmlStr = ElementTree.tostring( root )
    return S_OK( xmlStr )