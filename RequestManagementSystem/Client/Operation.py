########################################################################
# $HeadURL$
# File: Operation.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/07/24 12:12:05
########################################################################

""" :mod: Operation
    ===============

    .. module: Operation
    :synopsis: Operation implementation
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    Operation implementation
"""
# for properties
# pylint: disable=E0211,W0612,W0142,E1101,E0102
__RCSID__ = "$Id$"
# #
# @file Operation.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/07/24 12:12:18
# @brief Definition of Operation class.
# # imports
import xml.etree.ElementTree as ElementTree
from xml.parsers.expat import ExpatError
import datetime
# # from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.TypedList import TypedList
from DIRAC.RequestManagementSystem.private.Record import Record
from DIRAC.RequestManagementSystem.Client.File import File

########################################################################
class Operation( Record ):
  """
  .. class:: Operation

  :param long OperationID: OperationID as read from DB backend
  :param long RequestID: parent RequestID
  :param str Status: execution status
  :param str Type: operation to perform
  :param str Arguments: additional arguments
  :param str SourceSE: source SE name
  :param str TargetSE: target SE names as comma separated list
  :param str Catalog: catalog to use as comma separated list
  :param str Error: error string if any
  :param Request parent: parent Request instance
  """

  def __init__( self, fromDict = None ):
    """ c'tor

    :param self: self reference
    :param dict fromDict: attributes dictionary
    """
    Record.__init__( self )
    self._parent = None
    # # sub-request attributes
    # self.__data__ = dict.fromkeys( self.tableDesc()["Fields"].keys(), None )
    now = datetime.datetime.utcnow().replace( microsecond = 0 )
    self.__data__["SubmitTime"] = now
    self.__data__["LastUpdate"] = now
    self.__data__["CreationTime"] = now
    self.__data__["OperationID"] = 0
    self.__data__["RequestID"] = 0
    self.__data__["Status"] = "Queued"
    # # operation files
    self.__files__ = TypedList( allowedTypes = File )
    # # init from dict
    fromDict = fromDict if fromDict else {}
    for fileDict in fromDict.get( "Files", [] ):
      self +=File( fileDict )
    if "Files" in fromDict: del fromDict["Files"]
    for key, value in fromDict.items():
      if key not in self.__data__:
        raise AttributeError( "Unknown Operation attribute '%s'" % key )
      if key != "Order":
        setattr( self, key, value )

  @staticmethod
  def tableDesc():
    """ get table desc """
    return { "Fields" :
             { "OperationID" : "INTEGER NOT NULL AUTO_INCREMENT",
               "RequestID" : "INTEGER NOT NULL",
               "Type" : "VARCHAR(64) NOT NULL",
               "Status" : "ENUM('Waiting', 'Assigned', 'Queued', 'Done', 'Failed', 'Cancelled') "\
                 "DEFAULT 'Queued'",
               "Arguments" : "BLOB",
               "Order" : "INTEGER NOT NULL",
               "SourceSE" : "VARCHAR(255)",
               "TargetSE" : "VARCHAR(255)",
               "Catalog" : "VARCHAR(255)",
               "CreationTime" : "DATETIME",
               "SubmitTime" : "DATETIME",
               "LastUpdate" : "DATETIME" },
             "PrimaryKey" : "OperationID" }

  # # protected methods for parent only
  def _notify( self ):
    """ notify self about file status change """
    fStatus = self.fileStatusList()
    newStatus = self.Status
    # print "1 _notify", fStatus, self.Status, newStatus
    if "Done" in fStatus:
      newStatus = "Done"
    if "Failed" in fStatus:
      newStatus = "Failed"
    if "Waiting" in fStatus or "Scheduled" in fStatus:
      newStatus = "Queued"

    # print "2 _notify", fStatus, self.Status, newStatus
    if newStatus != self.Status:
      self.Status = newStatus
    # print "3 _notify", fStatus, self.Status, newStatus

  def _setQueued( self, caller ):
    """ don't touch """
    if caller == self._parent:
      self.__data__["Status"] = "Queued"

  def _setWaiting( self, caller ):
    """ don't touch as well """
    if caller == self._parent:
      self.__data__["Status"] = "Waiting"

  # # Files arithmetics
  def __contains__( self, subFile ):
    """ in operator """
    return subFile in self.__files__

  def __iadd__( self, subFile ):
    """ += operator """
    self.addFile( subFile )
    return self

  def addFile( self, subFile ):
    """ add :subFile: to operation """
    self.__files__.append( subFile )
    subFile._parent = self
    self._notify()

  # # helpers for looping
  def __iter__( self ):
    """ files iterator """
    return self.__files__.__iter__()

  def __getitem__( self, i ):
    """ [] op for files """
    return self.__files__.__getitem__( i )

  def fileStatusList( self ):
    """ get list of files statuses """
    return [ subFile.Status for subFile in self ]

  def __len__( self ):
    """ nb of subFiles """
    return len( self.__files__ )

  # # properties
  @property
  def RequestID( self ):
    """ RequestID getter (RO) """
    return self._parent.RequestID if self._parent else -1

  @RequestID.setter
  def RequestID( self, value ):
    """ can't set RequestID by hand """
    self.__data__["RequestID"] = self._parent.RequestID if self._parent else -1

  @property
  def OperationID( self ):
    """ OperationID getter """
    return self.__data__["OperationID"]

  @OperationID.setter
  def OperationID( self, value ):
    """ OperationID setter """
    self.__data__["OperationID"] = long( value ) if value else 0

  @property
  def Type( self ):
    """ operation type prop """
    return self.__data__["Type"]

  @Type.setter
  def Type( self, value ):
    """ operation type setter """
    self.__data__["Type"] = str( value )

  @property
  def Arguments( self ):
    """ arguments getter """
    return self.__data__["Arguments"]

  @Arguments.setter
  def Arguments( self, value ):
    """ arguments setter """
    self.__data__["Arguments"] = value if value else ""

  @property
  def SourceSE( self ):
    """ source SE prop """
    return self.__data__["SourceSE"] if self.__data__["SourceSE"] else ""

  @SourceSE.setter
  def SourceSE( self, value ):
    """ source SE setter """
    self.__data__["SourceSE"] = str( value )[:255] if value else ""

  @property
  def sourceSEList( self ):
    """ helper property returning source SEs as a list"""
    return list( set ( [ sourceSE for sourceSE in self.SourceSE.split( "," ) if sourceSE.strip() ] ) )

  @property
  def TargetSE( self ):
    """ target SE prop """
    return self.__data__["TargetSE"] if self.__data__["TargetSE"] else ""

  @TargetSE.setter
  def TargetSE( self, value ):
    """ target SE setter """
    self.__data__["TargetSE"] = value[:255] if value else ""

  @property
  def targetSEList( self ):
    """ helper property returning target SEs as a list"""
    return list( set ( [ targetSE for targetSE in self.TargetSE.split( "," ) if targetSE.strip() ] ) )

  @property
  def Catalog( self ):
    """ catalog prop """
    return self.__data__["Catalog"]

  @Catalog.setter
  def Catalog( self, value ):
    """ catalog setter """
    self.__data__["Catalog"] = value if value else ""

  @property
  def Error( self ):
    """ error prop """
    return self.__data__["Error"]

  @Error.setter
  def Error( self, value ):
    """ error setter """
    self.__data__["Error"] = str( value )[:255] if value else ""

  @property
  def Status( self ):
    """ Status prop """
    return self.__data__["Status"]

  @Status.setter
  def Status( self, value ):
    """ Status setter """
    if value not in ( "Waiting", "Assigned", "Queued", "Failed", "Done" ):
      raise ValueError( "unknown Status '%s'" % str( value ) )
    if value in ( "Failed", "Done" ) and self.__files__:
      fStatuses = self.fileStatusList()
      # # no update
      if "Scheduled" in fStatuses or "Waiting" in fStatuses:
        return
    # # update? notify parent
    old = self.__data__["Status"]
    self.__data__["Status"] = value
    if value != old and self._parent:
      self._parent._notify()

  @property
  def Order( self ):
    """ order prop """
    if self._parent:
      self.__data__["Order"] = self._parent.indexOf( self ) if self._parent else -1
    return self.__data__["Order"]

  @property
  def CreationTime( self ):
    """ operation creation time prop """
    return self.__data__["CreationTime"]

  @CreationTime.setter
  def CreationTime( self, value = None ):
    """ creation time setter """
    if type( value ) not in ( datetime.datetime, str ):
      raise TypeError( "CreationTime should be a datetime.datetime!" )
    if type( value ) == str:
      value = datetime.datetime.strptime( value.split( "." )[0], '%Y-%m-%d %H:%M:%S' )
    self.__data__["CreationTime"] = value

  @property
  def SubmitTime( self ):
    """ subrequest's submit time prop """
    return self.__data__["SubmitTime"]

  @SubmitTime.setter
  def SubmitTime( self, value = None ):
    """ submit time setter """
    if type( value ) not in ( datetime.datetime, str ):
      raise TypeError( "SubmitTime should be a datetime.datetime!" )
    if type( value ) == str:
      value = datetime.datetime.strptime( value.split( "." )[0], '%Y-%m-%d %H:%M:%S' )
    self.__data__["SubmitTime"] = value

  @property
  def LastUpdate( self ):
    """ last update prop """
    return self.__data__["LastUpdate"]

  @LastUpdate.setter
  def LastUpdate( self, value = None ):
    """ last update setter """
    if type( value ) not in ( datetime.datetime, str ):
      raise TypeError( "LastUpdate should be a datetime.datetime!" )
    if type( value ) == str:
      value = datetime.datetime.strptime( value.split( "." )[0], '%Y-%m-%d %H:%M:%S' )
    self.__data__["LastUpdate"] = value

  def toXML( self, dumpToStr = False ):
    """ dump operation to XML """
    data = dict( [ ( key, str( getattr( self, key ) ) if getattr( self, key ) != None else "" ) for key in self.__data__ ] )
    for key, value in data.items():
      if isinstance( value, datetime.datetime ):
        data[key] = str( value )
    element = ElementTree.Element( "operation", data )
    for opFile in self.__files__:
      fileElement = opFile.toXML()
      if not fileElement["OK"]:
        return fileElement
      element.append( fileElement["Value"] )
    return S_OK( { False: element,
                    True: ElementTree.tostring( element ) }[dumpToStr] )

  @classmethod
  def fromXML( cls, element ):
    """ generate Operation instance from :element:

    :param ElementTree.Element element: operation element
    """
    if type( element ) == str:
      try:
        element = ElementTree.fromstring( element )
      except ExpatError, error:
        return S_ERROR( str( error ) )
    if element.tag != "operation":
      return S_ERROR( "wrong tag <%s>, expected <operation>!" % element.tag )
    fromDict = dict( [ ( key, value ) for key, value in element.attrib.items() if value ] )
    operation = Operation( fromDict )
    for fileElement in element.findall( "file" ):
      opFile = File.fromXML( fileElement )
      if not opFile["OK"]:
        return opFile
      operation += opFile["Value"]
    return S_OK( operation )

  def __str__( self ):
    """ str operator """
    return ElementTree.tostring( self.toXML() )

  def toSQL( self ):
    """ get SQL INSERT or UPDATE statement """
    if not getattr( self, "RequestID" ):
      raise AttributeError( "RequestID not set" )
    colVals = [ ( "`%s`" % column, "'%s'" % getattr( self, column )
                  if type( getattr( self, column ) ) in ( str, datetime.datetime ) else str( getattr( self, column ) ) )
                for column in self.__data__
                if getattr( self, column ) and column not in ( "OperationID", "LastUpdate", "Order" ) ]
    colVals.append( ( "`LastUpdate`", "UTC_TIMESTAMP()" ) )
    colVals.append( ( "`Order`", str( self.Order ) ) )
    # colVals.append( ( "`Status`", "'%s'" % str(self.Status) ) )

    query = []
    if self.OperationID:
      query.append( "UPDATE `Operation` SET " )
      query.append( ", ".join( [ "%s=%s" % item for item in colVals  ] ) )
      query.append( " WHERE `OperationID`=%d;\n" % self.OperationID )
    else:
      query.append( "INSERT INTO `Operation` " )
      columns = "(%s)" % ",".join( [ column for column, value in colVals ] )
      values = "(%s)" % ",".join( [ value for column, value in colVals ] )
      query.append( columns )
      query.append( " VALUES %s;\n" % values )
    return S_OK( "".join( query ) )

  def toJSON( self ):
    """ get json digest """
    digest = dict( zip( self.__data__.keys(),
                        [ str( val ) if val else "" for val in self.__data__.values() ] ) )
    digest["RequestID"] = str( self.RequestID )
    digest["Order"] = str( self.Order )
    digest["Files"] = []
    for opFile in self:
      opJSON = opFile.toJSON()
      if not opJSON["OK"]:
        return opJSON
      digest["Files"].append( opJSON["Value"] )
    return S_OK( digest )