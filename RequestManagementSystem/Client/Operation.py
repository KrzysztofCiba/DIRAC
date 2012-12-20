########################################################################
# $HeadURL$
# File: Operation.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/07/24 12:12:05
########################################################################

""" :mod: Operation 
    =======================
 
    .. module: Operation
    :synopsis: Operation implementation
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    Operation implementation
"""
# for properties 
# pylint: disable=E0211,W0612,W0142,E1101,E0102 
__RCSID__ = "$Id$"
##
# @file Operation.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/07/24 12:12:18
# @brief Definition of Operation class.
## imports 
try:
  import xml.etree.cElementTree as ElementTree
except ImportError:
  import xml.etree.ElementTree
from xml.parsers.expat import ExpatError
import datetime
import itertools
## from DIRAC
from DIRAC import S_OK
from DIRAC.Core.Utilities.TypedList import TypedList
from DIRAC.RequestManagementSystem.Client.File import File

########################################################################
class Operation(object):
  """
  .. class:: Operation
 
  :param long OperationID: OperationID as read from DB backend
  :param long RequestID: parent RequestID 
  :param str Status: execution status
  :param str Type: operation to perform
  :param str Arguments: additional arguments
  :param str SourceSE: source SE name
  :param str TargetSE: target SE names as comma separated list
  :param str Catalogue: catalogue to use as comma separated list
  :param str Error: error string if any
  :param Request parent: parent Request instance
  """
 
  def __init__( self, fromDict=None ):
    """ c'tor

    :param self: self reference
    :param dict fromDict: attributes dictionary
    """
    self._parent = None
    ## sub-request attributes
    self.__data__ = dict.fromkeys( ( "RequestID",  "OperationID", "Status", "Error", 
                                     "Type",  "Arguments", "Order", "SourceSE", "TargetSE", 
                                     "Catalogue", "SubmitTime", "LastUpdate" ) )
    now = datetime.datetime.utcnow().replace( microsecond=0 )
    self.__data__["SubmitTime"] = now
    self.__data__["LastUpdate"] = now
    self.__data__["OperationID"] = 0
    self.__data__["RequestID"] = 0
    self.__data__["Status"] = "Queued"
    ## operation files
    self.__files__ = TypedList( allowedTypes = File )
    ## init from dict
    fromDict = fromDict if fromDict else {}
    for key, value in fromDict.items():
      if key not in self.__data__:
        raise AttributeError("Unknown Operation attribute '%s'" % key )
      setattr( self, key, value )

  def __setattr__( self, name, value ):
    """ beawre of tpyos """
    if not name.startswith("_") and name not in dir(self):
      raise AttributeError("'%s' has no attribute '%s'" % ( self.__class__.__name__, name ) )
    try:
      object.__setattr__( self, name, value )
    except AttributeError, error:
      print name, value, error

  ## protected methods for parent only
  def _notify( self ):
    """ notify self about file status change """
    if "Scheduled" not in self.fileStatusList() and "Waiting" not in self.fileStatusList():
      self.Status = "Done"
    else:
      self.Status = "Queued"

  def _setQueued( self, caller ):
    """ don't touch """
    if caller == self._parent:
      self.__data__["Status"] = "Queued"
    
  def _setWaiting( self, caller ):
    """ don't touch as well """
    if caller == self._parent:
      self.__data__["Status"] = "Waiting"

  ## Files aritmetics 
  def __contains__( self, subFile ):
    """ in operator """
    return subFile in self.__files__

  def __iadd__( self, subFile ):
    """ += operator """
    if subFile not in self:
      self.__files__.append( subFile )
      subFile._parent = self 
      self._notify()
    return self

  def __add__( self, subFile ):
    """ + operator """
    self += subFile
      
  def addFile( self, subFile ):
    """ add :subFile: to subrequest """
    self += subFile

  ## helpers for looping
  def __iter__( self ):
    """ subrequest files iterator """
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

  ## properties  
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
    self.__data__["OperationID"] = long(value) if value else 0

  @property
  def Type( self ):
    """ operation type prop """
    return self.__data__["Type"]

  @Type.setter
  def Type( self, value ):
    """ operation type setter """
    self.__data__["Type"] = str(value)
          
  @property
  def Arguments( self):
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
    self.__data__["SourceSE"] = str(value)[:32] if value else ""
    
  @property
  def TargetSE( self ):
    """ target SE prop """
    return self.__data__["TargetSE"]

  @TargetSE.setter
  def TargetSE( self, value ):
    """ target SE setter """
    self.__data__["TargetSE"] = value[:255] if value else ""
  
  @property
  def Catalogue( self ):
    """ catalogue prop """
    return self.__data__["Catalogue"]
  
  @Catalogue.setter
  def Catalogue( self, value ):
    """ catalogue setter """
    self.__data__["Catalogue"] = value if value else ""

  @property
  def Error( self ):
    """ error prop """
    return self.__data__["Error"]

  @Error.setter
  def Error( self, value ):
    """ error setter """
    self.__data__["Error"] = str(value)[:255] if value else ""

  @property
  def Status( self ):
    """ Status prop """
    return self.__data__["Status"]

  @Status.setter
  def Status( self, value ):
    """ Status setter """
    if value not in ( "Waiting", "Assigned", "Queued", "Failed", "Done" ):
      raise ValueError("unknown Status '%s'" % str(value) )
    if value in ( "Failed", "Done" ) and self.__files__:
      if "Waiting" in self.fileStatusList() or "Scheduled" in self.fileStatusList():
        return 
    ## update? notify parent
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
  def SubmitTime( self ):
    """ subrequest's submission time prop """
    return self.__data__["SubmitTime"]

  @SubmitTime.setter
  def SubmitTime( self, value = None ):
    """ submission time setter """
    if type(value) not in ( datetime.datetime, str ):
      raise TypeError("SubmissionTime should be a datetime.datetime!")
    if type(value) == str:
      value = datetime.datetime.strptime( value.split(".")[0], '%Y-%m-%d %H:%M:%S' )
    self.__data__["SubmitTime"] = value
 
  @property
  def LastUpdate( self ):
    """ last update prop """
    return self.__data__["LastUpdate"]
  
  @LastUpdate.setter
  def LastUpdate( self, value = None ):
    """ last update setter """
    if type( value ) not in ( datetime.datetime, str ):
      raise TypeError("LastUpdate should be a datetime.datetime!")
    if type(value) == str:
      value = datetime.datetime.strptime( value.split(".")[0], '%Y-%m-%d %H:%M:%S' )
    self.__data__["LastUpdate"] = value

  def toXML( self ):
    """ dump subrequest to XML """
    data = dict( [ ( key, str(getattr(self, key)) if getattr(self, key) != None else "" ) for key in self.__data__ ] )
    for key, value in data.items():
      if isinstance( value, datetime.datetime ):
        data[key] = str(value)
    element = ElementTree.Element( "operation", data ) 
    for opFile in self.__files__:
      element.append( opFile.toXML() )
    return element
  
  @classmethod
  def fromXML( cls, element ):
    """ generate Operation instance from :element: 
    
    :param ElementTree.Element element: subrequest element
    """
    if not isinstance( element, type(ElementTree.Element("operation")) ):
      raise TypeError("wrong argument type %s, excpected ElementTree.Element" % type(element) )
    if element.tag != "operation":
      raise ValueError("wrong tag <%s>, expected <operation>!" % element.tag )
    fromDict = dict( [ (key, value) for key, value in element.attrib.items() if value ] ) 
    operation = Operation( fromDict )
    for fileElement in element.findall( "file" ):
      operation += File.fromXML( fileElement )
    return operation

  def __str__( self ):
    """ str operator """
    return ElementTree.tostring( self.toXML() )

  def toSQL( self ):
    """ get SQL INSERT or UPDATE statement """
    if not getattr( self, "RequestID" ):
      raise AttributeError( "RequestID not set" )
    colVals = [ ( "`%s`" % column, "'%s'" % getattr( self, column ) 
                  if type(getattr(self, column)) in ( str, datetime.datetime ) else str( getattr(self, column) ) ) 
                for column in self.__data__
                if getattr(self, column) and column not in ( "OperationID", "LastUpdate", "Order" ) ] 
    colVals.append( ("`LastUpdate`", "UTC_TIMESTAMP()" ) )
    colVals.append( ( "`Order`", str(self.Order) ) )
    #colVals.append( ( "`Status`", "'%s'" % str(self.Status) ) )

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
      query.append(" VALUES %s;\n" % values )
    return "".join( query )
