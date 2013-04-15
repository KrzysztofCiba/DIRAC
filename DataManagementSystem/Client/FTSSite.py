########################################################################
# $HeadURL $
# File: FTSSite.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/15 12:33:08
########################################################################
""" :mod: FTSSite
    =============

    .. module: FTSSite
    :synopsis: class representing FTS site
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    class representing FTS site
"""

__RCSID__ = "$Id $"

# #
# @file FTSSite.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/15 12:33:21
# @brief Definition of FTSSite class.

# # imports
try:
  import xml.etree.cElementTree as ElementTree
except ImportError:
  import xml.etree.ElementTree as ElementTree


########################################################################
class FTSSite( object ):
  """
  .. class:: FTSSite

  """

  def __init__( self, fromDict = None ):
    """c'tor

    :param self: self reference
    :param dict fromDict: data dict
    """
    self.__data__ = dict.fromkeys( self.tableDesc()["Fields"].keys(), None )
    self.__data__["Status"] = "Active"
    fromDict = fromDict if fromDict else {}
    for attrName, attrValue in fromDict.items():
      if attrName not in self.__data__:
        raise AttributeError( "unknown FTSSite attribute %s" % str( attrName ) )
      setattr( self, attrName, attrValue )

  @staticmethod
  def tableDesc():
    """ get table desc """
    return { "Fields" :
             { "FTSSiteID": "INTEGER NOT NULL AUTO_INCREMENT",
               "Name": "VARCHAR(255) NOT NULL",
               "ServerURI":  "VARCHAR(255)",
               "SEs": "BLOB" },
             "PrimaryKey": [ "FTSSiteID" ] }

  def __setattr__( self, name, value ):
    """ bweare of tpyos!!! """
    if not name.startswith( "_" ) and name not in dir( self ):
      raise AttributeError( "'%s' has no attribute '%s'" % ( self.__class__.__name__, name ) )
    try:
      object.__setattr__( self, name, value )
    except AttributeError, error:
      print name, value, error

  @property
  def FTSSiteID( self ):
    """ FTSSiteID getter """
    return self.__data__["FTSSiteID"]

  @FTSSiteID.setter
  def FTSSiteID( self, value ):
    """ FTSSiteID setter """
    self.__data__["FTSSiteID"] = value

  @property
  def Name( self ):
    """ name getter """
    return self.__data__["Name"]

  @Name.setter
  def Name( self, value ):
    """ Name setter """
    self.__data__["Name"] = value

  @property
  def ServerURI( self ):
    """ FTS server uri getter """
    return self.__data__["ServerURI"]

  @ServerURI.setter
  def ServerURI( self, value ):
    """ server uri setter """
    self.__data__["ServerURI"] = value

  @property
  def SEs( self ):
    """ SEs getter """
    return self.__data__["SEs"]

  @SEs.setter
  def SEs( self, value ):
    """ SEs setter """
    self.__data__["SEs"] = value

  def getSEsTuple( self ):
    """ return SEs as tuple """
    return tuple( [ se.strip() for se in self.SEs.split( "," ) if se.strip() ] )


  def toXML( self, dumpToStr = False ):
    """ serialize FTS site to XML

    :param bool dumpToStr: dump to str
    """
    dumpToStr = bool( dumpToStr )
    attrs = dict( [ ( k, str( getattr( self, k ) ) if getattr( self, k ) else "" ) for k in self.__data__ ] )
    el = ElementTree.Element( "FTSSite", attrs )
    return { True : el, False : ElementTree.tostring( el ) }[dumpToStr]

  @classmethod
  def fromXML( cls, element ):
    """ build FTSSite form ElementTree.Element :element: """
    if element.tag != "FTSSite":
      raise ValueError( "wrong tag, expected 'FTSSite', got %s" % element.tag )
    fromDict = dict( [ ( key, value ) for key, value in element.attrib.items() if value ] )
    return FTSSite( fromDict )

  def toSQL( self ):
    """ prepare SQL INSERT or UPDATE statement """
    colVals = [ ( "`%s`" % column, "'%s'" % value if type( value ) == str else str( value ) )
                for column, value in self.__data__.items()
                if value and column != "FTSSiteID" ]
    query = []
    if self.FTSFileID:
      query.append( "UPDATE `FTSSite` SET " )
      query.append( ", ".join( [ "%s=%s" % item for item in colVals  ] ) )
      query.append( " WHERE `FTSSiteID`=%d;\n" % self.FTSSiteID )
    else:
      query.append( "INSERT INTO `FTSSite` " )
      columns = "(%s)" % ",".join( [ column for column, value in colVals ] )
      values = "(%s)" % ",".join( [ value for column, value in colVals ] )
      query.append( columns )
      query.append( " VALUES %s;" % values )
    return "".join( query )

