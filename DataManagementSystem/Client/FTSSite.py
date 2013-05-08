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

    we need this one to know which site is a part of FTS infrastructure

"""
# for properties
# pylint: disable=E0211,W0612,W0142,E1101,E0102
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
from xml.parsers.expat import ExpatError
# # from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.RequestManagementSystem.private.Record import Record

########################################################################
class FTSSite( Record ):
  """
  .. class:: FTSSite

  site with FTS infrastructure
  """

  def __init__( self, fromDict = None ):
    """c'tor

    :param self: self reference
    :param dict fromDict: data dict
    """
    Record.__init__( self )
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
               "ServerURI":  "VARCHAR(255)" },
             "PrimaryKey": [ "FTSSiteID" ] }

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
    """ Name getter """
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

  def toXML( self, dumpToStr = False ):
    """ serialize FTS site to XML

    :param bool dumpToStr: dump to str
    """
    dumpToStr = bool( dumpToStr )
    attrs = dict( [ ( k, str( getattr( self, k ) ) if getattr( self, k ) else "" ) for k in self.__data__ ] )
    el = ElementTree.Element( "ftssite", attrs )
    return S_OK( { False: el,
                    True: ElementTree.tostring( el ) }[dumpToStr] )

  @classmethod
  def fromXML( cls, element ):
    """ build FTSSite from xml fragment """
    if type( element ) == str:
      try:
        element = ElementTree.fromstring( element )
      except ExpatError, error:
        return S_ERROR( "unable to de-serialize FTSSite from xml: %s" % str( error ) )
    if element.tag != "ftssite":
      return S_ERROR( "wrong tag, expected 'ftssite', got %s" % element.tag )
    fromDict = dict( [ ( key, value ) for key, value in element.attrib.items() if value ] )
    return S_OK( FTSSite( fromDict ) )

  def toSQL( self ):
    """ prepare SQL INSERT or UPDATE statement """
    colVals = [ ( "`%s`" % column, "'%s'" % value if type( value ) == str else str( value ) )
                for column, value in self.__data__.items()
                if value and column != "FTSSiteID" ]
    query = []
    if self.FTSSiteID:
      query.append( "UPDATE `FTSSite` SET " )
      query.append( ", ".join( [ "%s=%s" % item for item in colVals  ] ) )
      query.append( " WHERE `FTSSiteID`=%d;\n" % self.FTSSiteID )
    else:
      query.append( "INSERT INTO `FTSSite` " )
      columns = "(%s)" % ",".join( [ column for column, value in colVals ] )
      values = "(%s)" % ",".join( [ value for column, value in colVals ] )
      query.append( columns )
      query.append( " VALUES %s;" % values )
    return S_OK( "".join( query ) )

  def toJSON( self ):
    """ dump FTSFile to JSON format """
    return S_OK( dict( zip( self.__data__.keys(),
                      [ str( val ) if val else "" for val in self.__data__.values() ] ) ) )