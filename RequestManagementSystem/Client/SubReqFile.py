########################################################################
# $HeadURL $
# File: SubReqFile.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/08/03 15:02:53
########################################################################

""" :mod: SubReqFile 
    ================
 
    .. module: SubReqFile
    :synopsis: sub-request file
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    sub-request file
"""

__RCSID__ = "$Id $"

##
# @file SubReqFile.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/08/03 15:03:03
# @brief Definition of SubReqFile class.

## imports 
try:
  import xml.etree.cElementTree as ElementTree
except ImportError:
  import xml.etree.ElementTree as ElementTree
from xml.parsers.expat import ExpatError
## from DIRAC
from DIRAC.Core.Utilities.File import checkGuid

########################################################################
class SubReqFile(object):
  """
  .. class:: SubReqFile

  A bag object holding sub-request file attributes.

  :param SubRequest parent: sub-request reference
  """
  parent = None

  __attrs = dict.fromkeys( ( "FileID", "LFN", "PFN", "GUID", "Size", 
                             "Addler", "Md5", "Status", "Attempt",  "Error"),  
                             None )
  
  def __init__( self, fromDict=None ):
    """c'tor

    :param self: self reference
    """
    if fromDict:
      for attrName, attrValue in fromDict.items():
        if attrName not in self.__attrs:
          raise AttributeError( "unknown SubReqFile attribute %s" % str(attrName) )
        setattr( self, attrName, attrValue )

  def __eq__( self, other ):
    """ == operator, comparing LFNs """
    return self.LFN == other.LFN

  def __subRequestID( ):
    pass

  ## props  
  def __fileID():
    """ file ID """
    doc = "FileID"
    def fset( self, value ):
      """ FileID setter """
      self.__attrs["FileID"] = long(value)
    def fget( self ):
      """ FileID getter """
      return self.__attrs["FileID"]
    return locals()
  FileID = property( **__fileID() ) 

  def __size():
    """ file size prop """
    doc = "file size in bytes"
    def fset( self, value ):
      """ file size setter """
      self.__attrs["Size"] = long(value)
    def fget( self ):
      """ file size getter """
      return self.__attrs["Size"]
    return locals()
  Size = property( **__size() )

  def __lfn():
    """ LFN prop """
    doc = "lfn"
    def fset( self, value ):
      """ lfn setter """
      if type(value) != str:
        raise TypeError("lfn has to be a string!")
      self.__attrs["LFN"] = value
    def fget( self ):
      """ lfn getter """
      return self.__attrs["LFN"]
    return locals()
  LFN = property( **__lfn() )

  def __pfn():
    """ pfn prop """
    doc = "pfn"
    def fset( self, value ):
      """ pfn setter """
      if type(value) != str:
        raise TypeError("pfn has to be a string!")
      self.__attrs["PFN"] = value
    def fget( self ):
      """ pfn getter """
      return self.__attrs["PFN"]
    return locals()
  PFN = property( **__pfn() )

  def __guid():
    """ GUID prop """
    doc = "GUID"
    def fset( self, value ):
      """ GUID setter """
      if not checkGuid( value ):
        raise TypeError("%s is not a GUID" % str(value) )
      self.__attrs["GUID"] = value
    def fget( self ):
      """ GUID getter """
      return self.__attrs["GUID"]
    return locals()
  GUID = property( **__guid() )

  def __addler():
    """ ADDLER32 checksum prop """
    doc = "ADDLER32 checksum"
    def fset( self, value ):
      """ ADDLER32 setter """
      self.__attrs["Addler"] = value
    def fget( self ):
      """ ADDLER32 getter """
      return self.__attrs["Addler"]
    return locals()
  Addler = property( **__addler() ) 

  def __md5():
    """ MD5 checksum prop """
    doc = "MD5 checksum"
    def fset( self, value ):
      """ MD5 setter """
      self.__attrs["Md5"] = value
    def fget( self ):
      """ MD5 getter """
      return self.__attrs["Md5"] 
    return locals()
  Md5 = property( **__md5() )
  
  def __attempt():
    """ attempt prop """
    doc = "attempt"
    def fset( self, value ):
      """ attempt getter """
      if type( value ) not in (int, long):
        raise TypeError("attempt has to ba an integer")
      self.__attrs["Attempt"] = value
    def fget( self ):
      """ attempt getter """
      return self.__attrs["Attempt"]
    return locals()
  Attempt = property( **__attempt() )

  def __error():
    """ error prop """
    doc = "error"
    def fset( self, value ):
      """ error setter """
      if type(value) != str:
        raise ValueError("error has to be a string!")
      self.__attrs["Error"] = value[255:]
    def fget( self ):
      """ error getter """
      return self.__attrs["Error"]
    return locals()
  Error = property( **__error() )
    
  def __status():
    """ status prop """
    doc = "file status"
    def fset( self, value ):
      """ status setter """
      if value not in ( "Waiting", "Failed", "Done", "Scheduled" ):
        raise ValueError( "unknown status: %s" % str(value) )
      self.__attrs["Status"] = value
    def fget( self ):
      """ status getter """
      return self.__attrs["Status"] 
    return locals()
  Status = property( **__status() )

  ## (de)serialisation   

  def toXML( self ):
    """ serialise SubReqFile to XML """
    attrs = dict( [ ( k, str(v) if v else "") for (k, v) in self.__attrs.items() ] )
    return ElementTree.Element( "file", attrs )

  @classmethod
  def fromXML( cls, element ):
    """ build SubReqFile form ElementTree.Element :element: """
    if element.tag != "file":
      raise ValueError("wrong tag, excpected file, got %s" % element.tag )
    return SubReqFile( element.attrib )

  def toSQL( self ):
    """ insert or update """
    if self.FileID and self.parent and self.parent.SubRequestID:
      ## update 
      pass
    else:
      ## insert
      pass
    query = "INSERT INTO `Files` (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s;"
    colNames = ",".join( ["`%s`" % attr for attr in self.__attrs 
                          if self.__attrs[attr] ] )
    colValues = ",".join( [ "%s" % value if type(value) != str else "'%s'" % str(value) 
                            for attr, value in self.__attrs.items() 
                            if value ] )
    updateValues = ",".join( [ "`%s`=%s" % ( attr, value if type(value) != str else "'%s'" % value ) 
                               for (attr, value) in self.__attrs.items() if value ] )
   
    return "INSERT INTO `Files` (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s;" % ( colNames, colValues, updateValues )
  
  def __str__( self ):
    """ str operator """
    return ElementTree.tostring( self.toXML() )


