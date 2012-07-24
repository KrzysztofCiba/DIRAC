########################################################################
# $HeadURL $
# File: File.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/07/24 12:12:32
########################################################################

""" :mod: File 
    =======================
 
    .. module: File
    :synopsis: File implementation
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    
"""

__RCSID__ = "$Id $"

##
# @file File.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/07/24 12:12:34
# @brief Definition of File class.

## imports 
from types import StringTypes, LongType, IntType
## DIRAC
from DIRAC.Core.Utilities.File import checkGuid 


########################################################################
class File(object):
  """
  .. class:: File
  
  :param str lfn: LFN
  :param str pfn: optional PFN
  :param long size: file size in bytes
  :param str guid: file's GUID
  :param str adler: ADLER32 checksum if any
  :param str md5: MD5 checksum if any
  :param str status: file status
  :param int attempt:
  :param str error: error if any
  """
  __lfn = None
  __pfn = None
  __size = None
  __guid = None
  __adler = None
  __md5 = None
  __status = None
  __attempt = None
  __error = None

  __parent = None


  def __init__( self ):
    """c'tor

    :param self: self reference
    """
    pass

  def lfn():
    """ lfn prop """
    doc = "lfn"
    def fset( self, value ):
      """ lfn setter """
      if type(value) not in StringTypes:
        raise TypeError( "lfn has to be a str or unicode" )
      self.__lfn = value
    def fget( self ):
      """ lfn getter """
      return self.__lfn 
    return locals()
  lfn = property( **lfn() )  

  def pfn():
    """ pfn prop """
    doc = "pfn"
    def fset( self, value ):
      """ pfn setter """
      if type(value) not in StringTypes:
        raise TypeError("pfn has to be a str or unicode")
      self.__pfn = value
    def fget( self ):
      """ pfn getter """
      return self.__pfn
    return locals()
  pfn = property( **pfn() )

  def size():
    """ size prop """
    doc = "file size in bytes"
    def fset( self, value ):
      """ size setter """
      if type( value ) not in ( IntType, LongType ):
        raise TypeError("size has to be integer or long")
      self.__size = value
    def fget( self ):
      """ size getter """
      return self.__size
    return locals()
  size = property( **size() )

  def guid( self ):
    """ GUID prop """
    doc = "GUID" 
    def fset( self, value ):
      """ GUID setter """
      if not checkGuid( value ):
        raise TypeError("%s is not a proper GUID" % str(value) )
      self.__guid = value
    def fget( self ):
      """ GUID getter """
      return self.__guid
    return locals()
  guid = property( **guid() )

  def adler():
    """ ADLER32 prop """
    doc = "ADLER32 checksum"
    def fset( self, value ):
      if type(value) in StringTypes:
        pass
      elif type(value) in ( IntType, LongType ):
        pass
      

    def fget( self ):
      """ ADLRE32 getter """
      return __adler
    return locals()
  adler = property( **adler() )
    
      
