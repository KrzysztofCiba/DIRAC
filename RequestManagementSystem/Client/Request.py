########################################################################
# $HeadURL $
# File: Request.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/07/16 13:43:45
########################################################################

""" :mod: Request 
    =======================
 
    .. module: Request
    :synopsis: request implementation
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    request implementation
"""

__RCSID__ = "$Id $"

##
# @file Request.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/07/16 13:44:00
# @brief Definition of Request class.

## imports 

class SubRequest( object ):
  pass

########################################################################
class Request(object):
  """
  .. class:: Request
  
  """
  ## request's name
  __name = None
  ## request's owner DN
  __ownerDN = None
  ## request's owner group
  __ownerGroup = None
  ## DIRAC setup
  __setup = None
  ## list of sub-requests
  __subRequests = []
  ## status
  __status = "Waiting"

  def __init__( self ):
    """c'tor

    :param self: self reference
    """
    self.__name = ""

  ## SubRequest aritmetics

  def __contains__( self, subRequest ):
    """ in operator 
    
    :param self: self reference
    :param SubRequest subRequest: a subRequest 
    """
    return bool( subRequest in self.__subRequests ) 

  def __iadd__( self, subRequest ):
    """ += operator for subRequest

    :param self: self reference
    :param SubRequest subRequest: sub-request to add
    """
    if not isinstance( subRequest, SubRequest ):
      raise TypeError( "wrong type for arg subRequest" )
    if subRequest not in self:
      self.__subRequests.append( subRequest )
      subRequest.parent = self
   return self

  def __isub__( self, subRequest ):
    """ -= operator for subRequest

    :param self: self reference
    :param SubRequest subRequest: sub-request to add
    """
    if not isinstance( subRequest, SubRequest ):
      raise TypeError( "wrong type for arg subRequest" )
    if subRequest in self:
      self.__subRequests.remove( subRequest )
      subRequest.parent = None
    return self
   
  def insertBefore( self, newSubRequest, existingSubRequest ):
    """ insert :newSubRequest: just before :existingSubRequest:

    """
    if existingSubRequest not in self:
      return S_ERROR( "%s is not in" % existingSubRequest )
    if newSubRequest in self:
      return S_ERROR( "%s is already in" % newSubRequest )
    self.__subRequests.insert( self.__subRequests.index( existingSubRequest ), newSubRequest )
    return S_OK()
 
  def insertAfter( self, newSubRequest, existingSubRequest ):
    """ insert :newSubRequest: just after :existingSubRequest: 
    
    :param self: self reference
    :param SubRequest newSubRequest:
    """
    if ( type(newSubRequest), type(existingSubRequest) ) != ( SubRequest, SubRequest ):
      return S_ERROR( "wrong type for arg" )
    if existingSubRequest not in self:
      return S_ERROR( "%s is not in" % existingSubRequest )
    if newSubRequest in self:
      return S_ERROR( "%s is already in" % newSubRequest )
    self.__subRequests.insert( self.__subRequests.index( existingSubRequest )+1, newSubRequest )
    return S_OK()

  ## props

  def ownerDN():
    """ request owner DN prop """
    doc = "request owner DN"
    def fset( self, value ):
      """ request owner DN setter """
      self.__ownerDN = value
    def fget( self ):
      """ request owner DN getter """
      return self.__ownerDN
  ownerDN = property( **ownerDN() )

  def ownerGroup():
    """ request owner group prop """
    doc = "request owner group "
    def fset( self, value ):
      """ request owner group setter """
      self.__ownerGroup = value
    def fget( self ):
      """ request owner group getter """
      return self.__ownerGroup
  ownerGroup = property( **ownerGroup() )

  def setup():
    """ DIRAC setup prop """
    doc = "DIRAC setup"
    def fset( self, value ):
      """ DIRAC setup setter """
      self.__setup = value
    def fget( self ):
      """ DIRAC setup getter """
      return self.__setup
    return locals() 
  setup = property( **setup() )

  def name():
    """ request's name prop """
    doc = "request's name"
    def fset( self, value ):
      """ request name setter """
      self.__name = value
    def fget( self ):
      """ request name getter """
      return self.__name
    return locals()
  name = property( **name() )


  def fromXML( self, xmlString ):
    pass

  def toXML( self ):
    pass

  
