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
import datetime
## from DIRAC
from DIRAC.Core.Utilities.TypedList import TypedList

class File( object ):
  pass

class SubRequest( object ):
  pass

  
########################################################################
class Request(object):
  """
  .. class:: Request
 
  :param str name: request' name
  :param str ownerDN: request's owner DN
  :param str ownerGroup: request owner group
  :param str setup: DIRAC setup
  :param str sourceComponent: ??? 
  :param int jobID: jobID 
  :param datetime.datetime creationTime: UTC datetime 
  :param datetime.datetime submissionTime: UTC datetime 
  :param datetime.datetime lastUpdate: UTC datetime 
  :param str status: request's status
  :p[aram TypedList subRequests: list of subrequests 
  """
  ## request's name
  __name = None
  ## request's owner DN
  __ownerDN = None
  ## request's owner group
  __ownerGroup = None
  ## DIRAC setup
  __setup = None
  ## source component
  __sourceComponent = None
  ## jobID
  __jobID = 0
  ## creation time
  __creationTime = None 
  ## submission time
  __submissionTime = None 
  ## last update 
  __lastUpdate = None

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
    if subRequest not in self:
      self.__subRequests.append( subRequest )
      subRequest.parent = self
    return self

  def __isub__( self, subRequest ):
    """ -= operator for subRequest

    :param self: self reference
    :param SubRequest subRequest: sub-request to add
    """
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
      if type(value) <> str:
        raise TypeError("ownerDN should be a string!")
      self.__ownerDN = value
    def fget( self ):
      """ request owner DN getter """
      return self.__ownerDN
    return locals()
  ownerDN = property( **ownerDN() )

  def ownerGroup():
    """ request owner group prop """
    doc = "request owner group "
    def fset( self, value ):
      """ request owner group setter """
      if type(value) <> str:
        raise TypeError("ownerGroup should be a string!")
      self.__ownerGroup = value
    def fget( self ):
      """ request owner group getter """
      return self.__ownerGroup
    return locals()
  ownerGroup = property( **ownerGroup() )

  def setup():
    """ DIRAC setup prop """
    doc = "DIRAC setup"
    def fset( self, value ):
      """ DIRAC setup setter """
      if type(value) != str:
        raise TypeError("setu should be a string!")
      self.__setup = value
    def fget( self ):
      """ DIRAC setup getter """
      return self.__setup
    return locals() 
  setup = property( **setup() )

  def sourceComponent():
    """ source component prop """
    doc = "source component "
    def fset( self, value ):
      """ source component setter """
      if type(value) != str:
        raise TypeError("setu should be a string!")
      self.__sourceComponent = value
    def fget( self ):
      """ source component getter """
      return self.__sourceComponent
    return locals() 
  sourceComponent = property( **sourceComponent() )

  def name():
    """ request's name prop """
    doc = "request's name"
    def fset( self, value ):
      """ request name setter """
      if type(value) <> str:
        raise TypeError("name should be a string")
      self.__name = value
    def fget( self ):
      """ request name getter """
      return self.__name
    return locals()
  name = property( **name() )

  def jobID():
    """ jobID prop """
    doc = "jobID"
    def fset( self, value=0 ):
      """ jobID setter """
      if type(value) not in ( long, int ):
        raise TypeError( "jobID as to be an int" )
      self.__jobID = value
    def fget( self ):
      """ jobID getter """
      return self.__jobID
    return locals()
  jobID = property( **jobID() )

  def creationTime():
    """ request's creation time prop """
    doc = "request's creation time"
    def fset( self, value = None ):
      """ creation time setter """
      if type( value != type(datetime.datetime) ):
        raise TypeError("creationTime should be a datetime.datetime!")
      self.__creationTime = value
    def fget( self ):
      """ creation time getter """
      return self.__creationTime
    return locals()
  creationTime = property( **creationTime() )

  def submissionTime():
    """ request's submission time prop """
    doc = "request's submisssion time"
    def fset( self, value = None ):
      """ submission time setter """
      if type( value != type(datetime.datetime) ):
        raise TypeError("submissionTime should be a datetime.datetime!")
      self.__submissionTime = value
    def fget( self ):
      """ submisssion time getter """
      return self.__submissionTime
    return locals()
  submissionTime = property( **submissionTime() )

  def lastUpdate():
    """ last update prop """ 
    doc = "request's last update"
    def fset( self, value = None ):
      """ last update setter """
      if type( value != type(datetime.datetime) ):
        raise TypeError("lastUpdate should be a datetime.datetime!")
      self.__lastUpdate = value
    def fget( self ):
      """ submisssion time getter """
      return self.__lastUpdate
    return locals()
  lastUpdate = property( **lastUpdate() )


  @classmethod
  def fromXML( cls, xmlString ):
    

    pass


  @classmethod
  def toXML( self ):
    pass


if __name__ == "__main__":
  r = Request()
  print r.creationTime
  
