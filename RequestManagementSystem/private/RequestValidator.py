########################################################################
# $HeadURL$
# File: RequestValidator.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/09/18 07:55:16
########################################################################

""" :mod: RequestValidator 
    =======================
 
    .. module: RequestValidator
    :synopsis: request validator
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    request validator
"""

__RCSID__ = "$Id$"

##
# @file RequestValidator.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/09/18 07:55:37
# @brief Definition of RequestValidator class.

## imports
from DIRAC import S_OK, S_ERROR, gConfig 
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.SubRequest import SubRequest
from DIRAC.RequestManagementSystem.Client.SubReqFile import SubReqFile

########################################################################
class RequestValidator(object):
  """
  .. class:: RequestValidator
  
  """
  requestValidator = ( self.RequestNameSet, self.SubRequestsPresent )
  
  @classmethod
  def validate( request ):
    

    pass

  @staticmethod
  def RequestNameSet( request ):
    if request.RequestName in ( None, "" ):
      return S_ERROR("RequestName not set")
    return S_OK()
    
  @staticmethod
  def SubRequestsPresent( request ):
    if len(request):
      return S_OK()
    return S_ERROR("SubRequests not present in request")

  @staticmethod
  def SubRequestTypeSet( request ):
    for subReq in request:
      if not subReq.RequestType:
        return S_ERROR("SubRequest #%d hasn't got RequestType set" % req.indexOf( subReq ) )
    return S_OK()
      
  @staticmethod
  def SubRequestOperationSet( request ):
    for subReq in request:
      if not subReq.Operation:
        return S_ERROR("SubRequest #%d hasn't got Operation set" % req.indexOf( suReq ) )
    return S_OK()

  @staticmethod
  def OperationRequestTypeMatch( subRequest ):
    pass

