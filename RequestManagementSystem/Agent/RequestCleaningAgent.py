########################################################################
# $HeadURL:  $
########################################################################
""" :mod: RequestCleaningAgent
    ==========================

    .. module: RequestCleaningAgent
    :synopsis: The RequestCleaningAgent removes the already executed requests from the database afte a grace period.
"""
# # rcsid
__RCSID__ = "$Id: $" 
# imports
from DIRAC import S_OK
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.Core.Utilities.Time import dateTime, day, second
## agent name
AGENT_NAME = 'RequestManagement/RequestCleaningAgent'

class RequestCleaningAgent(AgentModule):
  """
  .. class:: RequestCleanignAgent

  """
  def initialize(self):
    """ agent initialization """
    self.graceRemovalPeriod = self.am_getOption('GraceRemovalPeriod',7)
    self.checkAssigned = self.am_getOption('CheckAssigned',True)
    self.assignedResetDelay = self.am_getOption('AssignedResetDelay',7200)
    self.requestClient = RequestClient()
    return S_OK()

  def execute(self):
    """ Main execution method """
    
    toDate = dateTime() - day*self.graceRemovalPeriod
    result = self.requestClient.selectRequests({'Status':'Done','ToDate':str(toDate)})
    if not result['OK']:
      return result
    requestDict = result['Value']
    for rID,rName in requestDict.items():

      self.log.verbose("Removing request %s" % rName)
      result = self.requestClient.deleteRequest(rName)
      if not result['OK']:
        self.log.error( 'Failed to delete request %s' % rName, result['Message'] )
      else:
        self.log.info( 'Successfully removed request %d/%s' % ( rID, rName ) )
      
    if self.checkAssigned:
      toDate = dateTime() - second*self.assignedResetDelay
      result = self.requestClient.selectRequests( {'Status' : 'Assigned', 'ToDate' : str(toDate) } )
      if not result['OK']:
        return result
      requestDict = result['Value']
      for rID, rName in requestDict.items():
        self.log.verbose( 'Resetting request %s to Waiting' % rName )
        result = self.requestClient.setRequestStatus( rName, 'Waiting' )
        if not result['OK']:
          self.log.error( 'Failed to reset request %s to Waiting' % rName, result['Message'] )
        else:
          self.log.info( 'Successfully reset request %d/%s to Waiting' % ( rID, rName ) )
    
    return S_OK()  
