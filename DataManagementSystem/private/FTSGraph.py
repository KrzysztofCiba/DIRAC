########################################################################
# $HeadURL $
# File: FTSGraph.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/05/10 20:02:32
########################################################################
""" :mod: FTSGraph 
    ==============
 
    .. module: FTSGraph
    :synopsis: FTS graph
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    nodes are FTS sites sites and edges are routes between them
"""
__RCSID__ = "$Id: $"
##
# @file FTSGraph.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/05/10 20:03:00
# @brief Definition of FTSGraph class.

## imports 
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.Graph import Graph, Node, Edge
# # from RSS
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import Resources
# # from DMS
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.private.FTSHistoryView import FTSHistoryView

class Site( Node ):
  """
  .. class:: Site

  not too much here, inherited to change the name
  """
  def __init__( self, name, rwAttrs = None, roAttrs = None ):
    """ c'tor """
    Node.__init__( self, name, rwAttrs, roAttrs )

  def __contains__( self, se ):
    """ check if SE is hosted at this site """
    return se in self.SEs

  def __str__( self ):
    return "<site name='%s' SEs='%s' />" % ( self.name, ",".join( self.SEs.keys() ) )

class Route( Edge ):
  """
  .. class:: Route

  class representing transfers between sites
  """
  def __init__( self, fromNode, toNode, rwAttrs = None, roAttrs = None ):
    """ c'tor """
    Edge.__init__( self, fromNode, toNode, rwAttrs, roAttrs )

  @property
  def isActive( self ):
    """ check activity of this channel """
    successRate = 100.0
    attempted = self.SuccessfulFiles + self.FailedFiles
    if attempted:
      successRate *= self.SuccessfulFiles / attempted
    return bool( successRate > self.AcceptableFailureRate )

  @property
  def timeToStart( self ):
    """ get time to start for this channel """
    if not self.isActive:
      return float( "inf" )
    transferSpeed = { "File": self.Fileput,
                      "Throughput": self.Throughput }[self.SchedulingType]
    waitingTransfers = { "File" : self.WaitingFiles,
                         "Throughput": self.WaitingSize }[self.SchedulingType]
    if transferSpeed:
      return waitingTransfers / float( transferSpeed )
    return 0.0

  def __str__( self ):
    return "<route name='%s' timeToStart='%s' waitingFiles='%s' waitingSize='%s' fileput='%s' />" % ( self.name, self.timeToStart, self.WaitingFiles, self.WaitingSize, self.Fileput )

class FTSGraph( Graph ):
  """
  .. class:: FTSGraph

  graph holding FTS transfers (edges) and sites (nodes)
  """
  # # rss client
  __rssClient = None
  # # resources
  __resources = None

  def __init__( self,
                name,
                ftsSites = None,
                ftsHistoryViews = None,
                accFailureRate = 0.75,
                accFailedFiles = 5,
                schedulingType = "Files" ):
    """ c'tor

    :param str name: graph name
    :param list ftsSites: list with FTSSites
    :param list ftshsitoryViews: list with FTSHistoryViews
    :param float accFailureRate: acceptable failure rate
    :param int accFailedFiles: acceptable failed files
    :param str schedulingType: scheduling type
    """
    Graph.__init__( self, "FTSGraph" )
    self.log = gLogger.getSubLogger( name, True )
    self.accFailureRate = accFailureRate
    self.accFailedFiles = accFailedFiles
    self.schedulingType = schedulingType
    self.initialize( ftsSites, ftsHistoryViews )

  def initialize( self, ftsSites = None, ftsHistoryViews = None ):
    """ initialize FTSGraph  given FTSSites and FTSHistoryViews

    :param list ftsSites: list with FTSSites instances
    :param list ftsHistoryViews: list with FTSHistoryViews instances
    """
    self.log.debug( "initializing FTS graph..." )

    ftsSites = ftsSites if ftsSites else []
    ftsHistoryViews = ftsHistoryViews if ftsHistoryViews else []

    sitesDict = self.resources().getEligibleResources( "Storage" )
    if not sitesDict["OK"]:
      return sitesDict
    sitesDict = sitesDict["Value"]

    # # create nodes
    for ftsSite in ftsSites:
      rwDict = dict.fromkeys( sitesDict.get( ftsSite.Name ), {} )
      for se in rwDict:
        rwDict[se] = { "read": False, "write": False }
      site = Site( ftsSite.Name, {"SEs": rwDict, "ServerURI": ftsSite.ServerURI } )
      self.log.debug( "adding site %s using ServerURI %s" % ( ftsSite.Name, ftsSite.ServerURI ) )
      self.addNode( site )

    for sourceSite in self.nodes():
      for destSite in self.nodes():

        rwAttrs = { "WaitingFiles": 0, "WaitingSize": 0,
                    "SuccessfulFiles": 0, "SuccessfulSize": 0,
                    "FailedFiles": 0, "FailedSize": 0,
                    "Fileput": 0.0, "Throughput": 0.0 }

        roAttrs = { "routeName": "%s#%s" % ( sourceSite.name, destSite.name ),
                    "AcceptableFailureRate": self.accFailureRate,
                    "AcceptableFailedFiles": self.accFailedFiles,
                    "SchedulingType": self.schedulingType }

        route = Route( sourceSite, destSite, rwAttrs, roAttrs )
        self.log.debug( "adding route between %s and %s" % ( route.fromNode.name, route.toNode.name ) )
        self.addEdge( route )

    for ftsHistory in ftsHistoryViews:

      route = self.findRoute( ftsHistory.SourceSE, ftsHistory.TargetSE )
      if not route["OK"]:
        self.log.warn( "route between %s and %s not found" % ( ftsHistory.SourceSE, ftsHistory.TargetSE ) )
        continue
      route = route["Value"]

      if ftsHistory.Status in FTSJob.INITSTATES:
        route.WaitingFiles += ftsHistory.Files
        route.WaitingSize += ftsHistory.Size
      elif ftsHistory.Status in FTSJob.TRANSSTATES:
        route.WaitingSize += ftsHistory.Completeness * ftsHistory.Size / 100.0
        route.WaitingFiles += int( ftsHistory.Completeness * ftsHistory.Files / 100.0 )
      elif ftsHistory.Status in FTSJob.FAILEDSTATES:
        route.FailedFiles += ftsHistory.FailedFiles
        route.FailedSize += ftsHistory.FailedSize
      else:  # # FINISHEDSTATES
        route.SuccessfulFiles += ( ftsHistory.Files - ftsHistory.FailedFiles )
        route.SuccessfulSize += ( ftsHistory.Size - ftsHistory.FailedSize )

      route.Fileput = float( route.SuccessfulFiles - route.FailedFiles ) / FTSHistoryView.INTERVAL
      route.Throughput = float( route.SuccessfulSize - route.FailedSize ) / FTSHistoryView.INTERVAL

    self.updateRWAccess()
    self.log.debug( "init done!" )

  def rssClient( self ):
    """ RSS client getter """
    if not self.__rssClient:
      self.__rssClient = ResourceStatus()
    return self.__rssClient

  def resources( self ):
    """ resource helper getter """
    if not self.__resources:
      self.__resources = Resources()
    return self.__resources

  def updateRWAccess( self ):
    """ get RSS R/W for :seList:

    :param list seList: SE list
    """
    self.log.debug( "updateRWAccess: updating RW access..." )
    for site in self.nodes():
      seList = site.SEs.keys()
      rwDict = dict.fromkeys( seList )
      for se in rwDict:
        rwDict[se] = { "read": False, "write": False  }
      for se in seList:
        rAccess = self.rssClient().getStorageElementStatus( se, "ReadAccess" )
        if not rAccess["OK"]:
          self.log.error( rAccess["Message"] )
          continue
        rwDict[se]["read"] = True if rAccess["Value"] in ( "Active", "Degraded" ) else False
        wAccess = self.rssClient().getStorageElementStatus( se, "WriteAccess" )
        if not wAccess["OK"]:
          self.log.error( wAccess["Message"] )
          continue
        rwDict[se]["write"] = True if wAccess["Value"] in ( "Active", "Degraded" ) else False
        self.log.debug( "Site '%s' SE '%s' read %s write %s " % ( site.name, se,
                                                                  rwDict[se]["read"], rwDict[se]["write"] ) )
      site.SEs = rwDict
    return S_OK()

  def findSiteForSE( self, se ):
    """ return FTSSite for a given SE """
    for node in self.nodes():
      if se in node:
        return S_OK( node )
    return S_ERROR( "StorageElement %s not found" % se )

  def findRoute( self, fromSE, toSE ):
    """ find route between :fromSE: and :toSE: """
    for edge in self.edges():
      if fromSE in edge.fromNode.SEs and toSE in edge.toNode.SEs:
        return S_OK( edge )
    return S_ERROR( "FTSGraph: unable to find route between '%s' and '%s'" % ( fromSE, toSE ) )

