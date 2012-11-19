########################################################################
# $HeadURL $
# File: StateMachine.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/09/26 09:39:58
########################################################################

""" :mod: StateMachine 
    =======================
 
    .. module: StateMachine
    :synopsis: generic state machine 
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    generic state machine 
"""

__RCSID__ = "$Id $"

##
# @file StateMachine.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/09/26 09:40:12
# @brief Definition of StateMachine class.

## for WeakValuesDictionary
import weakref
## from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.Graph import Graph, Node, Edge 

class Observable( type ):
  """ 
  .. class:: Observable 

  metaclass to create observable pattern
  """
  def __new__( cls, name, bases, classdict ):     

    def observers( self ):
      """ get observers dict """
      if not hasattr( self, "__observers" ):
        setattr( self, "__observers", weakref.WeakValueDictionary() )
      return getattr( self, "__observers" )  
  
    def notifySetAttr( func ):
      """ to be applied exclusively on __setattr__ """
      def wrapper( *args, **kwargs ):
        instance, attr, newVal = args[0], args[1], args[2]
        oldVal = getattr( instance, attr ) if hasattr( instance, attr ) else None
        ret = func( *args, **kwargs )
        if oldVal != getattr( instance, attr ) and attr in instance.observers():
          instance._notify( attr, oldVal )
        return ret
      return wrapper

    def registerObserver( self, observer, watchedAttribute ):
      """ add new :observer: """
      ## check observer 
      if not isinstance( observer, Observer ):
        raise TypeError("registerObserver: supplied argument for observer should be inherited from Observer class")
      ## check for attribute, could raise AttributeError 
      getattr( self, watchedAttribute )
      if watchedAttribute not in self.observers():
        self.observers()[watchedAttribute] = observer 

    def unregisterObserver( self, observer, watchedAttribute=None ):
      """ remove :observer: """
      if not watchedAttribute:
        for watchedAttribute, regObserver in self.observers().items():
          if regObserver == observer:
            del self.observers()[watchedAttribute]
      else:
        if watchedAttribute in self.observers():
          del self.observers()[watchedAttribute]

    def _notify( self, attribute=None, oldVal=None ):
      """ notify observers """
      if attribute and attribute in self.observers():
        self.observers()[attribute].notify( attribute, oldVal, getattr(self, attribute), self )
      else:
        for attribute, observer in self.observers().items():
          observer.notify( attribute, oldVal, getattr(self, attribute), self )

    ## add functions 
    classdict["observers"] = observers
    classdict["registerObserver"] = registerObserver
    classdict["unregisterObserver"] = unregisterObserver
    classdict["_notify"] = _notify
    aType = type.__new__( cls, name, bases, classdict )
    ## decorate setattr
    aType.__setattr__ = notifySetAttr( aType.__setattr__ )
    return aType 

class Observer( object ):
  """
  .. class:: Observer
  
  generic class for Observer pattern
  """
  def notify( self, attr, oldVal, newVal, observable ):
    """ callback fruntion from :observable: on :attribute: change

    :param attr: attribute name
    :param mixed oldVal: previous value 
    :param mixed newVal: new value 
    :param instance observable: observable
    """
    raise NotImplementedError("'notify' has to be implemented in the child class")

class State( Node ):
  """
  .. class:: State

  genetric state
  """
  def __init__( self, stateName, entryAction=None, exitAction=None ):
    """ c'tor 
   
    :param str stateName: state name 
    :param callable entryAction: function to call on state entry
    :param callable exitAction: function to call on state exit
    """
    if entryAction: 
      if not callable( entryAction ):
        raise TypeError( "entryAction should be callable")  
    if exitAction: 
      if not callable( exitAction ):
        raise TypeError( "exitAction should be callable")
    Node.__init__( self, stateName, roAttrs = { "entryAction" : entryAction, 
                                                "exitAction" : exitAction } )

  def __call__( self, context, *args, **kwargs ):
    """ make it callable """
    if context == "entry" and self.entryAction:
      return self.entryAction( *args, **kwargs )
    elif context == "exit" and self.exitAction:
      return self.exitAction( *args, **kwargs )
    return S_OK()

class Action( Edge ):
  """ 
  .. class:: Action

  """
  def __init__( self, fromState, toState, action ):
    """ c'tor
    
    :param State fromState: start state
    :param State toState: end state
    :param callable action: state changing function
    """
    Edge.__init__( self, fromState, toState )
    if not callable( action ):
      raise TypeError("action argument should be callable")
    self.makeProperty( "action", action )
      
  def __call__( self, *args, **kwargs ):      
    """ """
    self.fromNode( "exit", *args, **kwargs )
    action = self.action( *args, **kwargs )
    if action["OK"]:
      self.graph.setCurrentState( self.toNode )
      self.toNode( "entry", *args, **kwargs )
    return action
      
class StateMachine( Graph ):
  """
  .. class:: StateGraph

  using directed graph
  """  

  def __init__( self, name ):
    """ c'tor """
    Graph.__init__( self, name )
    self.currentState = None

  def setCurrentState( self, state ):
    if state not in self:
      raise ValueError( "state not known!" )
    self.currentState = state

  def getCurrentState( self ):
    return self.currentState

  def step( self, *args, **kwargs ):
    """ """
    action = self.currentState.edges()[0]
    action( *args, **kwargs )
    self.currentState = action.toNode
    

class DoorsClosed( State ):
  
  def __init__( self ):
    State.__init__( self, "closed", self.OnEntryAction, self.OnExitAction )

  def OnEntryAction( self, *args, **kwargs ):
    print self, " entry action"
    return S_OK()

  def OnExitAction( self, *args, **kwargs ):
    print self, " exit action"
    return S_OK()


class DoorsOpened( State ):
  
  def __init__( self ):
    State.__init__( self, "opened", self.OnEntryAction, self.OnExitAction )

  def OnExitAction( self ):
    print self, " exit action"
    return S_OK()

  def OnEntryAction( self ):
    print self, " entry action"
    return S_OK()


class Closing( Action ):
  
  def __init__( self, fromState, toState ):
    Action.__init__( self, fromState, toState, self.OnAction )

  def OnAction( self, *args, **kwargs ):
    print self, " closing door now"
    return S_OK()

class Doors( Observer ):  
  __metaclass__ = Observable 
  state = "open"

  def notify(  self, attr, oldVal, newVal, observable ):
    print attr, oldVal, newVal, observable 
    

class DoorsSTM( StateMachine ):
  
  def __init__( self ):
    
    StateMachine.__init__( self, "doors" )
    
    doorsClosed = DoorsClosed()
    doorsOpened = DoorsOpened()
    
    closing = Closing( doorsOpened, doorsClosed )

    self.addNode( doorsClosed )
    self.addNode( doorsOpened )

if __name__ == "__main__":

  stm = DoorsSTM()
  stm.setCurrentState( stm.getNode( "opened" ) )

  stm.step( )
    
  curr = stm.getCurrentState()
  print curr
    
    

     
    
