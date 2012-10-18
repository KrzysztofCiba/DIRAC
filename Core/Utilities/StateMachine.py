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

class State( object ):
  
  def __init__( self, stateName ):
    self.__stateName = stateName 

  def stateName( self ):
    return self.__stateName 

  def __call__( self, event ):
    self.action( event )
      
class TranstionTable( dict ):
  """
  .. class:: TransitionTable
  """  
  def addTransition( stateA, stateB, event, action ):
    dict.__setitem__[stateA] = ( stateB, event, action )

########################################################################
class StateMachine( Observer ):
  """
  .. class:: StateMachine
  
  """  
  def __init__( self ):
    """ c'tor """
    self.transTable = TranstionTable()
  
  def notify( self, attr, oldVal, newVal, observable ):
    
    pass

  def registerState( self, state ):

    pass

  
