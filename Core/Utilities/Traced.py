########################################################################
# $HeadURL $
# File:  Traced.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/08/08 13:29:18
########################################################################
""" :mod: Traced
    ============
 
    .. module: Traced
    :synopsis: watched mutable metaclass
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    watched mutable metaclass tracing all updated indexes or keys
"""

__RCSID__ = "$Id $"

##
# @file Traced.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/08/08 13:29:27
# @brief Definition of Traced  metaclass.

########################################################################
class Traced( type ):
  """
  .. class:: Traced

  metaclass telling if some attrs was updated

  overwrites __setattr__ and __setitem__
  adds updated member function and __updated__ attribute 
  """
  def __new__( cls, cls_name, bases, classdict ):
    """ prepare new class instance """

    def updated( self, element=None, reset=False ):
      """ updates and returns __updated__ list 
      
      :param self: self reference
      :param mixed element: key name or list index
      :param bool reset: flag to zero __updated__ list
      
      :return: __updated__ list when called without arguments 
      """
      if not self.__update__ or reset:
        self.__update__ = list()
      if element and element not in self.__update__:
        self.__update__.append( element )
      return self.__update__

    def trace_setattr( self, name, value ):
      """ __setattr__ tracing value update """
      if name != "__update__":
        if not hasattr( self, name ) or getattr( self, name ) != value:
          self.updated( name )
      bases[0].__setattr__( self, name, value )
      
    def trace_setitem( self, ind, item ):
      """ __setitem__ tracing value update """
      try:
        if bases[0].__getitem__( self, ind ) != item:
          self.updated( ind )
      except KeyError:
        self.updated( ind )
      bases[0].__setitem__( self, ind, item )
   
    classdict["__setattr__"] = trace_setattr
    classdict["__setitem__"] = trace_setitem
    classdict["updated"] = updated 
    classdict["__update__"] = None

    return type.__new__( cls, cls_name, bases, classdict )
  
class TracedDict(dict):
  """ traced dict """
  __metaclass__ = Traced

class TracedList(list):
  """ traced list """
  __metaclass__ = Traced
