'''
Created on 2011-12-14

@author: frank
'''

import logging

import retval


class Helper():
    
    @staticmethod 
    def restful(func):
        
        def restful_call(*args, **kwargs):
            restful_call.__name__ = func.__name__
            func_name = func.__name__         
            logger = logging.getLogger("cleaner")    
            str_args = [str(i) for i in args[1:]]
            logger.debug("function %s arguments %s" %
                        (func_name, '/'.join(str_args)))
            res = None
            try:
                res = func(*args, **kwargs)
                logger.debug("function %s return value: %s" %
                            (func_name, str(res))) 
                return retval.return_value(status=0, val=res)           
            except Exception, e:
                logger.error("function %s exception: %s" %
                             (func_name, str(e)))
                return retval.return_value(status=-1, event="ERROR", 
                                           msg=str(e))
        
        return restful_call    