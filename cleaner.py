'''
Created on 2011-12-9

@author: frank
'''

import os
import logging.config

import cherrypy

from config import CleanerConfig
from persistence import Persistence
from restful import DBInstanceRestful
from restful import DBReplicaRestful
        

def init_path():
    real_path = os.path.realpath(__file__)
    home_path = os.path.split(real_path)[0]
    os.chdir(home_path)
    
def init_logger():
    logging.config.fileConfig('conf/logger.ini')    
            

def init_cherrypy():
    cherrypy.config.update('conf/cherrypy.ini')
    
def start_service():
    config = CleanerConfig()
    persistence = Persistence(config)
    cherrypy.tree.mount(root=DBInstanceRestful(config, persistence),
                        script_name="/dbinstance")
    cherrypy.tree.mount(root=DBReplicaRestful(config, persistence), 
                        script_name="/dbreplica") 
    cherrypy.quickstart(None, "/")         

if __name__ == "__main__":
    init_path()
    init_logger()
    init_cherrypy()
    start_service()