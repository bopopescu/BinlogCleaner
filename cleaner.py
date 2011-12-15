'''
Created on 2011-12-9

@author: frank
'''

import os
import json
import logging.config

import cherrypy

from helper import Helper
from config import CleanerConfig
from persistence import Persistence
from dbinstance import DBInstance
from dbinstance import DBInstanceController
from dbreplica import DBReplica
from dbreplica import DBReplicaController
from worker import ReplicaWorker


class BinlogCleaner():
    
    def __init__(self):
        self.config = CleanerConfig()
        self.persistence = Persistence()
        self.logger = logging.getLogger("cleaner")
        self.dbinstance_controller = DBInstanceController(self.persistence)
        self.dbreplica_controller = DBReplicaController(self.persistence)
        self._init_worker()
        
    def _init_worker(self):
        self.workers = {}
        dbreplicas = self.dbreplica_controller.get_all()
        for dbreplica in dbreplicas:
            worker = ReplicaWorker(self.persistence, dbreplica)
            worker.start()
            self.workers[dbreplica.id] = worker

    @cherrypy.expose
    @Helper.restful
    def dbinstance_add(self, id, host, port=3306, user="root", passwd=""):
        if self.dbinstance_controller.get(id) is not None:
            raise Exception("duplicate instance")
        else:
            dbinstance = DBInstance(id, host, int(port), user, passwd)
            self.dbinstance_controller.add(dbinstance)
            return "ok"

    @cherrypy.expose
    @Helper.restful
    def dbinstance_update(self, id, host, port=3306, user="root", passwd=""):
        if self.dbinstance_controller.get(id) is None:
            raise Exception("instance not exist")
        else:
            dbinstance = DBInstance(id, host, int(port), user, passwd)
            self.dbinstance_controller.update(dbinstance)
            return "ok"    
        
    @cherrypy.expose
    @Helper.restful
    def dbinstance_del(self, id):
        if self.dbinstance_controller.get(id) is None:
            raise Exception("instance not exist")
        else:
            self.dbinstance_controller.delete(id)
            return "ok"
    
    @cherrypy.expose
    @Helper.restful
    def dbinstance_exist(self, id):
        dbinstance = self.dbinstance_controller.get(id)
        if dbinstance is not None:
            return True
        else:
            return False      
    
    @cherrypy.expose
    @Helper.restful
    def dbreplica_add(self, replica_id, master, slaves, 
                      check_period=300, binlog_window=0):
        if self.workers.has_key(replica_id):
            raise Exception("duplicate replication")
        else:
            dbreplica = DBReplica(replica_id, master, slaves, 
                                  int(check_period), 
                                  int(binlog_window))
            worker = ReplicaWorker(self.persistence, dbreplica)
            worker.start()
            self.workers[replica_id] = worker
            self.dbreplica_controller.add(dbreplica)            
            return "ok"
    
    @cherrypy.expose
    @Helper.restful
    def dbreplica_del(self, replica_id):
        if not self.workers.has_key(replica_id):
            raise Exception("replication not exist")
        else:
            worker = self.workers.pop(replica_id)
            worker.stop()
            self.dbreplica_controller.delete(replica_id)            
            return "ok"
    
    @cherrypy.expose
    @Helper.restful
    def dbreplica_add_slave(self, replica_id, slave_id):
        if not self.workers.has_key(replica_id):
            raise Exception("replication not exist")
        elif self.dbinstance_controller.get(slave_id) is None:
            raise Exception("no %s slave instance" % slave_id)
        else:        
            self.dbreplica_controller.add_slave(replica_id, slave_id)
            self.workers[replica_id].stop()
            worker = ReplicaWorker(self.persistence, 
                                   self.dbreplica_controller.get(replica_id))
            worker.start()
            self.workers[replica_id] = worker            
            return "ok"
        
    @cherrypy.expose
    @Helper.restful
    def dbreplica_add_slaves(self, replica_id, slave_ids):
        if not self.workers.has_key(replica_id):
            raise Exception("replication not exist")
        else:
            slaves = json.loads(slave_ids)
            for slave_id in slaves:
                if self.dbinstance_controller.get(slave_id) is None:
                    raise Exception("no %s slave instance" % slave_id)
            self.dbreplica_controller.add_slaves(replica_id,slaves)
            self.workers[replica_id].stop()
            worker = ReplicaWorker(self.persistence, 
                                   self.dbreplica_controller.get(replica_id))
            worker.start()
            self.workers[replica_id] = worker            
            return "ok"        

    @cherrypy.expose
    @Helper.restful
    def dbreplica_del_slave(self, replica_id, slave_id):
        if not self.workers.has_key(replica_id):
            raise Exception("replication not exist")
        else:        
            self.dbreplica_controller.del_slave(replica_id, slave_id)
            self.workers[replica_id].stop()
            worker = ReplicaWorker(self.persistence,
                                   self.dbreplica_controller.get(replica_id))
            worker.start()
            self.workers[replica_id] = worker            
            return "ok"        
    
    @cherrypy.expose
    @Helper.restful    
    def dbreplica_update_check_period(self, replica_id, check_period):
        if not self.workers.has_key(replica_id):
            raise Exception("replication not exist")
        else:
            self.dbreplica_controller.update_check_period(replica_id, 
                                                          check_period)
            self.workers[replica_id].stop()
            worker = ReplicaWorker(self.persistence, 
                                   self.dbreplica_controller.get(replica_id))
            worker.start()
            self.workers[replica_id] = worker            
            return "ok"

    @cherrypy.expose
    @Helper.restful    
    def dbreplica_update_binlog_window(self, replica_id, binlog_window):
        if not self.workers.has_key(replica_id):
            raise Exception("replication not exist")
        else:
            self.dbreplica_controller.update_binlog_window(replica_id, 
                                                           binlog_window)
            self.workers[replica_id].stop()
            worker = ReplicaWorker(self.persistence,
                                   self.dbreplica_controller.get(replica_id))
            worker.start()
            self.workers[replica_id] = worker            
            return "ok"
    
    @cherrypy.expose
    @Helper.restful
    def dbreplica_purge(self, replica_id):
        if not self.workers.has_key(replica_id):
            raise Exception("replication not exist")
        else:
            self.workers[replica_id].purge()
            return "ok"
        
    @cherrypy.expose
    @Helper.restful
    def dbreplica_master_binlogs(self, replica_id):
        if not self.workers.has_key(replica_id):
            raise Exception("replication not exist")
        else:
            return self.workers[replica_id].master_binlogs()
        
    @cherrypy.expose
    @Helper.restful
    def dbreplica_master_status(self, replica_id):
        if not self.workers.has_key(replica_id):
            raise Exception("replication not exist")
        else:
            return self.workers[replica_id].master_status()
        
    @cherrypy.expose
    @Helper.restful
    def dbreplica_slave_status(self, replica_id, slave_id):
        if not self.workers.has_key(replica_id):
            raise Exception("replication not exist")
        else:
            return self.workers[replica_id].slave_status(slave_id)
        
    @cherrypy.expose
    @Helper.restful
    def dbreplica_worker_status(self, replica_id):
        if not self.workers.has_key(replica_id):
            raise Exception("worker not exist")
        else:
            return self.workers[replica_id].isAlive()      
             
    @cherrypy.expose
    @Helper.restful
    def dbreplica_worker_restart(self, replica_id):
        if not self.workers.has_key(replica_id):
            raise Exception("worker not exist")
        else:
            self.workers[replica_id].stop()
            worker = ReplicaWorker(self.persistence,
                                   self.dbreplica_controller.get(replica_id))
            worker.start()
            self.workers[replica_id] = worker
            return "ok"            
            

if __name__ == "__main__":
    
    real_path = os.path.realpath(__file__)
    home_path = os.path.split(real_path)[0]
    os.chdir(home_path)
    
    logging.config.fileConfig('conf/logger.ini')    
    
    try:
        cherrypy.config.update('conf/cherrypy.ini')
    except Exception as e:
        logger = logging.getLogger('cleaner')
        logger.error("initialize BinlogCleaner configuration error: %s" %
                     str(e))
    
    cherrypy.quickstart(BinlogCleaner())  