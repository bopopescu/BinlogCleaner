'''
Created on 2011-12-21

@author: frank
'''

import json

import cherrypy

from controller import DBInstanceController
from controller import DBReplicaController
from model import DBInstance
from model import DBReplica
from worker import ReplicaWorker
from helper import Helper


class DBInstanceRestful():
    
    def __init__(self, config, persistence):
        self.config = config
        self.persistence = persistence
        self.controller = DBInstanceController(self.persistence)
        
    @cherrypy.expose
    @Helper.restful
    def add(self, id, host, port=3306, user="root", passwd="", 
            data_dir="/var/lib/mysql"):
        if self.controller.get(id) is not None:
            raise Exception("duplicate instance")
        else:
            dbinstance = DBInstance(id, host, int(port), user, 
                                    passwd, data_dir)
            self.controller.add(dbinstance)
            return "ok"

    @cherrypy.expose
    @Helper.restful
    def update(self, id, host, port=3306, user="root", passwd="",
               data_dir="/var/lib/mysql"):
        if self.controller.get(id) is None:
            raise Exception("instance not exist")
        else:
            dbinstance = DBInstance(id, host, int(port), user, 
                                    passwd, data_dir)
            self.controller.update(dbinstance)
            return "ok"    
        
    @cherrypy.expose
    @Helper.restful
    def delete(self, id):
        if self.controller.get(id) is None:
            raise Exception("instance not exist")
        else:
            self.controller.delete(id)
            return "ok"
    
    @cherrypy.expose
    @Helper.restful
    def exist(self, id):
        dbinstance = self.controller.get(id)
        if dbinstance is not None:
            return True
        else:
            return False

class DBReplicaRestful():
    
    def __init__(self, config, persistence):
        self.config = config
        self.persistence = persistence
        self.controller = DBReplicaController(persistence)
        self.dbinstance_controller = DBInstanceController(persistence)
        self._init_worker()
        
    def _init_worker(self):
        self.workers = {}
        dbreplicas = self.controller.get_all()
        for dbreplica in dbreplicas:
            worker = ReplicaWorker(self.persistence, dbreplica, self.config)
            worker.start()
            self.workers[dbreplica.id] = worker        
        
    @cherrypy.expose
    @Helper.restful
    def add(self, replica_id, name, master, slaves, 
            check_period=60, binlog_window=0, no_slave_purge=1):
        if self.workers.has_key(replica_id):
            raise Exception("duplicate replication")
        else:
            dbreplica = DBReplica(replica_id, name, master, slaves, 
                                  int(check_period), 
                                  int(binlog_window),
                                  int(no_slave_purge))
            worker = ReplicaWorker(self.persistence, dbreplica, self.config)
            worker.start()
            self.workers[replica_id] = worker
            self.controller.add(dbreplica)            
            return "ok"
    
    @cherrypy.expose
    @Helper.restful
    def delete(self, replica_id):
        if not self.workers.has_key(replica_id):
            raise Exception("replication not exist")
        else:
            worker = self.workers.pop(replica_id)
            worker.stop()
            self.controller.delete(replica_id)            
            return "ok"
    
    @cherrypy.expose
    @Helper.restful
    def add_slave(self, replica_id, slave_id):
        if not self.workers.has_key(replica_id):
            raise Exception("replication not exist")
        elif self.dbinstance_controller.get(slave_id) is None:
            raise Exception("no %s slave instance" % slave_id)
        else:        
            self.controller.add_slave(replica_id, slave_id)
            self.workers[replica_id].stop()
            worker = ReplicaWorker(self.persistence, 
                                   self.controller.get(replica_id),
                                   self.config)
            worker.start()
            self.workers[replica_id] = worker            
            return "ok"
        
    @cherrypy.expose
    @Helper.restful
    def add_slaves(self, replica_id, slave_ids):
        if not self.workers.has_key(replica_id):
            raise Exception("replication not exist")
        else:
            slaves = json.loads(slave_ids)
            for slave_id in slaves:
                if self.dbinstance_controller.get(slave_id) is None:
                    raise Exception("no %s slave instance" % slave_id)
            self.controller.add_slaves(replica_id,slaves)
            self.workers[replica_id].stop()
            worker = ReplicaWorker(self.persistence, 
                                   self.controller.get(replica_id),
                                   self.config)
            worker.start()
            self.workers[replica_id] = worker            
            return "ok"        

    @cherrypy.expose
    @Helper.restful
    def del_slave(self, replica_id, slave_id):
        if not self.workers.has_key(replica_id):
            raise Exception("replication not exist")
        else:        
            self.controller.del_slave(replica_id, slave_id)
            self.workers[replica_id].stop()
            worker = ReplicaWorker(self.persistence,
                                   self.controller.get(replica_id),
                                   self.config)
            worker.start()
            self.workers[replica_id] = worker            
            return "ok"        
    
    @cherrypy.expose
    @Helper.restful    
    def update_check_period(self, replica_id, check_period):
        if not self.workers.has_key(replica_id):
            raise Exception("replication not exist")
        else:
            self.controller.update_check_period(replica_id, 
                                                int(check_period))
            self.workers[replica_id].stop()
            worker = ReplicaWorker(self.persistence, 
                                   self.controller.get(replica_id),
                                   self.config)
            worker.start()
            self.workers[replica_id] = worker            
            return "ok"

    @cherrypy.expose
    @Helper.restful    
    def update_binlog_window(self, replica_id, binlog_window):
        if not self.workers.has_key(replica_id):
            raise Exception("replication not exist")
        else:
            self.controller.update_binlog_window(replica_id, 
                                                 int(binlog_window))
            self.workers[replica_id].stop()
            worker = ReplicaWorker(self.persistence,
                                   self.controller.get(replica_id),
                                   self.config)
            worker.start()
            self.workers[replica_id] = worker            
            return "ok"
        
    @cherrypy.expose
    @Helper.restful    
    def update_no_slave_purge(self, replica_id, no_slave_purge):
        if not self.workers.has_key(replica_id):
            raise Exception("replication not exist")
        else:
            self.controller.update_no_slave_purge(replica_id, 
                                                  int(no_slave_purge))
            self.workers[replica_id].stop()
            worker = ReplicaWorker(self.persistence,
                                   self.controller.get(replica_id),
                                   self.config)
            worker.start()
            self.workers[replica_id] = worker            
            return "ok"        
    
    @cherrypy.expose
    @Helper.restful
    def purge(self, replica_id):
        if not self.workers.has_key(replica_id):
            raise Exception("replication not exist")
        else:
            self.workers[replica_id].purge()
            return "ok"
        
    @cherrypy.expose
    @Helper.restful
    def master_binlogs(self, replica_id):
        if not self.workers.has_key(replica_id):
            raise Exception("replication not exist")
        else:
            return self.workers[replica_id].master_binlogs()
        
    @cherrypy.expose
    @Helper.restful
    def master_status(self, replica_id):
        if not self.workers.has_key(replica_id):
            raise Exception("replication not exist")
        else:
            return self.workers[replica_id].master_status()
        
    @cherrypy.expose
    @Helper.restful
    def slave_status(self, replica_id, slave_id):
        if not self.workers.has_key(replica_id):
            raise Exception("replication not exist")
        else:
            return self.workers[replica_id].slave_status(slave_id)
        
    @cherrypy.expose
    @Helper.restful
    def worker_status(self, replica_id):
        if not self.workers.has_key(replica_id):
            raise Exception("worker not exist")
        else:
            return self.workers[replica_id].isAlive()      
             
    @cherrypy.expose
    @Helper.restful
    def worker_restart(self, replica_id):
        if not self.workers.has_key(replica_id):
            raise Exception("worker not exist")
        else:
            self.workers[replica_id].stop()
            worker = ReplicaWorker(self.persistence,
                                   self.controller.get(replica_id))
            worker.start()
            self.workers[replica_id] = worker
            return "ok"     