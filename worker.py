'''
Created on 2011-12-14

@author: frank
'''

import json
import threading
import logging
import time

import MySQLdb

from dbreplica import DBReplicaController
from dbinstance import DBInstanceController

class ReplicaWorker(threading.Thread):
    
    def __init__(self, persistence, dbreplica):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger("cleaner")
        self.lock = threading.Lock()
        self.stopped = False
        self.dbreplica = dbreplica
        self.dbreplica_controller = DBReplicaController(persistence)
        self.dbinstance_controller = DBInstanceController(persistence)
        self._init_replica()
        self._init_handler()
    
    
    def _init_replica(self):
        master_id = self.dbreplica.master
        if self.dbreplica.slaves is None or len(self.dbreplica.slaves) == 0:
            slave_ids = []
        else:
            slave_ids = json.loads(self.dbreplica.slaves)
        master = self.dbinstance_controller.get(master_id)
        if master is None:
            raise ReplicaWorkerException("no master %s record" % master_id)
        self.master = master
        self.slaves = {}
        for slave_id in slave_ids:
            slave = self.dbinstance_controller.get(slave_id)
            if slave is None:
                raise ReplicaWorkerException("no slave %s record" % slave_id)
            self.slaves[slave_id] = slave
    
    def _init_handler(self):
        self.master_handler = ReplicaMasterHandler(self.master)
        self.slaves_handler = {}
        for slave_id in self.slaves.keys():
            slave = self.slaves[slave_id]
            self.slaves_handler[slave_id] = ReplicaSlaveHandler(slave)
    
    def _earliest_slave_binlog(self):
        earliest_log_index = -1
        earliest_log_name = None
        for slave_id in self.slaves_handler.keys():
            slave_handler = self.slaves_handler[slave_id]
            (log_index, log_name) = slave_handler.master_binlog()
            if earliest_log_index < 0 or earliest_log_index > log_index:
                earliest_log_index = log_index
                earliest_log_name = log_name
        return (earliest_log_index, earliest_log_name)
            
    def _target_master_binlog(self, earliest_slave_binlog):
        slave_binlog_index = earliest_slave_binlog[0]
        master_binlogs = self.master_handler.binlogs_sorted()
        binlog_length = len(master_binlogs)
        for i in range(binlog_length):
            master_binlog_index = master_binlogs[i][0]
            if slave_binlog_index == master_binlog_index:
                binlog_window = self.dbreplica.binlog_window
                if i > binlog_window:
                    return (False, 
                            master_binlogs[i - binlog_window],
                            master_binlogs[0],
                            master_binlogs[binlog_length - 1])
                else:
                    return (True, 
                            None,
                            master_binlogs[0],
                            master_binlogs[binlog_length - 1])
        raise ReplicaWorkerException("slave binary log not" +
                                     " in master binary logs")
    
    def _do_purge(self):
        if len(self.slaves) <= 0:
            self.logger.info("skip purge, no slave")
        else:
            slave_binlog = self._earliest_slave_binlog()
            (skip, 
             target_master_binlog,
             earliest_master_binlog,
             latest_master_binlog) = self._target_master_binlog(slave_binlog)
            if not skip:
                self.logger.info(("start purging, " +
                                  "earliest_slave_binlog %s, " +
                                  "earliest_master_binlog %s, " +
                                  "lateset_master_binlog %s, " +
                                  "target_master_binlog %s") %
                                 (slave_binlog[1], 
                                  earliest_master_binlog[1],
                                  latest_master_binlog[1],
                                  target_master_binlog[1]))
                self.master_handler.purge(target_master_binlog[1])
                self.logger.info("binary log successfully purged")
            else:
                self.logger.info(("skip purge, "+
                                  "earliest_slave_binlog %s, " +
                                  "earliest_master_binlog %s, " +
                                  "latest_master_binlog %s, " +
                                  "binlog_window %s") %
                                 (slave_binlog[1],
                                  earliest_master_binlog[1],                                  
                                  latest_master_binlog[1],
                                  self.dbreplica.binlog_window))
    def purge(self):
        if not self.lock.acquire(False):
            raise ReplicaWorkerException("another purge is running")
        else:
            try:
                self._do_purge()
                self.lock.release()
            except Exception as e:
                self.lock.release()
                raise e
    
    def master_binlogs(self):
        return self.master_handler.binlogs()
    
    def master_status(self):
        return self.master_handler.status()
    
    def slave_status(self, slave_id):
        if self.slaves.has_key(slave_id):
            return self.slaves_handler[slave_id].status()
        else:
            raise ReplicaWorkerException("no such slave")
        
    def stop(self):
        self.lock.acquire()
        self.stopped = True
        self.lock.release()   
        
    def run(self):
        self.logger.info("worker %s started" % self.dbreplica.id)
        while True:
            time.sleep(self.dbreplica.check_period)
            if not self.stopped:
                self.lock.acquire()
                try:
                    self._do_purge()
                    self.lock.release()
                except Exception as e:
                    self.lock.release()
                    self.logger.error("run purge error: %s" % str(e))
            else:
                self.logger.info("worker %s stopped" % self.dbreplica.id)                
                break


class ReplicaMasterHandler():
    
    def __init__(self, master):
        self.master = master
        
    def _get_connect(self):
        return MySQLdb.connect(host = self.master.host,
                               port = self.master.port,
                               user = self.master.user,
                               passwd = self.master.passwd)        
    
    def binlogs_sorted(self):
        connect = self._get_connect()
        cursor = connect.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute("show binary logs")
        rows = cursor.fetchall()
        unsorted_binlogs = {}
        for row in rows:
            log_name = row["Log_name"]
            log_index = int(log_name[log_name.rfind(".")+1:len(log_name)])
            unsorted_binlogs[log_index] = log_name
        cursor.close()
        connect.close()
        return sorted(unsorted_binlogs.iteritems())
    
    def binlogs(self):
        connect = self._get_connect()
        cursor = connect.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute("show binary logs")
        rows = cursor.fetchall()
        cursor.close()
        connect.close()
        return rows    
    
    def status(self):
        connect = self._get_connect()
        cursor = connect.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute("show master status")
        row = cursor.fetchone()
        cursor.close()
        connect.close()
        return row
        
    
    def purge(self, binlog):
        connect = self._get_connect()
        cursor = connect.cursor()
        cursor.execute("purge binary logs to '%s'" % binlog)
        cursor.close()
        connect.close()
        
class ReplicaSlaveHandler():
    
    def __init__(self, slave):
        self.slave = slave
    
    def _get_connect(self):
        return MySQLdb.connect(host = self.slave.host,
                               port = self.slave.port,
                               user = self.slave.user,
                               passwd = self.slave.passwd)
    
    def master_binlog(self):
        connect = self._get_connect()
        cursor = connect.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute("show slave status")
        row = cursor.fetchone()
        log_name = row['Master_Log_File']
        log_index = int(log_name[log_name.rfind(".")+1:len(log_name)])
        cursor.close()
        connect.close()
        return (log_index, log_name)
    
    def status(self):
        connect = self._get_connect()
        cursor = connect.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute("show slave status")
        row = cursor.fetchone()
        cursor.close()
        connect.close()
        return row
    

class ReplicaWorkerException(Exception):
    
    def __init__(self, value):
        self.value = value
     
    def __str__(self):
        return repr(self.value)