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
        for i in range(len(master_binlogs)):
            master_binlog_index = master_binlogs[i][0]
            if slave_binlog_index == master_binlog_index:
                binlog_window = self.dbreplica.binlog_window
                if i > binlog_window:
                    return (False, master_binlogs[i-binlog_window])
                else:
                    return (True, master_binlogs[0])
        raise ReplicaWorkerException("slave binary log not" +
                                     " in master binary logs")
    
    def _do_purge(self):
        slave_binlog = self._earliest_slave_binlog()
        (skip, master_binlog) = self._target_master_binlog(slave_binlog)
        if not skip:
            self.logger.info(("start purging, " +
                              "earliest slave binary log %s, " +
                              "target master binary log %s") %
                             (slave_binlog[1], 
                              master_binlog[1]))
            self.master_handler.purge(master_binlog[1])
            self.logger.info("binary log successfully purged")
        else:
            self.logger.info(("skip purge, "+
                              "earliest slave binary log %s, "+
                              "earliest master binary log %s, "+
                              "binary log window size %s") %
                             (slave_binlog[1],
                              master_binlog[1],
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
        return self.slaves_handler[slave_id].status()
        
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
                self.master_handler.close()
                for slave_id in self.slaves_handler.keys():
                    self.slaves_handler[slave_id].close()
                self.logger.info("worker %s stopped" % self.dbreplica.id)                
                break


class ReplicaMasterHandler():
    
    def __init__(self, master):
        self.master = master
        self._init_connect()
    
    def _init_connect(self):
        self.connect = MySQLdb.connect(host = self.master.host,
                                       port = self.master.port,
                                       user = self.master.user,
                                       passwd = self.master.passwd)
    
    def binlogs_sorted(self):
        cursor = self.connect.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute("show binary logs")
        rows = cursor.fetchall()
        unsorted_binlogs = {}
        for row in rows:
            log_name = row["Log_name"]
            log_index = int(log_name[log_name.rfind(".")+1:len(log_name)])
            unsorted_binlogs[log_index] = log_name
        cursor.close()
        return sorted(unsorted_binlogs.iteritems())
    
    def binlogs(self):
        cursor = self.connect.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute("show binary logs")
        rows = cursor.fetchall()
        cursor.close()
        return rows    
    
    def status(self):
        cursor = self.connect.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute("show master status")
        row = cursor.fetchone()
        cursor.close()
        return row
        
    
    def purge(self, binlog):
        cursor = self.connect.cursor()
        cursor.execute("purge binary logs to '%s'" % binlog)
        cursor.close()
        
    
    def close(self):
        self.connect.close()
        
class ReplicaSlaveHandler():
    
    def __init__(self, slave):
        self.slave = slave
        self._init_connect()
    
    def _init_connect(self):
        self.connect = MySQLdb.connect(host = self.slave.host,
                                       port = self.slave.port,
                                       user = self.slave.user,
                                       passwd = self.slave.passwd)
    
    def master_binlog(self):
        cursor = self.connect.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute("show slave status")
        row = cursor.fetchone()
        log_name = row['Master_Log_File']
        log_index = int(log_name[log_name.rfind(".")+1:len(log_name)])
        cursor.close()
        return (log_index, log_name)
    
    def status(self):
        cursor = self.connect.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute("show slave status")
        row = cursor.fetchone()
        cursor.close()
        return row
    
    def close(self):
        self.connect.close()
    

class ReplicaWorkerException(Exception):
    
    def __init__(self, value):
        self.value = value
     
    def __str__(self):
        return repr(self.value)