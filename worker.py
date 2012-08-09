'''
Created on 2011-12-14

@author: frank
'''

import smtplib
import json
import threading
import logging
import time
import traceback

from datetime import datetime
from email.mime.text import MIMEText

import MySQLdb

from controller import DBReplicaController
from controller import DBInstanceController

class ReplicaWorker(threading.Thread):
    
    def __init__(self, persistence, dbreplica, config):
        threading.Thread.__init__(self)
        self.config = config
        self.logger = logging.getLogger("cleaner")
        self.lock = threading.Lock()
        self.stopped = False
        self.dbreplica = dbreplica
        self.dbreplica_controller = DBReplicaController(persistence)
        self.dbinstance_controller = DBInstanceController(persistence)
        self._init_replica()
        self._init_handler()
        self.monitor = ReplicaMonitor(self.config, self.dbreplica,
                                      self.master_handler,
                                      self.slaves_handler)
    
    
    def _init_replica(self):
        master_id = self.dbreplica.master
        if self.dbreplica.slaves is None or len(self.dbreplica.slaves) == 0:
            slave_ids = []
        else:
            slave_ids = json.loads(self.dbreplica.slaves)
        master = self.dbinstance_controller.get(master_id)
        if master is None:
            raise Exception("%s no master %s record" % 
                            (self.dbreplica.name, master_id))
        self.master = master
        self.slaves = {}
        for slave_id in slave_ids:
            slave = self.dbinstance_controller.get(slave_id)
            if slave is None:
                raise Exception("%s no slave %s record" % 
                                (self.dbreplica.name, slave_id))
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
        raise Exception(("%s slave binary log not" +
                        " in master binary logs") %
                        (self.dbreplica.name))
    
    def _do_no_slave_purge(self):
        master_binlogs = self.master_handler.binlogs_sorted()
        binlog_window = self.dbreplica.binlog_window
        binlog_length = len(master_binlogs)
        if binlog_window + 1 < binlog_length:
            target_binlog = master_binlogs[binlog_length-binlog_window-1]
            self.logger.info(("%s start no slave purging, " +
                              "earliest_master_binlog %s, " +
                              "target_master_binlog %s, " +
                              "latest_master_binlog %s") %
                             (self.dbreplica.name,
                              master_binlogs[0][1],
                              target_binlog[1],
                              master_binlogs[binlog_length-1][1]))
            self.master_handler.purge(target_binlog[1])
        else:
            self.logger.info(("%s skip no slave purging, " +
                              "earliest_master_binlog %s, " +
                              "latest_master_binlog %s, " +
                              "binlog window %s") %
                             (self.dbreplica.name,
                              master_binlogs[0][1],
                              master_binlogs[binlog_length-1][1],
                              binlog_window))
    
    def _do_purge(self, no_slave_purge):
        if len(self.slaves) <= 0 and no_slave_purge == 0:
            self.logger.info("%s skip purge, no slave" % self.dbreplica.name)
        elif len(self.slaves) <= 0 and no_slave_purge != 0:
            self._do_no_slave_purge()    
        else:
            slave_binlog = self._earliest_slave_binlog()
            (skip, 
             target_master_binlog,
             earliest_master_binlog,
             latest_master_binlog) = self._target_master_binlog(slave_binlog)
            if not skip:
                self.logger.info(("%s start purging, " +
                                  "earliest_slave_binlog %s, " +
                                  "earliest_master_binlog %s, " +
                                  "lateset_master_binlog %s, " +
                                  "target_master_binlog %s") %
                                 (self.dbreplica.name,
                                  slave_binlog[1], 
                                  earliest_master_binlog[1],
                                  latest_master_binlog[1],
                                  target_master_binlog[1]))
                self.master_handler.purge(target_master_binlog[1])
                self.logger.info("binary log successfully purged")
            else:
                self.logger.info(("%s skip purge, "+
                                  "earliest_slave_binlog %s, " +
                                  "earliest_master_binlog %s, " +
                                  "latest_master_binlog %s, " +
                                  "binlog_window %s") %
                                 (self.dbreplica.name,
                                  slave_binlog[1],
                                  earliest_master_binlog[1],                                  
                                  latest_master_binlog[1],
                                  self.dbreplica.binlog_window))
    
    def purge(self):
        if not self.lock.acquire(False):
            raise Exception("%s another purge is running" % 
                            (self.dbreplica.name))
        else:
            try:
                self._do_purge(1)
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
            raise Exception("%s no such slave" % self.dbreplica.name)
        
    def stop(self):
        self.lock.acquire()
        self.stopped = True
        self.monitor.stop()
        self.lock.release()   
        
    def run(self):
        self.logger.info("worker %s started" % self.dbreplica.name)
        self.monitor.start()
        while True:
            time.sleep(self.dbreplica.check_period)
            if not self.stopped:
                self.lock.acquire()
                try:
                    self._do_purge(self.dbreplica.no_slave_purge)
                    self.lock.release()
                except Exception as e:
                    self.lock.release()
                    self.monitor.send_mail("purge error " + str(e), traceback.format_exc())
                    self.logger.error("%s run purge error: %s" % 
                                      (self.dbreplica.name, str(e)))
            else:
                self.logger.info("worker %s stopped" % self.dbreplica.name)         
                break
        if not self.monitor.isstopped():
            self.monitor.stop()

class ReplicaMonitor(threading.Thread):
    
    def __init__(self, config, dbreplica,
                 master_handler, slaves_handler):
        threading.Thread.__init__(self)
        self.config = config
        self.logger = logging.getLogger("cleaner")
        self.dbreplica = dbreplica
        self.master_handler = master_handler
        self.slaves_handler = slaves_handler
        self.stopped = False
        self._init_monitor_status()

    def _init_monitor_status(self):
        self.purge_status = {"last_error": None, "error_repeats": 0, "send_mail_flag": 1}
        self.master_status = {"last_error": None, "error_repeats": 0, "send_mail_flag": 1}
        self.slaves_status = {}
        for slave in self.slaves_handler:
            self.slaves_status[slave] = {"last_error": None, "error_repeats": 0, "send_mail_flag": 1}

    def run(self):
        self.logger.info("monitor %s is started" % self.dbreplica.name)
        while True:
            time.sleep(self.config.monitor_check_period)
            if not self.stopped:
                self._check()
            else:
                break
        self.logger.info("monitor %s is stopped" % self.dbreplica.name)
    
    def stop(self):
        self.stopped = True
        
    def isstopped(self):
        return self.stopped
   
    def send_mail(self, short_msg, tb):
        if self._should_send_mail(self.purge_status, short_msg):
            title, msg = self._error_mail(self.dbreplica.name, "", short_msg, tb, self.purge_status["error_repeats"])
            self._send_mail(title, msg)

    def _should_send_mail(self, status, error):
        should_send_mail = False
        if status["last_error"] == error:
            status["error_repeats"] = status["error_repeats"] + 1
            should_send_mail = (status["error_repeats"] == status["send_mail_flag"])
            if should_send_mail:
                status["send_mail_flag"] = (status["send_mail_flag"] << 1)
        else:
            status["last_error"] = error
            status["error_repeats"] = 0
            status["send_mail_flag"] = 1
            should_send_mail = True
        return should_send_mail

    def _check(self):
        self._check_master()
        self._check_slaves()
        
    def _check_master(self):
        try:
            status = self.master_handler.status()
            self._check_master_status(status)
        except Exception as e:
            tb = traceback.format_exc()
            if self._should_send_mail(self.master_status, str(e)):
                title,msg = self._error_mail(self.dbreplica.name,
                                             "master "+ self.dbreplica.master,
                                             str(e), tb, self.master_status["error_repeats"])
                self._send_mail(title, msg)
                self.logger.error("check master error\n" + msg)

    def _check_master_status(self, stats):
        pass
    
    def _check_slaves(self):
        for slave in self.slaves_handler:
            try:
                slave_handler = self.slaves_handler[slave]
                status = slave_handler.status()
                flag,what = self._check_slave_status(status)
                if not flag:
                    info = ""
                    keylist = status.keys()
                    keylist.sort()
                    for key in keylist:
                        info = info + "%s: %s\n" % (key, status[key])
                    if self._should_send_mail(self.slaves_status[slave], what):
                        title, msg = self._error_mail(self.dbreplica.name, 
                                                      "slave "+ slave,
                                                      what, info, self.slaves_status[slave]["error_repeats"])
                        self._send_mail(title, msg)
                        self.logger.error("check slave error\n" + msg)
            except Exception as e:
                tb = traceback.format_exc()
                if self._should_send_mail(self.slaves_status[slave], str(e)):
                    title,msg = self._error_mail(self.dbreplica.name, 
                                                 "slave " + slave, 
                                                 str(e), tb, self.slaves_status[slave]["error_repeats"])  
                    self._send_mail(title, msg)
                    self.logger.error("check slave error\n" + msg)
    
    def _check_slave_status(self, status):
        if status['Last_IO_Errno'] != 0:
            return False,"IO error"
        if status['Last_SQL_Errno'] != 0:
            return False, 'SQL error'
        
        return True,'ok'
        
 
    def _error_mail(self, name, db, msg, info, repeat):
        title = "%s %s %s" % (name, db, msg)
        msg = ("title: %s\n" % title +
               "timestamp: %d\n" % int(time.time()) +
               "date: %s\n" % datetime.now().strftime("%H:%M:%S %d/%m/%Y") +
               "repeat: %d\n" % repeat +
               "---------------------information---------------------\n%s" % info)
        return title,msg        
            
    
    def _mail_msg(self, sender, recievers, title, msg):
        text = MIMEText(msg)
        text["Subject"] = title
        text["From"] = sender
        text["To"] = ";".join(recievers)
        return text.as_string()
    
    def _send_mail(self, title, msg):
        try:
            client = smtplib.SMTP()
            client.connect(self.config.mail_host, self.config.mail_port)
            client.login(self.config.mail_sender, self.config.mail_passwd)
            client.sendmail(self.config.mail_sender, self.config.mail_recievers,
                            self._mail_msg(self.config.mail_sender,
                                           self.config.mail_recievers,
                                           title, msg))
            client.close()
        except Exception as e:
            self.logger.error("send mail error: " + traceback.format_exc())        

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
