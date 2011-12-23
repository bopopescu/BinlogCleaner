'''
Created on 2011-12-21

@author: frank
'''

import sqlalchemy

from persistence import ORMBase

class DBInstance(ORMBase):
    
    __tablename__ = "dbinstances"
    id = sqlalchemy.Column(sqlalchemy.String(64), 
                           nullable=False,
                           unique=True, 
                           primary_key=True)
    host = sqlalchemy.Column(sqlalchemy.String(128),
                             nullable=False)
    port = sqlalchemy.Column(sqlalchemy.Integer,
                             nullable=False)
    user = sqlalchemy.Column(sqlalchemy.String(32),
                             nullable=False)
    passwd = sqlalchemy.Column(sqlalchemy.Text,
                               nullable=False)
    data_dir = sqlalchemy.Column(sqlalchemy.Text,
                                 nullable=False)

    def __init__(self, id, host, port=3306,
                 user='root', passwd='',
                 data_dir='/var/lib/mysql'):
        self.id = id
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.data_dir = data_dir
    
    def update(self, dbinstance):
        self.id = dbinstance.id
        self.host = dbinstance.host
        self.port = dbinstance.port
        self.user = dbinstance.user
        self.passwd = dbinstance.passwd
        self.data_dir = dbinstance.data_dir

class DBReplica(ORMBase):
    
    __tablename__ = "dbreplicas"
    id = sqlalchemy.Column(sqlalchemy.String(64), 
                           nullable=False,
                           unique=True, 
                           primary_key=True)
    master = sqlalchemy.Column(sqlalchemy.String(64),
                               nullable = False)
    slaves = sqlalchemy.Column(sqlalchemy.Text,
                               nullable=False)
    check_period = sqlalchemy.Column(sqlalchemy.Integer,
                                     nullable=False)
    binlog_window = sqlalchemy.Column(sqlalchemy.Integer,
                                     nullable=False)
    no_slave_purge = sqlalchemy.Column(sqlalchemy.Integer,
                                       nullable=False)
    
    def __init__(self, id, master, slaves, 
                 check_period = 60,
                 binlog_window = 0,
                 no_slave_purge = 1):
        self.id = id
        self.master = master
        self.slaves = slaves
        self.check_period = check_period
        self.binlog_window = binlog_window
        self.no_slave_purge = no_slave_purge