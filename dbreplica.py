'''
Created on 2011-12-9

@author: frank
'''

import json

import sqlalchemy

from persistence import ORMBase

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
    
    def __init__(self, id, master, slaves, 
                 check_period = 300,
                 binlog_window = 0):
        self.id = id
        self.master = master
        self.slaves = slaves
        self.check_period = check_period
        self.binlog_window = binlog_window

class DBReplicaController():
    
    def __init__(self, persistence):
        self.persistence = persistence
        
    def get(self, id):
        session = self.persistence.session()
        query = session.query(DBReplica)
        dbreplica = query.get(id)
        session.expunge_all()
        session.close()
        return dbreplica
    
    def get_all(self):
        session = self.persistence.session()
        dbreplicas = []
        for row in session.query(DBReplica):
            dbreplicas.append(row)
        session.expunge_all()
        session.close()
        return dbreplicas
    
    def add(self, dbreplica):
        session = self.persistence.session()
        session.add(dbreplica)
        session.commit()
        session.close()
        
    def delete(self, replica_id):
        session = self.persistence.session()
        query = session.query(DBReplica)
        dbreplica = query.get(replica_id)
        if dbreplica is not None:
            session.delete(dbreplica)
            session.commit()
        session.close()  
        
    def add_slave(self, replica_id, slave_id):
        session = self.persistence.session()
        query = session.query(DBReplica)
        dbreplica = query.get(replica_id)
        if dbreplica is not None:
            if dbreplica.slaves is None or len(dbreplica.slaves) == 0:
                slaves = set([])
            else:
                slaves = set(json.loads(dbreplica.slaves))
            slaves.add(slave_id)
            dbreplica.slaves = json.dumps(list(slaves))
            session.commit()
        session.close()
        
    def add_slaves(self, replica_id, slave_ids):
        session = self.persistence.session()
        query = session.query(DBReplica)
        dbreplica = query.get(replica_id)
        if dbreplica is not None:
            if dbreplica.slaves is None or len(dbreplica.slaves) == 0:
                slaves = set([])
            else:
                slaves = set(json.loads(dbreplica.slaves))
            for slave_id in slave_ids:
                slaves.add(slave_id)
            dbreplica.slaves = json.dumps(list(slaves))
            session.commit()
        session.close()
            
    def del_slave(self, replica_id, slave_id):
        session = self.persistence.session()
        query = session.query(DBReplica)
        dbreplica = query.get(replica_id)
        if dbreplica is not None:
            slaves = set(json.loads(dbreplica.slaves))
            slaves.discard(slave_id)
            dbreplica.slaves = json.dumps(list(slaves))
            session.commit()
        session.close()        
        
    def update_check_period(self, replica_id, check_period):
        session = self.persistence.session()
        query = session.query(DBReplica)
        dbreplica = query.get(replica_id)
        if dbreplica is not None:
            dbreplica.check_period = check_period
            session.commit()
        session.close()
        
    def update_binlog_window(self, replica_id, binlog_window):
        session = self.persistence.session()
        query = session.query(DBReplica)
        dbreplica = query.get(replica_id)
        if dbreplica is not None:
            dbreplica.binlog_window = binlog_window
            session.commit()
        session.close()        