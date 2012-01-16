'''
Created on 2011-12-21

@author: frank
'''

import json


from model import DBInstance
from model import DBReplica

class DBInstanceController():
    
    def __init__(self, persistence):
        self.persistence = persistence
        
    def get(self, id):
        session = self.persistence.session()
        try:
            query = session.query(DBInstance)
            dbinstance = query.get(id)
            session.expunge_all()
        finally:
            session.close()
        return dbinstance
    
    def add(self, dbinstance):
        session = self.persistence.session()
        try:
            session.add(dbinstance)
            session.commit()
        finally:
            session.close()
    
    def update(self, dbinstance):
        session = self.persistence.session()
        try:
            query = session.query(DBInstance)
            old = query.get(dbinstance.id)
            old.update(dbinstance)
            session.commit()
        finally:
            session.close()
        
    def delete(self, id):
        session = self.persistence.session()
        try:
            query = session.query(DBInstance)
            dbinstance = query.get(id)
            if dbinstance is not None:
                session.delete(dbinstance)
                session.commit()
        finally:
            session.close()

class DBReplicaController():
    
    def __init__(self, persistence):
        self.persistence = persistence
        
    def get(self, id):
        session = self.persistence.session()
        try:
            query = session.query(DBReplica)
            dbreplica = query.get(id)
            session.expunge_all()
        finally:    
            session.close()
        return dbreplica
    
    def get_all(self):
        session = self.persistence.session()
        try:
            dbreplicas = []
            for row in session.query(DBReplica):
                dbreplicas.append(row)
            session.expunge_all()
        finally:
            session.close()
        return dbreplicas
    
    def add(self, dbreplica):
        session = self.persistence.session()
        try:
            session.add(dbreplica)
            session.commit()
        finally:    
            session.close()
        
    def delete(self, replica_id):
        session = self.persistence.session()
        try:
            query = session.query(DBReplica)
            dbreplica = query.get(replica_id)
            if dbreplica is not None:
                session.delete(dbreplica)
                session.commit()
        finally:        
            session.close()  
        
    def add_slave(self, replica_id, slave_id):
        session = self.persistence.session()
        try:
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
        finally:
            session.close()
        
    def add_slaves(self, replica_id, slave_ids):
        session = self.persistence.session()
        try:
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
        finally:
                session.close()
            
    def del_slave(self, replica_id, slave_id):
        session = self.persistence.session()
        try:
            query = session.query(DBReplica)
            dbreplica = query.get(replica_id)
            if dbreplica is not None:
                slaves = set(json.loads(dbreplica.slaves))
                slaves.discard(slave_id)
                dbreplica.slaves = json.dumps(list(slaves))
                session.commit()
        finally:
            session.close()        
        
    def update_check_period(self, replica_id, check_period):
        session = self.persistence.session()
        try:
            query = session.query(DBReplica)
            dbreplica = query.get(replica_id)
            if dbreplica is not None:
                dbreplica.check_period = check_period
                session.commit()
        finally:
            session.close()
        
    def update_binlog_window(self, replica_id, binlog_window):
        session = self.persistence.session()
        try:
            query = session.query(DBReplica)
            dbreplica = query.get(replica_id)
            if dbreplica is not None:
                dbreplica.binlog_window = binlog_window
                session.commit()
        finally:
            session.close()
        
    def update_no_slave_purge(self, replica_id, no_slave_purge):
        session = self.persistence.session()
        try:
            query = session.query(DBReplica)
            dbreplica = query.get(replica_id)
            if dbreplica is not None:
                dbreplica.no_slave_purge = no_slave_purge
                session.commit()
        finally:
            session.close()       