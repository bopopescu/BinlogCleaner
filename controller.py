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
        
    def add_subordinate(self, replica_id, subordinate_id):
        session = self.persistence.session()
        try:
            query = session.query(DBReplica)
            dbreplica = query.get(replica_id)
            if dbreplica is not None:
                if dbreplica.subordinates is None or len(dbreplica.subordinates) == 0:
                    subordinates = set([])
                else:
                    subordinates = set(json.loads(dbreplica.subordinates))
                subordinates.add(subordinate_id)
                dbreplica.subordinates = json.dumps(list(subordinates))
                session.commit()
        finally:
            session.close()
        
    def add_subordinates(self, replica_id, subordinate_ids):
        session = self.persistence.session()
        try:
            query = session.query(DBReplica)
            dbreplica = query.get(replica_id)
            if dbreplica is not None:
                if dbreplica.subordinates is None or len(dbreplica.subordinates) == 0:
                    subordinates = set([])
                else:
                    subordinates = set(json.loads(dbreplica.subordinates))
                for subordinate_id in subordinate_ids:
                    subordinates.add(subordinate_id)
                dbreplica.subordinates = json.dumps(list(subordinates))
                session.commit()
        finally:
                session.close()
            
    def del_subordinate(self, replica_id, subordinate_id):
        session = self.persistence.session()
        try:
            query = session.query(DBReplica)
            dbreplica = query.get(replica_id)
            if dbreplica is not None:
                subordinates = set(json.loads(dbreplica.subordinates))
                subordinates.discard(subordinate_id)
                dbreplica.subordinates = json.dumps(list(subordinates))
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
        
    def update_no_subordinate_purge(self, replica_id, no_subordinate_purge):
        session = self.persistence.session()
        try:
            query = session.query(DBReplica)
            dbreplica = query.get(replica_id)
            if dbreplica is not None:
                dbreplica.no_subordinate_purge = no_subordinate_purge
                session.commit()
        finally:
            session.close()       