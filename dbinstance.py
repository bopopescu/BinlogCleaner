'''
Created on 2011-12-9

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

    def __init__(self, id, host, port=3306,
                 user='root', passwd=''):
        self.id = id
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
    
    def update(self, dbinstance):
        self.id = dbinstance.id
        self.host = dbinstance.host
        self.port = dbinstance.port
        self.user = dbinstance.user
        self.passwd = dbinstance.passwd
    

class DBInstanceController():
    
    def __init__(self, persistence):
        self.persistence = persistence
        
    def get(self, id):
        session = self.persistence.session()
        query = session.query(DBInstance)
        dbinstance = query.get(id)
        session.expunge_all()
        session.close()
        return dbinstance
    
    def add(self, dbinstance):
        session = self.persistence.session()
        session.add(dbinstance)
        session.commit()
        session.close()
    
    def update(self, dbinstance):
        session = self.persistence.session()
        query = session.query(DBInstance)
        old = query.get(dbinstance.id)
        old.update(dbinstance)
        session.commit()
        session.close()
        
    def delete(self, id):
        session = self.persistence.session()
        query = session.query(DBInstance)
        dbinstance = query.get(id)
        if dbinstance is not None:
            session.delete(dbinstance)
            session.commit()
        session.close()