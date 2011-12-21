'''
Created on 2011-12-9

@author: frank
'''

import sqlalchemy.orm
import sqlalchemy.ext.declarative


ORMBase = sqlalchemy.ext.declarative.declarative_base()

class Persistence():
    
    def __init__(self, config):
        self.config = config
        self.engine = sqlalchemy.create_engine(self._connect_url(),
                                               echo=True)
        self._init_session()
        self._init_tables()
                
    def _connect_url(self):
        return "sqlite:///%s" % self.config.persistence_path
    
    def _init_session(self):
        session_factory = sqlalchemy.orm.sessionmaker(bind=self.engine, 
                                                      expire_on_commit=False)
        self.Session = sqlalchemy.orm.scoped_session(session_factory)
        
    def _init_tables(self):
        ORMBase.metadata.create_all(self.engine)
        
    def session(self):
        return self.Session()