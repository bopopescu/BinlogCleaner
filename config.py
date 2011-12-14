'''
Created on 2011-12-12

@author: frank
'''

import logging
import ConfigParser

from singleton import Singleton

class CleanerConfig(Singleton):
    
    DEFAULT_CHECK_PERIOD = 300
    DEFAULT_PERSISTENCE_PATH = "db/persistence.db"
    
    def __init__(self):
        self.parser = ConfigParser.ConfigParser()
        self.logger = logging.getLogger("cleaner")
        self.parser.read('conf/cleaner.ini')

    def _check_empty_string(self, argument):
        if argument is None:
            return True
        argument = argument.replace(' ', '')
        if len(argument) == 0:
            return True
        return False
    
    @property
    def check_period(self):
        try:
            attr = self.parser.get("global", "check_period")
            if not self._check_empty_string(attr):
                return int(attr)
            else:
                return CleanerConfig.DEFAULT_CHECK_PERIOD
        except:
            return CleanerConfig.DEFAULT_CHECK_PERIOD
        
    @property
    def persistence_path(self):
        try:
            attr = self.parser.get("global", "persistence_path")
            if not self._check_empty_string(attr):
                return attr
            else:
                return CleanerConfig.DEFAULT_PERSISTENCE_PATH
        except:
            return CleanerConfig.DEFAULT_PERSISTENCE_PATH        
        