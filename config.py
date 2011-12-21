'''
Created on 2011-12-12

@author: frank
'''

import logging
import ConfigParser


class CleanerConfig():

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
    def persistence_path(self):
        try:
            attr = self.parser.get("global", "persistence_path")
            if not self._check_empty_string(attr):
                return attr
            else:
                return CleanerConfig.DEFAULT_PERSISTENCE_PATH
        except:
            return CleanerConfig.DEFAULT_PERSISTENCE_PATH        
        