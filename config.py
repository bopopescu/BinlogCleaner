'''
Created on 2011-12-12

@author: frank
'''

import json
import logging
import ConfigParser


class CleanerConfig():

    DEFAULT_PERSISTENCE_PATH = "db/persistence.db"
    DEFAULT_MONITOR_CHECK_PERIOD = 60
    DEFAULT_MAIL_PORT = 25
    
    def __init__(self):
        self.parser = ConfigParser.ConfigParser()
        self.logger = logging.getLogger("cleaner")
        self.parser.read('conf/cleaner.ini')
        
        self._check_config()

    def _check_config(self):
        self._check_attr_empty('mail.host', 
                               self.parser.get('mail', 'mail.host'))
        self._check_attr_empty('mail.sender', 
                               self.parser.get('mail', 'mail.sender'))
        self._check_attr_empty('mail.passwd', 
                               self.parser.get('mail', 'mail.passwd'))
        self._check_attr_empty('mail.reciever', 
                               self.parser.get('mail', 'mail.recievers'))                
        self._check_json()

    def _check_json(self):
        json.loads(self.parser.get('mail', 'mail.recievers'))

    def _check_attr_empty(self, attr_name, attr_value):
        if attr_value is None or self._check_empty_string(attr_value):
            raise Exception("attribute %s is emtpry" % attr_name)

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

    @property
    def monitor_check_period(self):
        try:
            attr = self.parser.get("global", "monitor.check_period")
            if not self._check_empty_string(attr):
                return int(attr)
            else:
                return CleanerConfig.DEFAULT_MONITOR_CHECK_PERIOD
        except:
            return CleanerConfig.DEFAULT_MONITOR_CHECK_PERIOD
    
    @property
    def mail_host(self):
        return self.parser.get('mail', 'mail.host')
    
    @property
    def mail_port(self):       
        try:
            attr = self.parser.get("mail", "mail.port")
            if not self._check_empty_string(attr):
                return int(attr)
            else:
                return CleanerConfig.DEFAULT_MAIL_PORT
        except:
            return CleanerConfig.DEFAULT_MAIL_PORT

    @property
    def mail_sender(self):
        return self.parser.get('mail', 'mail.sender')
    
    @property
    def mail_passwd(self):
        return self.parser.get('mail', 'mail.passwd')
    
    @property
    def mail_recievers(self):
        recievers = json.loads(self.parser.get('mail', 'mail.recievers'))
        if not isinstance(recievers, list):
            return []
        else:
            return recievers

