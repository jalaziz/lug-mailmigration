#!/usr/bin/python
'''
LUG Mail Migration

Mail migration script to migrate email messages from mbox files to Google Apps

Created on Sep 14, 2010

Linux Users Group at UCLA
@author: jalaziz
'''

import getopt
import getpass
import sys
import Queue
import threading
import os
import mailbox
import time

import gdata.apps.service
import gdata.apps.migration.service
from gdata.apps.migration.service import MigrationService

class MboxMigrate(threading.Thread):
    def __init__(self, mboxes=['mbox','sent']):
        threading.Thread.__init__(self)
        self.mboxes = mboxes
        
    def run(self):
        while True:
            data = work_pool.get()
            
            if data == 'stop':
                exit(0)
            
            user = data['username']
            home_prefix = data['home_prefix']
            gd_client = data['gd_client']
            
            for mbox in self.mboxes:
                path = home_prefix + user + '/' + mbox
                try:
                    mb = mailbox.mbox(path, create=False)
                    # mb.lock()
                    for msg in mb:
                        mail_properties = []
                        if mbox == 'sent':
                            mail_properties.append('IS_SENT')
                        else:
                            mail_properties.append('IS_INBOX')
                        
                        if 'R' not in msg.get_flags():
                            mail_properties.append('IS_UNDREAD')
                        if 'D' in msg.get_flags():
                            mail_properties.append('IS_TRASH')
                        if 'F' in msg.get_flags():
                            mail_properties.append('IS_STARRED')
                        
                        print 'Importing mail for ' + user
                        print 'Mail properties: ' + mail_properties.__str__()
                        print 'Mail message:'
                        print msg.as_string(False)
                        
                        try:
                            gd_client.ImportMail(user, msg.as_string(False), mail_properties, [ 'Migrated' ])
                        except gdata.apps.service.AppsForYourDomainException, msg:
                            print 'Error: ' + msg
                            
                        time.sleep(1)
                    # mb.unlock()
                    mb.close()
                except mailbox.NoSuchMailboxError:
                    print 'Cannot find mailbox: ' + path
                        
            work_pool.task_done()

class LUGMailMigration(object):
    def __init__(self, email, password, domain, home_dir='/home', users=None):
        self.gd_client = MigrationService(email, password, domain, 'LUGMailMigration')
        self.gd_client.ProgrammaticLogin()
        self.home_dir = home_dir
        self.users = []
        
        if not users:
            self.users = [dir for dir in os.listdir(self.home_dir)
                            if os.path.isdir(os.path.join(home_dir, dir))]
        else:
            self.users = users;
    
    def run(self):
        for user in self.users:
            work_pool.put({ 
                           'username': user, 
                           'home_prefix': self.home_dir + '/', 
                           'gd_client': self.gd_client,
                           })
        work_pool.join()

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], '', ['user=', 'pw=', 'domain=', 'dest-users=', 'max-threads=', 'home-dir='])
    except getopt.error, msg:
        print 'python migrate.py --user [username] --pw [password]'
        print '--dest-users [destination users] --domain [domain]'
        print '--max-threads [number of threads] --home-dir [home directory]'
        sys.exit(2)
    
    user = ''
    pw = ''
    domain = ''
    dest_users = []
    max_threads = 8
    home_dir = '/home'
    
    for option, arg in opts:
        if option == '--user':
            user = arg
        elif option == '--pw':
            pw = arg
        elif option == '--dest-users':
            dest_users = arg.split(',')
        elif option == '--domain':
            domain = arg
        elif option == '--max-threads':
            max_threads = int(arg)
        elif option == '--home-dir':
            home_dir = arg
            
    while not domain:
        domain = raw_input('Please enter your apps domain: ')
    while not user:
        user = raw_input('Please enter an Administrator account: ')+'@'+domain
    while not pw:
        pw = getpass.getpass('Please enter password: ')
        if not pw:
            print 'Password cannot be blank.'
    
    try:
        migration = LUGMailMigration(user, pw, domain, home_dir, dest_users)
    except gdata.service.BadAuthentication:
        print 'Invalid user credentials.'
        return
    
    for i in xrange(max_threads):
        MboxMigrate().start()
    
    migration.run()
    
    for i in xrange(max_threads):
        work_pool.put('stop')
        
    print 'Done.'

work_pool = Queue.Queue()

if __name__ == '__main__':
    sys.exit(main())