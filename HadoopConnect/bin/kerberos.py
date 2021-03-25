import os,os.path
import random as r

from util import *
from constants import *

KEYTAB_FILE = "principal.keytab"
TGT_CACHE   = "krb5_tgt.cache"


class Krb5Principal:

      def __init__(self, name):
          self.name = name.strip()

      def create(self):
          try:
              dir = self.getPrincipalDir()
              if not os.path.isdir(dir):
                 os.makedirs(dir)
          except Exception as e:
              raise HcException(HCERR1521, {'name':self.name, 'error':str(e)})
          
      def remove(self):
          try:
              dir = self.getPrincipalDir()
              if os.path.isdir(dir):
                 import shutil
                 shutil.rmtree(dir)
          except Exception as e:
              raise HcException(HCERR1520, {'name':self.name, 'error':str(e)})
          
      def validate(self):
          if not os.path.exists(self.getKeytabFile()):
              raise HcException(HCERR0006, {'keytab':self.getKeytabFile(), 'principal':self.name})      

      def getPrincipalDir(self):
          return os.path.join(os.environ['SPLUNK_HOME'], 'etc', 'apps', APP_NAME, 'local', 'principals', makeFileSystemSafe(self.name))
 
      def getKeytabFile(self):
          return os.path.join(self.getPrincipalDir(), KEYTAB_FILE)

      def getTgtCacheFile(self):
          return os.path.join(self.getPrincipalDir(), TGT_CACHE)
      
      def shouldRenewTgt(self, tgt_cache, tgtRenewTtl=None):
          # add 5 min randomness to reduce race condition (multiple exports using same principal are trying to update same cache) 
          if tgtRenewTtl == None:
              tgtRenewTtl = DEFAULT_TGT_RENEW_TTL + int(r.random() * 300)
              
          # check modtime of tgt cache, if it's less than 30 minutes assume it is still valid
          statinfo = os.stat(tgt_cache) if os.path.exists(tgt_cache) else None
          return statinfo==None or statinfo.st_mtime < time.time() - tgtRenewTtl
      
      def randomSleep(self, sec=5):
          time.sleep( int(1 + r.random() * sec) )
                  
      def setupEnv(self, env=None, tgtRenewTtl=None):
          tgt_cache = self.getTgtCacheFile()
          keytab    = self.getKeytabFile()
          env = dict(os.environ) if not env else env
          
          # this env variable is how the Hadoop CLI knows where to find the kerberos TGT 
          # to access a kerberized Hadoop cluster 
          env["KRB5CCNAME"] = tgt_cache 
          
          kinit_args = ['kinit', '-k', '-t', keytab, self.name]
          logger.debug('running: %s' % ' '.join(kinit_args))

          # run kinit to get a TGT
          # try up to 3 times due to possible race condition
          count = 0
          while (count < 3):
              count += 1
              # check to see if we need to update (another process might have updated the tgt)
              if not self.shouldRenewTgt(tgt_cache, tgtRenewTtl):
                 return
              process = self.createProcess(kinit_args, env)
              output = process.communicate()
              logger.debug('completed running, rv=%d command="%s"' % (process.returncode, ' '.join(kinit_args)))
              if process.returncode == 0:
                  return
              # maybe another export process updated the tgt cache (avoid sleep)
              if not self.shouldRenewTgt(tgt_cache, tgtRenewTtl):
                  return
              # randomly pick up a time between [1,5] seconds to wait for next try 
              self.randomSleep(5)
              
          raise HcException(HCERR0011, {'cmd':'kinit', 'options':','.join(kinit_args), 'error':output[1]})
      
      def createProcess(self, kinit_args, env):
          import subprocess
          try:
              return subprocess.Popen(kinit_args, shell=False, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
          except Exception as e:
              raise HcException(HCERR0011, {'cmd':'kinit', 'options':','.join(kinit_args), 'error':str(e)})
    
