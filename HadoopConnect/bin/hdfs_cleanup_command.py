"""
External cleanup command for deleting export data on HDFS. 

Usage:
    | hdfsinternal clean hdfs:localhost:9000/export
"""

import csv,sys,re,time
import splunk.util, splunk.Intersplunk as isp
import urlparse
from hadooputils import * 
from constants import *
from util import *

try:
    from cStringIO import StringIO
except:
    from StringIO import StringIO

def csv_escape(s):
    return s.replace('"', '""')


class HDFSCleanCommand:
    def __init__(self):
        self.directives = {'clean': self.handle_clean}
        self.raiseAll = True           
        self.messages = {}
        self.keywords = []
        self.settings = {}
        self.info     = SearchResultsInfo() # we'll read this if splunk passes it along
        
    def _addMessage(self, type, msg):
        if type in self.messages:
           self.messages[type].append(msg)
        else:
           self.messages[type] = [msg]

        self.info.addMessage(type, msg);

        if type == 'WARN':
           logger.warn(msg)
        elif type == 'ERROR':
           logger.error(msg)
    
    def createHDFSDirLister(self):
        return HDFSDirLister(self.raiseAll)
    
    def createHadoopCliJob(self, path):
        return HadoopEnvManager.getCliJob(path)
    
    def _getDeleteDirs(self, uri, days_to_keep):
        hls = self.createHDFSDirLister()
        dirs = {}
        # get all top level dirs under uri
        for e in hls.ls(uri, self.krb5_principal):
            #logger.info('%s, %s, %s, %s' % (e.acl, e.date, e.time, e.path))
            if e.acl.strip().startswith('d'):
                #logger.info('add %s to dirs' % e.path)
                dirs[e.date+e.time+e.path] = e.path
        
        logger.info('before sort, dirs:')
        for k, v in dirs.iteritems():
            logger.info('%s: %s' % (k,v))
                
        paths = []
        keys = dirs.keys()
        keys.sort()
        
        logger.info('after sort, paths:')    
        for k in keys:
            logger.info('%s: %s' % (k, dirs[k]))
            paths.append(dirs[k])
        
        delpaths = []
        logger.info('delpaths:')
        for p in paths[0:-days_to_keep]:
            logger.info(p)
            delpaths.append(hdfsPathJoin(getBaseURI(uri), p))
            
        # we get here iff self.raiseAll == False and there was an error
        if hls.error:
            self._addMessage('WARN', hls.error)

        return delpaths
    
    def _deleteDirs(self, dirs):
        waiter = HDFSJobWaiter()
        for d in dirs:
            hj = self.createHadoopCliJob(d)
            hj.rmr(d)
            waiter.addJob(hj)
        waiter.wait()    
            
    def handle_clean(self):
        l = len(self.keywords)
        if l != 2:
            raise Exception('uri and days_to_keep are required')
        uri = self.keywords[0]
        days_to_keep = int(self.keywords[1])
        dirs = self._getDeleteDirs(uri, days_to_keep)
        self._deleteDirs(dirs)
        
    def _main_impl(self):
        if len(self.keywords) == 0:
           raise Exception('Missing required hdfs command directive. Usage: hdfs_internal <directive> (<hdfs-paths>)+ (<directive-options>)?')

        d = self.keywords.pop(0)
        if not d in self.directives:
           raise Exception('Unknown directive: %s' % d)

        if len(self.keywords) == 0:
           raise Exception('At least one HDFS path is required')
       
        logger.info('keywords:'+str(self.keywords))
        k = self.keywords[0]
        if k.startswith('file://'):
              k = k[7:] 
        elif not k.startswith('hdfs://'):
            raise Exception('Invalid HDFS path, "%s", it should start with hdfs://' % k)

        self.directives[d]()

    def main(self):
        results, dummyresults, self.settings = isp.getOrganizedResults()
        self.keywords, self.argvals = isp.getKeywordsAndOptions()
        logger.info('keywords:'+str(self.keywords))
        
        # in Splunk pre 5.0 we don't get the info, so we just read it from it's standard location
        infoPath = self.settings.get('infoPath', '')
        if len(infoPath) == 0:
           infoPath = os.path.join(getDispatchDir(self.settings.get('sid'), self.settings.get('sharedStorage', None)), 'info.csv')
        self.info.readFrom(infoPath)
        
        
        self.raiseAll = splunk.util.normalizeBoolean(unquote(self.argvals.get('raiseall', 'f')))
        self.sessionKey = self.settings.get('sessionKey', None)
        self.owner      = self.settings.get('owner',      None)
        self.namespace  = self.settings.get('namespace',  None)
        self.krb5_principal = unquote(self.argvals.get('kerberos_principal', '')).strip()
        
        
        if len(self.krb5_principal) == 0:
           self.krb5_principal = None
        HadoopEnvManager.init(APP_NAME, 'nobody', self.sessionKey, self.krb5_principal)
        
        self._main_impl()

if __name__ == '__main__':
    rv   = 0
    hdfs = HDFSCleanCommand()     
    #TODO: improve error messaging, here - right now the messages just go to search.log 
    try:
          hdfs.main()
    except Exception, e:
          import traceback
          stack =  traceback.format_exc()
    
          if hdfs.info != None:
             hdfs.info.addErrorMessage(str(e))
             logger.error("sid=%s, %s\nTraceback: %s" % (hdfs.settings.get('sid', 'N/A'), str(e), str(stack)))
          else:
             print "ERROR %s" % str(e).replace('\n', '\\n')
          print >> sys.stderr, "ERROR %s\nTraceback: %s" % (str(e), str(stack))
          rv = 1
    finally:
        try:
            if hdfs.info != None:
               hdfs.streamResults('') # just write out the info
        except Exception, e:
            logger.exception("Failed to update search result info: ", e)
    sys.exit(rv)

