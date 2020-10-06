import sys
import csv
from util import *
from errors import *
from export_hdfs import *
from hadooputils import *
# TODO: Remove inspect once done debugging
import inspect

class DumpResultHandler(SplunkResultHandler):
    def __init__(self):
        BaseSplunkResultHandler.__init__(self)
        self.dst        = None                  # hdfs://foobar:123/path/to/export
        self.uri        = None                  # hdfs://foobar:123 or file://
        self.uri_path   = None                  # /this/comes/after/uri
        self.tmp_dir    = None                  # either tmp dir or final destination for file://
        self.hdfs_mover = HDFSFileMover() 
        self.hdfs_time = 0.000                  # time spent writing to hdfs file system
        self.hdfs_bytes  = 0                    # number of bytes sent to hdfs
        self.parent_sid  = None                 # the search id of our parent
        self.hdfs_file_count = 0                # number of files moved to hdfs during this invocation
        self.rolledFiles = []                   # a list of rolled file paths read from stdin
        self.rolledFile_idx = None              # the index of rolled_file field in header
        self.rolled_ext = DEFAULT_ROLL_EXTENSION 
       
    def raiseException(self, message_format, options=[]):
        SplunkResultHandler.raiseException(self, message_format, options, 'movehdfs')

    def checkPreconditions(self, argvals):
        if not 'dst' in argvals:
            self.raiseException(HCERR0501, {'argument':'dst'})
        logger.error("[--0--] Splunk Debug: Checking for Preconditions")
        dst = unquote(argvals['dst'])
        try:       
            uri, uri_path = getBaseURIAndPath(dst)
            logger.error("URI: {}".format(uri))
            logger.error("URI Path: {}".format(uri_path))
        except Exception as e:
            logger.exception('Failed to get base uri and path')
            if isinstance(e, HcException):
                self.raiseException(e.message_format, e.options)
            else:
                self.raiseException(str(e))
        
        if 'tmp_dir' in argvals:
            if not os.path.isabs(argvals['tmp_dir']):
                self.raiseException(HCERR0502, {'name':'tmp_dir', 'value':argvals['tmp_dir'], 'error':'needs to be an absolute path.'})
            if not os.path.exists(argvals['tmp_dir']):
                self.raiseException(HCERR0502, {'name':'tmp_dir', 'value':argvals['tmp_dir'], 'error':'does not exist.'})

        return dst, uri, uri_path

    # called once, after reading any settings send by search
    def handleSettings(self, settings, keywords, argvals):
        # applied only in Splunk 4.3.4
        self.disptachtmpWorkAround(settings)

        self.handleInfo(settings.get('infoPath', ''))
                                        
        argvals['dst'], self.uri, self.uri_path = self.checkPreconditions(argvals)
        self.dst = argvals['dst']
        logger.error("[---] Splunk Debug Inspect Results: {}".format(inspect.stack()[1][3]))
        self.sessionKey = settings.get('sessionKey', None)
        self.owner      = settings.get('owner',      None)
        self.namespace  = settings.get('namespace',  None)

        self.parent_sid    = unquote(argvals.get('parentsid', None))
        self.export_name   = unquote(argvals.get('exportname', ''))
        self.krb5_principal = unquote(argvals.get('kerberos_principal', '')).strip()

        if len(self.krb5_principal) == 0:
            self.krb5_principal = None
            HadoopEnvManager.init(APP_NAME, 'nobody', self.sessionKey, self.krb5_principal)

        dispatch_dir = getDispatchDir(self.info.get('_sid'), settings.get('sharedStorage', None))
        self.tmp_dir = argvals['tmp_dir'] if 'tmp_dir' in argvals else DEFAULT_DUMP_TMP_DIR_NAME    
        self.tmp_dir = os.path.join(dispatch_dir, self.tmp_dir)
 
        if self.info.countMap:
            self.script_call_counter = int(self.info.countMap.get('invocations.command.movehdfs', '0'))        
                   
            if self.script_call_counter % 10 == 0:
                # see if our parent is still alive
                self.checkParentSearchStatus()
    
    def handleHeader(self, header):
        self.header = header
        if 'rolled_file' in header:
            self.rolledFile_idx = header.index('rolled_file')

    def flushToHdfs(self, toflush):
        logger.debug("toflush:"+str(toflush))
        SplunkResultHandler.flushToHdfs(self, toflush, 0)        
      
    def getToFlushFiles(self):
        toflush = {}
        for local_path in self.rolledFiles:
            toflush[local_path] = self.getHdfsPath(local_path)
        return toflush
    
    def getHdfsPath(self, local_path):
        return self.dst + makeFileSystemSafe(local_path[len(self.tmp_dir):], [[':','_'], ['//','/']]) + self.rolled_ext

    # wait for all the hadoop cli jobs to complete 
    def waitForHdfsJobs(self, writeInfo=True):
        SplunkResultHandler.waitForHdfsJobs(self, writeInfo, 'movehdfs')

    def handleResult(self, result):
        if self.rolledFile_idx != None:
            self.rolledFiles.append(result[self.rolledFile_idx])

    def handleFinish(self):
        self.flushToHdfs(self.getToFlushFiles())
        self.hdfs_time       = self.hdfs_mover.getWallTime()
        self.hdfs_file_count = self.hdfs_mover.getJobCount()
        if self.info.countMap:
            if self.hdfs_file_count > 0:
                self.info.updateMetric('command.movehdfs.hdfs', int(self.hdfs_time*1000), self.hdfs_file_count)
        self.info.writeOut()


class DumpResultStreamer(SplunkResultStreamer):
    def __init__(self, handler, din=sys.stdin, dout=sys.stdout):
        SplunkResultStreamer.__init__(self, handler, din, dout)
    
    def run(self):
        #1. read settings
        self.populateSettings(file=self.din)

        keywords, argvals = getKeywordsAndOptions()
        self.handler.handleSettings(self.settings, keywords, argvals)

        cr = csv.reader(self.din)
        cw = csv.writer(self.dout)
        try:
            #2. read header
            self.header = cr.next()
            out_header = self.handler.handleHeader(self.header)
            #3. csv: read input results
            for row in cr:
                self.handler.handleResult(row)
        except StopIteration, sp:
            pass
        finally:       
            #4. get any rows withheld by the handle until it sees finish
            self.handler.handleFinish()


def run_streamer():
    drh = DumpResultHandler()
    drs = DumpResultStreamer(drh)

    try:
        drs.run()
        drh.waitForHdfsJobs()
    except Exception as e:
        logger.exception('Failed to call run_streamer')
        if isinstance(e, FinishSearchException):
            raise e
        elif isinstance(e, HcException):
            drh.raiseException(e.message_format, e.options)
        else:
            drh.raiseException(str(e))  

    if drh.hdfs_bytes > 0:
        logger_metrics.info("group=transfer, exportname=\"%s\", sid=%s, hdfs_KB=%d, hdfs_time=%.3f, hdfs_files=%d" % 
                               (drh.export_name, drh.info.get("_sid"), long(drh.hdfs_bytes/1024), drh.hdfs_time, drh.hdfs_file_count))

if __name__ == '__main__':  
    try:
        run_streamer()
    except FinishSearchException, e:
        sys.stderr.write(str(e))

