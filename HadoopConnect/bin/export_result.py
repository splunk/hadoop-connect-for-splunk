import gzip,csv
import splunk
import splunk.auth as auth 
import splunk.rest as rest
import splunk.entity as en
import splunk.search as search
import export_hdfs
from util import *
from constants import *
from hadooputils import *

def toList(str, delimiter=','):
    if str == None or len(str)==0:
        return []
    a = str.split(delimiter)
    l = []
    for i in a:
        l.append(i.strip())
    return l
    
def streamJobExport(job, kwargs):
    """
    Stream exported job results to the client (does not buffer the whole result in memory)
    """
    ns = job.eaiacl['app']
    owner = job.eaiacl['owner']
    uri = en.buildEndpoint('search/jobs/export', namespace=ns, owner=owner)
    request = {}
    request['output_mode'] = kwargs['output_mode']
    
    try:
        count = int(kwargs.get('count'))
        if count>0:
            request['count'] = count
    except ValueError:
        logger.warn("Failed to parse count field for export, count=%s" % str(kwargs.get('count')))
        pass

    try:
        if 'offset' in kwargs:
            offset = int(kwargs.get('offset'))
            request['offset'] = offset
    except ValueError:
        logger.warn("Failed to parse offset field for export, offset=%s" % str(kwargs.get('offset')))
        pass

    try:
        if 'field_list' in kwargs:
            request['f'] = kwargs.get('field_list')
    except:
        logger.warn("Failed to parse field_list field for export, field_list=%s" % str(kwargs.get('field_list')))
        
    postargs = getargs = None
    #SPL-51662
    if job.reportSearch is None and ((job.eventIsTruncated and (count == 0 or count > job.eventAvailableCount)) or splunk.util.normalizeBoolean(job.isRemoteTimeline)):
        # re-run the search to get the complete results
        uri = en.buildEndpoint('search/jobs/export', namespace=ns, owner=owner)
        request.update(job.request)
        if count > 0:
            request['search'] += '|head %s' % count
        postargs = request
    elif job.reportSearch: 
        uri = en.buildEndpoint('search/jobs/%s/results/export' % job.sid, namespace=ns, owner=owner)
        getargs = request
    else:
        uri = en.buildEndpoint('search/jobs/%s/events/export' % job.sid, namespace=ns, owner=owner)
        getargs = request
    
    stream = rest.streamingRequest(uri, getargs=getargs, postargs=postargs)
    return stream.readall() # returns a generator
    
class ReportError(Exception):
    def __init__(self, code, response):
        self.code = code
        self.response = response

    def __repr__(self):
        return "(%s) %s" % (self.code, self.response)

    def __str__(self):
        return repr(self)

class ExportResult(rest.BaseRestHandler):
    '''ExportResult is a RestHandler to manage jobs that will export data from a given sid to Hadoop file system'''
    
    def handle_GET(self):
        try:
            self.handleParameters()
            self.export()
        except ReportError:
            raise    
        except Exception as e:
            logger.exception("Failed to export search result:")
            raise ReportError(400, "Failed to export search result, please make sure hadoop server is up")
        finally:
            if self.local_path!=None and os.path.exists(self.local_path):
                os.remove(self.local_path)
            
    def export(self):
        job= search.getJob(self.sid, sessionKey=self.sessionKey)
        kwargs = {'output_mode':self.output_mode, 
                  'count':job.eventAvailableCount if self.count==0 else self.count,
                  'offset':self.offset,
                  'field_list':self.field_list} 
        data = streamJobExport(job, kwargs)
        
        dispatch_dir = getDispatchDir(job.sid)
        tmp_dir = os.path.join(dispatch_dir, 'export_tmp')
        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir)
        filename = '%s.%s' % (self.sid, self.output_mode)
        self.local_path = os.path.join(tmp_dir, filename)
        with open(self.local_path, 'w') as f:
            for block in data:
                f.write(str(block))
        
        HadoopEnvManager.init(APP_NAME, 'nobody', self.sessionKey, self.kerberos_principal)
        cli_job = HadoopCliJob(HadoopEnvManager.getEnv(self.dst))
          
        # attempt to make destination dir
        cli_job.mkdir(self.dst)
        cli_job.wait() 
        logger.error("[---] ExportResult export: Splunk Debug")        
        cli_job.moveFromLocal(self.local_path, self.dst+'/'+filename)
        if cli_job.wait()!=0:
            raise Exception("Failed to move file to hdfs: " + cli_job.getOutput()[1])

    def getUnsignedInt(self, val, name):
        try:
            v = int(val)
        except:
            raise ReportError(400, "%s must be an integer" % name)
        if v < 0:
            raise ReportError(400, "%s cannot be a negative number" % name)
        return v
 
         
    def handleParameters(self):
        self.local_path = None
        query = self.request['query']
        #check required parameters
        if 'sid' not in query:
            raise ReportError(400, "No sid specified")
        if 'dst' not in query:
            raise ReportError(400, "No dst specified")
        
        self.kerberos_principal = query.get('kerberos_principal', None)
            
        # get parameters
        self.sid = query.get('sid')
        self.dst = query.get('dst')
        self.output_mode = query.get('output_format', 'raw')
        self.offset = query.get('offset', 0)
        self.count = query.get('count', 0)
        self.field_list = query.get('field_list', '*')

        # this is mainly for testing purposes 
        if self.sessionKey==None or len(self.sessionKey)==0:
            username = query.get('username', 'admin')
            password = query.get('password', 'changeme')
            self.sessionKey = auth.getSessionKey(username, password)
        
        #validate and cast parameters
        self.count  = self.getUnsignedInt(self.count, 'count')
        self.offset = self.getUnsignedInt(self.offset, 'offset')

        if not self.dst.startswith('hdfs://'):
            raise ReportError(400, "dst must start with hdfs://")

        if self.output_mode not in ['raw', 'csv', 'json', 'xml']:
            raise ReportError(400, "output_mode is not supported, must be one of these: raw, csv, json, xml")

        try:
            if self.field_list!='*':
                self.field_list = toList(self.field_list)
        except:
            raise ReportError(400, "field_list must be either a comma separated field list or a *")
        
