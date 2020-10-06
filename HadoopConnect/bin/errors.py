import os, logging.handlers, json

def createLogger(name, level, filename, format, rotateSize=25*1024*1024, bkCount=4):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    filename = os.path.join(os.environ['SPLUNK_HOME'], 'var', 'log', 'splunk', filename)
    handler = logging.handlers.RotatingFileHandler(filename, maxBytes=rotateSize, backupCount=bkCount)
    formatter = logging.Formatter(format)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

logger_metrics = createLogger("hdfsexport.metrics", logging.INFO, 'export_metrics.log', '%(asctime)s - %(message)s')
logger = createLogger("hdfsexport.default", logging.INFO, 'HadoopConnect.log', '%(asctime)s %(levelname)s %(filename)s [%(funcName)s] [%(lineno)s] - %(message)s')


# system errors 0001-0500
HCERR0000 = '{"id":"HCERR0000", "message":"Unexpected error in HadoopConnect. Please see HadoopConnect.log for more details.", "error":"%s"}'
HCERR0001 = '{"id":"HCERR0001", "message":"Failed to run hadoop CLI Job", "cmd":"%s", "options":"%s", "error":"%s"}'
HCERR0002 = '{"id":"HCERR0002", "message":"HADOOP_HOME is not set. Please configure a valid HADOOP_HOME for this cluster."}'
HCERR0003 = '{"id":"HCERR0003", "message":"Invalid HADOOP_HOME.", "HADOOP_HOME":"%s"}'
HCERR0004 = '{"id":"HCERR0004", "message":"JAVA_HOME is not set. Please configure a valid JAVA_HOME for this cluster."}'
HCERR0005 = '{"id":"HCERR0005", "message":"Invalid JAVA_HOME", "JAVA_HOME":"%s"}'
HCERR0006 = '{"id":"HCERR0006", "message":"Could not find Kerberos keytab file.", "keytab":"%s", "principal":"%s"}'
HCERR0007 = '{"id":"HCERR0007", "message":"Could not find cluster configuration", "cluster":"%s"}'
HCERR0008 = '{"id":"HCERR0008", "message":"The job is already in progress", "cmd":"%s", "options":"%s"}'
HCERR0009 = '{"id":"HCERR0009", "message":"Failed to run local FS job", "cmd":"%s", "options":"%s", "error":"%s"}'
HCERR0010 = '{"id":"HCERR0010", "message":"Failed to parse the output of list command", "path":"%s", "error":"%s"}'
HCERR0011 = '{"id":"HCERR0011", "message":"Failed to renew kerberos TGT", "cmd":"%s", "options":"%s", "error":"%s"}'
HCERR0012 = '{"id":"HCERR0012", "message":"Invalid hadoop cli command path", "path":"%s"}'
HCERR0013 = '{"id":"HCERR0013", "message":"Failed to get local hadoop version", "error":"%s"}'
HCERR0014 = '{"id":"HCERR0014", "message":"Failed to get remote hadoop version", "host":"%s", "port":"%d", "error":"%s"}'
HCERR0015 = '{"id":"HCERR0015", "message":"Unable to connect to hadoop cluster", "uri":"%s", "principal":"%s", "error":"%s"}'
HCERR0016 = '{"id":"HCERR0016", "message":"Path does not exist", "path":"%s"}'
HCERR0017 = '{"id":"HCERR0017", "message":"Failed to get remote hadoop cluster information", "host":"%s", "port":"%d", "error":"%s"}'
HCERR0018 = '{"id":"HCERR0018", "message":"Invalid Kerberos principals to short names mapping rules"}'

# argument/field validation errors 0501-1000
HCERR0501 = '{"id":"HCERR0501", "message":"Missing required argument", "argument":"%s"}'
HCERR0502 = '{"id":"HCERR0502", "message":"Invalid argument", "name":"%s", "value":"%s", "error":"%s"}'
HCERR0503 = '{"id":"HCERR0503", "message":"Invalid argument", "name":"%s", "value":"%s", "accepted_values":"%s"}'
HCERR0504 = '{"id":"HCERR0504", "message":"empty string or all whitespace characters found", "name":"%s"}'
HCERR0505 = '{"id":"HCERR0505", "message":"directive is required"}'
HCERR0506 = '{"id":"HCERR0506", "message":"Invalid directive", "directive":"%s", "accepted_values":"%s"}'

# search errors 1001-1500
HCERR1001 = '{"id":"HCERR1001", "message":"Parent search job is not running. Stopping...", "sid":"%s"}'
HCERR1002 = '{"id":"HCERR1002", "message":"Failed to get parent search", "sid":"%s", "error":"%s"}'
HCERR1003 = '{"id":"HCERR1003", "message":"Failed to get search status information"}'
HCERR1004 = '{"id":"HCERR1004", "message":"Failed to get disk usage information for search job"}' 
HCERR1005 = '{"id":"HCERR1005", "message":"No cluster configuration found", "uri":"%s"}'
HCERR1006 = '{"id":"HCERR1006", "message":"\'read\' directive is not allowed to read a directory", "dir":"%s"}'
HCERR1007 = '{"id":"HCERR1007", "message":"Error in \'hdfs\' command", "error":"%s"}'
HCERR1008 = '{"id":"HCERR1008", "message":"\'read\' directive is not allowed to read a wildcarded path", "path":"%s"}'

# export errors 1501-2000
HCERR1501 = '{"id":"HCERR1501", "message":"Unable to update export status", "export":"%s", "error":"%s"}'
HCERR1502 = '{"id":"HCERR1502", "message":"Failed to update export cursor", "cursor":"%s", "error":"%s"}'
HCERR1503 = '{"id":"HCERR1503", "message":"runexport is only meant to be ran by scheduled searches. Add forcerun=1 to the command to disable this check, but please make sure the search is not interfering with any scheduled searches"}'
HCERR1504 = '{"id":"HCERR1504", "message":"Found unexpected partial state cursor", "cursor":"%s"}'
HCERR1505 = '{"id":"HCERR1505", "message":"Enabled peers down", "peer_count":"%d", "peers":"%s"}'
HCERR1506 = '{"id":"HCERR1506", "message":"Failed to save scheduled search", "export":"%s", "error":"%s"}'
HCERR1507 = '{"id":"HCERR1507", "message":"Invalid search string. Only search whose first command is \'search\' can be exported.", "search":"%s"}'
HCERR1508 = '{"id":"HCERR1508", "message":"Error in \'runexport\' command", "error":"%s"}' 
HCERR1509 = '{"id":"HCERR1509", "message":"Failed to create cli job", "error":"%s"}'
HCERR1510 = '{"id":"HCERR1510", "message":"Failed to get configuration", "export":"%s", "error":"%s"}'
HCERR1511 = '{"id":"HCERR1511", "message":"field is not set or set to all whitespace in cluster configuration", "field":"%s"}'
HCERR1512 = '{"id":"HCERR1512", "message":"Canceled export search(es) due to error(s).", "error_count":"%d", "child_sids":"%s", "errors":"%s"}'
HCERR1513 = '{"id":"HCERR1513", "message":"Error in \'exporthdfs\' command", "error":"%s"}'
HCERR1514 = '{"id":"HCERR1514", "message":"Search has ran into error(s), stopping export ...", "error_count":"%d", "errors":"%s"}'
HCERR1515 = '{"id":"HCERR1515", "message":"path is used by another cluster configuration, cannot be used again", "path":"%s", "cluster":"%s"}'
HCERR1516 = '{"id":"HCERR1516", "message":"Failed to delete cluster configuration", "name":"%s", "error":"%s"}'
HCERR1517 = '{"id":"HCERR1517", "message":"Hadoop version mismatch", "local_version":"%s", "remote_version":"%s"}'
HCERR1518 = '{"id":"HCERR1518", "message":"Failed to save cluster configuration", "name":"%s", "error":"%s"}'
HCERR1519 = '{"id":"HCERR1519", "message":"Failed to format an event data", "format":"%s", "error":"%s"}'
HCERR1520 = '{"id":"HCERR1520", "message":"Failed to delete kerberos configuration", "name":"%s", "error":"%s"}'
HCERR1521 = '{"id":"HCERR1521", "message":"Failed to save kerberos configuration", "name":"%s", "error":"%s"}'

# REST endpoint errors 2001-2100
HCERR2001 = '{"id":"HCERR2001", "message":"Failed to get entity object", "entity_path":"%s", "entity_name":"%s", "uri":"%s", "error":"%s"}'
HCERR2002 = '{"id":"HCERR2002", "message":"Failed to get entities object", "entity_path":"%s", "search":"%s", "uri":"%s", "error":"%s"}'
HCERR2003 = '{"id":"HCERR2003", "message":"Failed to save conf object", "conf":"%s", "stanza":"%s", "error":"%s"}'
HCERR2004 = '{"id":"HCERR2004", "message":"Failed to delete entity object", "entity_path":"%s", "entity_name":"%s", "error":"%s"}'
HCERR2005 = '{"id":"HCERR2005", "message":"Invalid custom action", "custom_action":"%s", "eai_action":"%s", "error":"%s"}'
HCERR2006 = '{"id":"HCERR1526", "message":"Bad http request", "uri":"%s", "status":"%d", "reason":"%s", "response":"%s"}'
HCERR2007 = '{"id":"HCERR2007", "message":"Stanza already exists", "stanza":"%s"}'
HCERR2008 = '{"id":"HCERR2008", "message":"Invalid stanza name", "stanza":"%s", "valid_format":"%s"}'
HCERR2009 = '{"id":"HCERR2009", "message":"Failed to parse hdfs-site", "error":"%s"}'

# index errors 2101-2500


# import errors 2501-3000


# explore errors 3001-3500


# convert backend error message to user friendly error message
def toUserFriendlyErrMsg(e, prefixErrorCode=False):
    if not isinstance(e, HcException):
        msg = str(e)
    else:
        if e.code == 'HCERR0001':
            msg = "Failed to run Hadoop CLI job command '%s' with options '%s': %s." % (e.error['cmd'], e.error['options'], e.error['error'])
        elif e.code == 'HCERR0003':
            msg = "Invalid HADOOP_HOME. Cannot find Hadoop command under bin directory HADOOP_HOME='%s'" % (e.error['HADOOP_HOME'])
        elif e.code == 'HCERR0005':
            msg = "Invalid JAVA_HOME. Cannot find Java command under bin directory JAVA_HOME='%s'." % (e.error['JAVA_HOME'])
        elif e.code == 'HCERR0006':
            msg = "Could not find Kerberos keytab file '%s' for principal '%s'." % (e.error['keytab'], e.error['principal'])
        elif e.code == 'HCERR0007':
            msg = "Could not find configuration for cluster '%s', please configure your cluster first." % (e.error['cluster'])
        elif e.code == 'HCERR0008':
            msg = "CLI job is already in progress."
        elif e.code == 'HCERR0009':
            msg = "Failed to run local FS job command '%s' with options '%s': %s." % (e.error['cmd'], e.error['options'], e.error['error'])
        elif e.code == 'HCERR0010':
            msg = "Failed to parse the output of list command at '%s': %s." % (e.error['path'], e.error['error'])
        elif e.code == 'HCERR0011':
            msg = "Failed to renew Kerberos TGT: %s." % (e.error['error'])
        elif e.code == 'HCERR0012':
            msg = "Invalid Hadoop CLI command path: %s." % (e.error['path'])
        elif e.code == 'HCERR0013':
            msg = "Failed to get local Hadoop version: %s." % (e.error['error'])
        elif e.code == 'HCERR0014':
            msg = "Failed to get remote Hadoop version (namenode=%s, port=%d): %s." % (e.error['host'], e.error['port'], e.error['error'])
        elif e.code == 'HCERR0015':
            msg = "Unable to connect to Hadoop cluster '%s' with principal '%s': %s." % (e.error['uri'], e.error['principal'], e.error['error'])
        elif e.code == 'HCERR0016':
            msg = "Path does not exist: %s." % (e.error['path'])
        elif e.code == 'HCERR0501':
            msg = "Missing required argument: %s." % (e.error['argument'])
        elif e.code == 'HCERR0502':
            msg = "Invalid argument %s='%s': %s." % (e.error['name'], e.error['value'], e.error['error'])
        elif e.code == 'HCERR0503':
            msg = "Invalid argument %s='%s'. Accepted values are: %s." % (e.error['name'], e.error['value'], e.error['accepted_values'])
        elif e.code == 'HCERR0504':
            msg = "'%s' cannot be an empty string or all whitespace characters." % (e.error['name'])
        elif e.code == 'HCERR0506':
            msg = "Invalid directive '%s', accepted directives are: %s." % (e.error['directive'], e.error['accepted_values'])
            
        elif e.code == 'HCERR1001':
            msg = "Parent search job '%s' is not running. Stopping ..." % (e.error['sid'])
        elif e.code == 'HCERR1002':
            msg = "Failed to get parent search (sid='%s'): %s." % (e.error['sid'], e.error['error'])
        elif e.code == 'HCERR1005':
            msg = "No configuration found for cluster: %s." % (e.error['uri'])
        elif e.code == 'HCERR1006':
            msg = "The \'read\' directive is not allowed to read a directory: %s." % (e.error['dir'])
        elif e.code == 'HCERR1007':
            msg = "Found error in \'hdfs\' command: %s." % (e.error['error'])
        elif e.code == 'HCERR1008':
            msg = "The \'read\' directive is not allowed to read the wildcarded path: %s." % (e.error['path'])
                
        elif e.code == 'HCERR1501':
            msg = "Unable to update the status for export '%s': %s." % (e.error['export'], e.error['error'])
        elif e.code == 'HCERR1502':
            msg = "Failed to update the export cursor '%s': %s." % (e.error['cursor'], e.error['error'])
        elif e.code == 'HCERR1504':
            msg = "Found unexpected partial state cursor: %s." % (e.error['cursor'])
        elif e.code == 'HCERR1505':
            msg = "Found %d peer(s) enabled but not running, the export job cannot run until all peers are running. Peers: %s." % (e.error['peer_count'], e.error['peers'])
        elif e.code == 'HCERR1506':
            msg = "Failed to save scheduled search for export '%s': %s." % (e.error['export'], e.error['error'])
        elif e.code == 'HCERR1507':
            msg = "Invalid search string. Only searches where the first command is \'search\' can be exported. search='%s'." % (e.error['search'])
        elif e.code == 'HCERR1508':
            msg = "Error in \'runexport\' command: %s." % (e.error['error'])
        elif e.code == 'HCERR1509':
            msg = "Failed to create the CLI job: %s." % (e.error['error'])
        elif e.code == 'HCERR1510':
            msg = "Failed to get the configuration for export '%s': %s." % (e.error['export'], e.error['error'])
        elif e.code == 'HCERR1511':
            msg = "'%s' is not set or consists of all white spaces, please correct this in your cluster configuration." % (e.error['field'])
        elif e.code == 'HCERR1512':
            msg = "Cancelled export search(es) due to %d error(s) in the child search jobs: %s." % (e.error['error_count'], e.error['child_sids'])
        elif e.code == 'HCERR1513':
            msg = "There was an error in the \'exporthdfs\' command: %s." % (e.error['error'])
        elif e.code == 'HCERR1514':
            msg = "This search has run into %d error(s), stopping export ..." % (e.error['error_count'])
        elif e.code == 'HCERR1515':
            msg = "The path '%s' is used by another cluster configuration '%s', and cannot be used again." % (e.error['path'], e.error['cluster'])
        elif e.code == 'HCERR1516':
            msg = "Failed to delete cluster configuration '%s': %s." % (e.error['name'], e.error['error'])
        elif e.code == 'HCERR1517':
            msg = "Hadoop version mismatch occured. The local version is: %s, the remote version is: %s" % (e.error['local_version'], e.error['remote_version'])
        elif e.code == 'HCERR1518':
            msg = "Failed to save cluster configuration '%s': %s." % (e.error['name'], e.error['error'])
        elif e.code == 'HCERR1519':
            msg = "Failed to convert an event to '%s' format: %s." % (e.error['format'], e.error['error'])
        elif e.code == 'HCERR1520':
            msg = "Failed to delete Kerberos configuration '%s': %s." % (e.error['name'], e.error['error'])
        elif e.code == 'HCERR1521':
            msg = "Failed to save Kerberos configuration '%s': %s." % (e.error['name'], e.error['error'])
        
        elif e.code == 'HCERR2002':
            msg = "Failed to get entities\' object '%s'." % (e.error['entity_path'])
        elif e.code == 'HCERR2009':
            msg = "Failed to parse hdfs_site: %s" % (e.error['error'])
               
        else:
            msg = e.message
        if prefixErrorCode:
            msg = "%s: %s" % (e.code, msg)
    return msg

def getMessageFromRestError (error, prefixErrorCode=False):
    try:
        errorJSON = '{' + error.split('"{',1)[1].rsplit('}"',1)[0] + '}'
        myException = HcException(errorJSON)
        return toUserFriendlyErrMsg(myException, prefixErrorCode)
    except Exception:
        pass
    return error

class HcException(Exception):
    def __init__(self, message_format, options={}):
        self.message_format = message_format
        self.options = options
        
        self.value = message_format
        if len(options) > 0:
            try:
                d = json.loads(message_format)
                for k,v in options.iteritems():
                    d[k] = v
                self.value = json.dumps(d)
            except:
                logger.exception('Failed to populate message, message_format=%s, options=%s', message_format, str(options))
                try:
                    d = json.loads(message_format)
                    self.value = d['message']
                    d = json.loads(HCERR0000)
                    d['error'] = self.value
                    self.message_format = HCERR0000
                    self.options = {'error':d['error']}
                    self.value = json.dumps(d)
                except:
                    logger.exception('Failed to populate HCERR0000 error message')
                    self.message_format = HCERR0000
                    self.options = {'error':''}
                    self.value = HCERR0000

        self.error = json.loads(self.value)
        self.code = self.error['id']
        self.message = self.error['message']

    def __str__(self):
        return self.value

if __name__ == '__main__':
    msg = "In handler 'conf-clusters': Could not find object id=sveserv51-vm1.sv.splunk.com:9000"
    uri = '/servicesNS/nobody/HadoopConnect/configs/conf-clusters/sveserv51-vm1.sv.splunk.com%3A9000'
    json.loads(HCERR2002)
    e = HcException(HCERR2002, {'entity_path':'', 'search':'', 'uri':uri, 'error':msg})
    
    # case 0: fill up a int value and get it back later
    print 'case 0'
    d = json.loads(HCERR1505)
    d['peer_count'] = 5
    s = json.dumps(d)
    d = json.loads(s)
    print 'peer_count: %d' % d['peer_count']
    print 
    
    # case 1, msg is a simple error message, manually construct a json error message, then convert to dict object, should pass test
    print 'case 1'
    msg = "In handler \'savedsearch\': Unable to create a saved search with name \'ExportSearch:test-error-message\'. A saved search with that name already exists."
    json_str = HCERR1506 % ('test-error-message', msg)
    print json_str
    print json.loads(json_str)
    print 
    
    # case 2: msg is very complex: 
    # 1) manually build dict object 2) convert dict to json error message 3) convert json string back to dict. should pass test 
    print 'case 2'
    msg = "[HTTP 409] [{'text': \"In handler 'savedsearch': Unable to create saved search with name 'ExportSearch: test-error-message'. A saved search with that name already exists.\", 'code': None, 'type': 'ERROR'}]"
    # 1) manually build dict object
    d = {}
    d['id'] = 'HCERR1506'
    d['message'] = 'Failed to save scheduled search'
    d['export'] = 'test-error-message'
    d['error'] = msg
    # 2) convert dict to json error message
    json_str = json.dumps(d)
    print json_str
    # 3) convert json string back to dict
    d = json.loads(json_str)
    print d
    print d['id']
    print d['message']
    print d['export']
    print d['error']
    print 
    
    # case 3: msg is very complex, but HCERR1506 is a simple json string and is sure to be able to convert to dict. 
    # 1) convert HCERR1506 to dict
    # 2) fill up the blank (export and error in this case)
    # 3) convert dict to json error message
    # 4) convert json string back to dict
    # should pass test.
    print 'case 3' 
    # 1) convert HCERR1506 to dict
    d = json.loads(HCERR1506)
    # 2) fill up the blank (export and error in this case)
    d['export'] = 'test-error-message'
    d['error'] = msg
    # 3) convert dict to json error message
    json_str = json.dumps(d)
    print json_str
    # 4) convert json string back to dict
    d = json.loads(json_str)
    print d
    print d['id']
    print d['message']
    print d['export']
    print d['error']
    print 
    
    # case 4: msg is very complex. 
    # 1) create HcExeption with msg 2) get error dict from the exception
    # should pass test
    print 'case 4'
    # 1) create HcExeption with msg 
    e = HcException(HCERR1506, {'export':'test-error-message', 'error':msg})
    # 2) get error dict from the exception
    d = e.error
    print d
    print d['id']
    print d['message']
    print d['export']
    print d['error']
    print 
    
    # case 5: msg is very complex. 
    # 1) create HcExeption with msg and more fields (foo) than it is needed 2) get error dict from the exception
    # should pass test
    print 'case 5'
    # 1) create HcExeption with msg 
    e = HcException(HCERR1506, {'export':'test-error-message', 'error':msg, 'foo':'bar'})
    # 2) get error dict from the exception
    d = e.error
    print d
    print d['id']
    print d['message']
    print d['export']
    print d['error']
    print d['foo']
    print 
    
    # case 6: msg is very complex. 
    # 1) create HcExeption with msg and less fields (export) than it is needed 2) get error dict from the exception
    # should pass test
    print 'case 6'
    # 1) create HcExeption with msg 
    e = HcException(HCERR1506, {'error':msg})
    # 2) get error dict from the exception
    d = e.error
    print d
    print d['id']
    print d['message']
    print d['export']
    print d['error']
    print 
    
    # case 7: msg is very complex
    # 1) manually construct a json error string
    # 2 ) convert it to dict object
    # should fail test
    print 'case 7' 
    # 1) manually construct a json error string
    json_str = HCERR1506 % ('test-error-message', msg)
    print json_str
    # 2 ) convert it to dict object
    print json.loads(json_str)
    
