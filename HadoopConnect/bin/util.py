import time,sys,csv
from urllib.parse import urlparse
from errors import *
import inspect

# change field size limit to 1MB
csv.field_size_limit(1 * 1024 * 1024 * 1024)

def formatLsOutput(path):
    import os, stat
    result = ''
    si = os.stat(path)
    # get acl
    # returns permission bits as integer, oct(m) returns oct string literal prefixed with 0   
    m = stat.S_IMODE(si.st_mode) 
    acl = list('----------') if os.path.isfile(path) else list('d---------')
    acl[1] = 'r' if m & stat.S_IRUSR else '-'
    acl[2] = 'w' if m & stat.S_IWUSR else '-'
    acl[3] = 'x' if m & stat.S_IXUSR else '-'
    acl[4] = 'r' if m & stat.S_IRGRP else '-'
    acl[5] = 'w' if m & stat.S_IWGRP else '-'
    acl[6] = 'x' if m & stat.S_IXGRP else '-'
    acl[7] = 'r' if m & stat.S_IROTH else '-'
    acl[8] = 'w' if m & stat.S_IWOTH else '-'
    acl[9] = 'x' if m & stat.S_IXOTH else '-'
    result += ''.join(acl)
    
    # get replication
    #FIXME get replication
    result += ' 0'
    
    # get user name
    uname = str(si.st_uid)
    try: 
        import pwd
        uname = pwd.getpwuid(si.st_uid).pw_name
    except: 
        #logger.exception('Failed to get user name from uid')
        pass # if we cannot get name from uid, just display the uid
    result += ' '+uname
    
    # get group name
    gname = str(si.st_gid)
    try:
        #FIXME figure out how to load grp model
        # http://stackoverflow.com/questions/10910096/command-to-get-groupid-of-a-group-name-in-mac-or-linux  
        import grp
        #grp = __import__('grp')
        gname = grp.getgrgid(si.st_gid).gr_name
    except:
        #logger.exception('Failed to get group name from gid')
        pass # if we cannot get name from gid, just display gid
    result += ' '+gname
    
    # size
    result += ' %d' % si.st_size
    
    st = time.localtime(si.st_mtime)
    # date
    result += ' '+time.strftime('%Y-%m-%d', st)
    
    # time
    result += ' '+time.strftime('%H:%M', st)
    
    # path
    result += ' '+path+'\n'
        
    return result 
    
def getProperty(callerArgs, name, conf, defaultValue=None):
    value = None
    if name in callerArgs:
        value = callerArgs[name]   
    elif name in conf:
        value = conf[name]
    elif defaultValue == None:
        raise HcException(HCERR0501, {'argument':name})
    else:
        value = defaultValue
        return value
        
    if type(value) is list:
        value = value[0]
    
    if type(value) is str:
        value = value.strip()
        if len(value) == 0:
            value = None
        
    return value
    
def splitList(arr, parts=2):
    length = len(arr)
    parts  = min(parts, length)
    return [ arr[ int(i*length/parts) : int((i+1)*length / parts)] for i in range(parts) ]

#ERP-681
def makeFileSystemSafe(path, replace_list=[[':','_'], ['/','__'], ['[','___'],[']','___']]):
    #NOTE: ':' is safe but it can cause problems because it is used as path separator
    safe_path = path
    for item in replace_list:
        safe_path = safe_path.replace(item[0], item[1])
    return safe_path

def unquote(s, unescape = False):
    if type(s) is not str:
        return s
    if s == '' or s == None:
        return s
    elif s.startswith("'"):
        return s.lstrip("'").rstrip("'").replace(r"\'","'") if unescape else s.lstrip("'").rstrip("'")
    elif s.startswith('"'):
        return s.lstrip('"').rstrip('"').replace(r'\"','"') if unescape else s.lstrip('"').rstrip('"')
    else: 
        return s.replace(r'\"','"') if unescape else s
    
def getDispatchDir(sid, sharedStorage=None):
    dispatch_dir = sharedStorage
    if dispatch_dir == None:
        dispatch_dir = os.environ['SPLUNK_HOME']
    logger.error("[---] Splunk Debug printing Splunk_HOME: {}".format(dispatch_dir))
    logger.error("[---] Splunk Debug printing SID: {}".format(sid))   
    logger.error("[---] Splunk Debug printing Inspect Results: {}".format(inspect.stack()[1]))  
    dispatch_dir   = os.path.join(dispatch_dir, 'var', 'run', 'splunk', 'dispatch', sid)
    logger.error("[---] Splunk Debug printing Dispatch Dir: {}".format(dispatch_dir))
    return dispatch_dir
        
def getBaseURI(uri):
    p = urlparse(uri)
    if p.scheme == '' or p.scheme == None or not (uri.startswith('file://') or uri.startswith('hdfs://')):
        raise HcException(HCERR0502, {'name':'uri', 'value':uri, 'error':'accepted schemes: file://, hdfs://'})
    if p.scheme == 'file':
        return 'file://'
    return p.scheme + '://' + p.netloc

def getBaseURIAndPath(uri):
    base_uri = getBaseURI(uri)
    uri_path = uri[len(base_uri):]
    if base_uri.startswith("file://") and not os.path.isabs(uri_path):
       raise HcException(HCERR0502, {'name':'uri', 'value':uri, 'error':'the destination needs to be an absolute path'})     
    return base_uri, uri_path
        
def writeToCSV(w, row=None):
    if row == None:
        w.writeheader()
    else:
        w.writerow(row)   

##### START: yanked from Intersplunk.py so we avoid having to import it - (takes ~20ms to import) ##### 
def win32_utf8_argv():
    """Uses shell32.GetCommandLineArgvW to get sys.argv as a list of UTF-8                                           
    strings.                                                                                                         
                                                                                                                     
    Versions 2.5 and older of Python don't support Unicode in sys.argv on                                            
    Windows, with the underlying Windows API instead replacing multi-byte                                            
    characters with '?'.                                                                                             
                                                                                                                     
    Returns None on failure.                                                                                         
                                                                                                                     
    Example usage:                                                                                                   
                                                                                                                     
    >>> def main(argv=None):                                                                                         
    ...    if argv is None:                                                                                          
    ...        argv = win32_utf8_argv() or sys.argv                                                                  
    ...                                                                                                              
    """

    try:
        from ctypes import POINTER, byref, cdll, c_int, windll
        from ctypes.wintypes import LPCWSTR, LPWSTR

        GetCommandLineW = cdll.kernel32.GetCommandLineW
        GetCommandLineW.argtypes = []
        GetCommandLineW.restype = LPCWSTR

        CommandLineToArgvW = windll.shell32.CommandLineToArgvW
        CommandLineToArgvW.argtypes = [LPCWSTR, POINTER(c_int)]
        CommandLineToArgvW.restype = POINTER(LPWSTR)

        cmd = GetCommandLineW()

        argc = c_int(0)
        argv = CommandLineToArgvW(cmd, byref(argc))
        if argc.value > 0:
            # Remove Python executable if present                                                                    
            if argc.value - len(sys.argv) == 1:
                start = 1
            else:
                start = 0
            return [argv[i].encode('utf-8') for i in
                    xrange(start, argc.value)]
    except Exception:
        pass

# from sys.argv, get key=value args as well as other plain keyword args (e.g. "file")
def getKeywordsAndOptions():
    import re
    keywords = []
    kvs = {}
    first = True

    # SPL-30670 - handle unicode args specially in windows
    argv = win32_utf8_argv() or sys.argv

    # for each arg
    for arg in argv:
        if first:
            first = False
            continue
        # handle case where arg is surrounded by quotes
        # remove outter quotes and accept attr=<anything>
        if arg.startswith('"') and arg.endswith('"'):
            arg = arg[1:-1]
            matches = re.findall('(?:^|\s+)([a-zA-Z0-9_-]+)\\s*(::|==|=)\\s*(.*)', arg)
        else:
            matches = re.findall('(?:^|\s+)([a-zA-Z0-9_-]+)\\s*(::|==|=)\\s*((?:[^"\\s]+)|(?:"[^"]*"))', arg)
        if len(matches) == 0:
            keywords.append(arg)
        else:
            # for each k=v match
            for match in matches:
                attr, eq, val = match
                # put arg in a match
                kvs[attr] = val
    return keywords,kvs

##### END: yanked from Intersplunk.py so we avoid having to import it !!! ##### 



        
class SearchResultsInfo:
    def __init__(self):
        self.header = []
        self.info = {}
        self.messages = []
        self.dirty = False
        self.path  = None
        self.countMap = None 

    def readFrom(self, path):
        self.path = path
        with open(path, 'r') as f:
            r = csv.reader(f)
            self.header = next(r)
            self.info = dict(zip(self.header, next(r)))

            # parse _countMap
            if "_countMap" in self.info:
               p = self.info["_countMap"].split(';')
               m = {}
               for i in range(0, len(p)-1, 2):
                   m[p[i]] = p[i+1]
               self.countMap = m

            try:
                while True:
                    msg = dict(zip(self.header, next(r)))
                    self.messages.append(msg)
            except StopIteration as sp:
                pass       

    def updateMetric(self, metric, spent_ms, inv=1):
         if not self.countMap:
            return 
         self.countMap['invocations.' + metric ] = int(self.countMap.get('invocations.'+metric, '0')) + inv
         self.countMap['duration.' + metric]     = int(self.countMap.get('duration.'+metric, '0')) + spent_ms
         self.dirty = True

    def checkAndAddHeaderCols(self, cols):
        for k in cols:
            if not k in self.header:
               self.header.append(k)

    def serializeTo(self, out):
        if self.countMap != None:
           p = []
           for k,v in self.countMap.items():
               p.append(str(k))
               p.append(str(v))
           self.info["_countMap"] = ';'.join(p) + ';'

        #ensure the keys are all present in the header
        self.checkAndAddHeaderCols(self.info.keys())
        for m in self.messages:
            self.checkAndAddHeaderCols(m.keys())

        w = csv.DictWriter(out, fieldnames=self.header, restval='')
        writeToCSV(w)
        writeToCSV(w, self.info)
        for m in self.messages: 
            writeToCSV(w, m)

    def writeOut(self):
        if not self.dirty or self.path == None:
           return
      
        tmp_path = self.path + '.tmp'
        with open(tmp_path, 'w') as f:
           self.serializeTo(f)
        logger.error("[---] Splunk Debug is Writing out to this path: {} from {}".format(self.path,tmp_path))
        os.rename(tmp_path, self.path)
        
        # update atime/mtime of the file so search process reloads the info
        next_sec = (time.time() + 1)
        os.utime(self.path, (next_sec, next_sec))   
 
    def get(self, name, def_val=None):
        return self.info.get(name, def_val)

    def set(self, name, val):
        self.info[name] = val
        self.dirty = True

    def finalizeSearch(self):
       self.set('_request_finalization', '1')
       self.set('_query_finished', '1')
       
    def addErrorMessage(self, msg):
        self.addMessage("ERROR", msg)

    def addWarnMessage(self, msg):
        self.addMessage("WARN", msg)

    def addInfoMessage(self, msg):
        self.addMessage("INFO", msg)

    def addDebugMessage(self, msg):
        self.addMessage("DEBUG", msg)

    def addMessage(self, type, msg):
        self.messages.append({"msgType": type, "msg": msg})
        self.dirty = True
     
    def getErrorMessages(self):
        return self.getMessages("ERROR")

    def getWarnMessages(self):
        return self.getMessages("WARN")

    def getInfoMessages(self):
        return self.getMessages("INFO")

    def getDebugMessages(self):
        return self.getMessages("DEBUG")
    
    def getMessages(self, type):
        result = [] 
        for msg in self.messages:
            if msg.get('msgType', '') == type:
               result.append(msg.get('msg', ''))

        return result
    


class BaseSplunkResultHandler:
    def __init__(self):
       pass

    # called with settings sent by search process
    def handleSettings(self, settings, keywords, argvals):
        pass

    # called with csv header, should return header to output or None  
    def handleHeader(self, header):
        return None

    # called for each results, should return the result to output or None  
    def handleResult(self, result):
        return None
     
    # called after last result is read in, should return a list of results to output or None  
    def handleFinish(self):
        return None


class SplunkResultStreamer:
   def __init__(self, handler, din=sys.stdin, dout=sys.stdout):
      self.settings = {}
      self.header   = []
      self.handler  = handler
      self.din      = din
      self.dout     = dout 
      self.total_events = 0     #total number of events processed
      pass

   def populateSettings(self, file=sys.stdin):
       for line in file:
          if line == '\n':
              break
          parts = line.strip().split(':', 1)
          if len(parts) != 2:
             continue
          parts[1] = unquote(parts[1])
          self.settings[parts[0]] = parts[1]   
          
   def run(self):

       #1. read settings
       self.populateSettings(file=self.din)

       #TODO: parse authString XML
       keywords, argvals = getKeywordsAndOptions()
       self.handler.handleSettings(self.settings, keywords, argvals)

       cr = csv.reader(self.din)
       cw = csv.writer(self.dout)
       
       try:
          #2. csv: read header
          self.header = next(cr)
          out_header = self.handler.handleHeader(self.header)
          if out_header != None:
              self.dout.write('\n')  # end output header section
              writeToCSV(cw, out_header)
                  
          #3. csv: read input results
          for row in cr:
              out_row = self.handler.handleResult(row)
              if out_row != None and out_header != None:
                  writeToCSV(cw, out_row)
              self.total_events += 1    
       except StopIteration as sp:
          pass
       finally:       
          #4. get any rows withheld by the handle until it sees finish
          out_rows = self.handler.handleFinish()
          if out_rows != None and out_header != None:
              for out_row in out_rows:
                  writeToCSV(cw, out_row)



