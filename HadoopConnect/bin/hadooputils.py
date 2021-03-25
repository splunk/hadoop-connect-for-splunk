import os,sys,subprocess,time
from util import *
from constants import *
from kerberos import Krb5Principal
from errors import *
import re

def makeHdfsDir(hadoopCliJob, dst, krb5_principal=None):
    # we must make sure dst does not exist before calling mkdir2, 
    # otherwise, mkdir2 will create a -p dir in hadoop 1 rather than taking -p as an command option 
    hadoopCliJob.test('-d', dst)
    if hadoopCliJob.wait() == 0:
        return 

    # since we don't know if we are hitting hadoop 1 or 2
    # and hadoop 1 and 2 mkdir command behave differently,
    # hadoop 1 does not support -p option but       allows to create parent dir if not exists.
    # hadoop 2 does     support -p option but don't allow  to create parent dir if not exists.
    # So, let's first try hadoop 1 mkdir, if failed due to 'No such file or directory',  
    # we know we are on hadoop 2 and trying to create parent dir without using -p  
    hadoopCliJob.mkdir(dst)
    if hadoopCliJob.wait() != 0:
        logger.error("[---] Splunk Debug had to wait for more than 0 seconds to create the directory {}. Going to try with -p".format(dst))
        if hadoopCliJob.getStderr().find('No such file or directory'):
            # at this time, we know we are hitting hadoop 2 and need to use -p
            hadoopCliJob.mkdir2(dst)
            hadoopCliJob.wait(True)
        else:    
            # most likely got 'Permission denied', throw up all errors    
            logger.error("[---] Permission Denied: Splunk Debug")
            raise HcException(HCERR0001, {'cmd':hadoopCliJob.fscmd, 'options':','.join(hadoopCliJob.fsopts), 'error':hadoopCliJob.getStderr()})
 
def validateHadoopHome(env):
    if 'HADOOP_HOME' not in env:
       raise HcException(HCERR0002)

    hadoop_cli = os.path.join(env["HADOOP_HOME"], "bin", "hadoop")
    if not os.path.exists(hadoop_cli):
       raise HcException(HCERR0003, {'HADOOP_HOME':env["HADOOP_HOME"]})

    if 'JAVA_HOME' not in env:
       raise HcException(HCERR0004)

    if not os.path.exists(os.path.join(env["JAVA_HOME"], "bin", "java")):
       raise HcException(HCERR0005, {'JAVA_HOME':env["JAVA_HOME"]})
 
def setupHadoopEnv(hadoop_home, java_home, hadoop_conf_dir, krb_principal=None):
    # setup HADOOP_HOME and HADOOP_CONF_DIR 
    # the later is needed for cluster specific configs, such as kerberos support etc
    if hadoop_home == None or len(hadoop_home.strip()) == 0:
       raise HcException(HCERR1511, {'field':'hadoop_home'})
  
    if java_home == None or len(java_home.strip()) == 0:
       raise HcException(HCERR1511, {'field':'java_home'})

    # start with the env we're called in 
    env = dict(os.environ) #copy 

    if hadoop_home != "$HADOOP_HOME":
       env["HADOOP_HOME"] = hadoop_home  
       # ERP-1104 newer hadoop 2 need these env variables
       env['HADOOP_PREFIX'] = env["HADOOP_HOME"] 
       env['HADOOP_COMMON_HOME'] = env['HADOOP_PREFIX'] 
       env['HADOOP_HDFS_HOME'] = env['HADOOP_PREFIX'] 

    if java_home != "$JAVA_HOME":
       env["JAVA_HOME"] = java_home

    validateHadoopHome(env)

    if hadoop_conf_dir != None and not len(hadoop_conf_dir.strip()) == 0:
       env["HADOOP_CONF_DIR"] = hadoop_conf_dir

    # make cli stop complaining about HADOOP_HOME
    env["HADOOP_HOME_WARN_SUPPRESS"] = "true"

    # set up environment needed for Kerberos access to Hadoop
    if krb_principal == None or len(krb_principal.strip()) == 0:
       return env 
   
    krb5p = Krb5Principal(krb_principal)
    krb5p.validate()  

    # set up env for this principal 
    krb5p.setupEnv(env)  
    return env


class HadoopEnvManager(object):
   namespace = None
   user = None
   sessionKey = None
   krb5_principal = None # default principal if None is provided to getEnv
   env = {}  # maps a (host, port, principal) -> hadoop env

   @classmethod
   def init(cls, app, user, sessionKey, krb5_principal=None):
       cls.namespace = app
       cls.user= user
       cls.sessionKey = sessionKey
       cls.krb5_principal = krb5_principal

   @classmethod
   def getEnv(cls, uri, principal=None):
       if cls.namespace == None:
          raise HcException(HCERR0000, {'error':'HadoopEnvManager.init() must be called before getEnv'})
       from urllib.parse import urlparse
       
       #1. parse uri to get just host:port 
       host = None
       port = None
       p = urlparse(uri) 
       if p.scheme == 'hdfs':
          host = p.hostname
          port = str(p.port) if p.port != None else None  
       else:
          return dict(os.environ)
   
       if principal == None:
          principal = cls.krb5_principal

       key = (host, port, principal) #host, port, principal

       #2. check if env is in cache
       if key in cls.env:
          #TODO: for long running processes we'll need to refresh the TGT if it's too old
          return dict(cls.env[key]) # copy

       #3.use REST to get cluster info from splunkd
       rc = None #result cluster
       hostport = '%s:%s' % (host, port) if port != None else host
       try:
          from splunk.bundle import getConf
          clusters = getConf('clusters', sessionKey=cls.sessionKey, namespace=cls.namespace, owner=cls.user )
          for c in clusters:
              if not c == hostport:
                 continue
              rc = clusters[c]
              break
       except Exception as e:
          logger.exception('Failed to get conf info from clusters.conf')
 
       result = None
       if rc == None:
          raise HcException(HCERR0007, {'cluster':hostport})
       else:
          hadoop_home = rc['hadoop_home']
          java_home   = rc['java_home']
          app_local = os.path.join(os.environ['SPLUNK_HOME'], 'etc', 'apps', APP_NAME, 'local')
          hadoop_conf_dir = os.path.join(app_local, 'clusters', makeFileSystemSafe(hostport))  # use _ instead of : in host:port since : is used as a path separator
          if principal == None:
             principal = rc.get('kerberos_principal', None)

          if principal != None and len(principal.strip()) == 0:
             principal = None


          #TODO: ensure current user has permission to use the given principal
          result = setupHadoopEnv(hadoop_home, java_home, hadoop_conf_dir, principal)
          logger.debug("uri=%s, hadoop_home=%s, java_home=%s" % (uri, result.get('HADOOP_HOME', ''), result.get('JAVA_HOME', '')))
          cls.env[key] = result

       return result

   @classmethod
   def getCliJob(cls, uri, principal=None):
       if uri.startswith('hdfs://'):
          return HadoopCliJob(cls.getEnv(uri, principal))
       elif uri.startswith('file://'):
          return LocalFsJob(dict(os.environ))    
       else:
          raise HcException(HCERR1509, {'error':"Invalid path '%s', accepted schemes: hdfs:// or file://" % uri})

def validatePrincipalAndKeytab(principal):
    krb5p = Krb5Principal(principal)
    krb5p.validate()  
    env = dict(os.environ)
    krb5p.setupEnv(env)

# 1. set KRB5CCNAME env variable
# 2. verify principal works with given keytab file, generate tgt cache file
# 3. verify service_principal works by listing root directory of remote hadoop server with tgt cache file and service principal   
def validateConnectionToHadoop(sessionKey, principal, uri):
    msg = None
    try:
        HadoopEnvManager.init(APP_NAME, 'nobody', sessionKey, principal)
        hj = HadoopCliJob(HadoopEnvManager.getEnv(uri, principal))
        hj.ls(uri)
        hj.wait(True)
    except HcException as e1:
        logger.exception('Failed to validate hadoop connection')
        if 'error' in e1.error and e1.error['error'] and e1.error['error'].lower().find('invalid rule') > 0:
            msg = 'Invalid Kerberos principals to short names mapping rules' 
        else:
            msg = toUserFriendlyErrMsg(e1)
    except Exception as e:
        logger.exception('Failed to validate hadoop connection')
        msg = str(e)
        
    if msg != None:
        raise HcException(HCERR0015, {'uri':uri, 'principal':principal, 'error':msg})
    
def parseHTMLTag(body, lWord, rTag='<tr', extraWord=True):
    v = None
    if extraWord:
        lWord += '<td id="col2"> :<td id="col3">'
    i = body.find(lWord)
    if i > 0:
        j = body[i+len(lWord):].find(rTag)
        if j > 0:
            i += len(lWord)
            v = body[i:i+j].strip()
    return v

def parseHTMLActiveNN(body):
    i = body.find('<h1>NameNode')
    j = body[i:].find('</h1>')
    return body[i:j].find('active') > 0
        
def toBytes(s):
    if not s: return
    v = float(s[:-2].strip())
    if s[-2:].lower() == 'kb':
        v *= 1024
    if s[-2:].lower() == 'mb':
        v *= 1024 ** 2
    if s[-2:].lower() == 'gb':
        v *= 1024 ** 3
    if s[-2:].lower() == 'tb':
        v *= 1024 ** 4
    if s[-2:].lower() == 'pb':
        v *= 1024 ** 5
    return v    

def getHadoopClusterInfoFromJmx(url, convertKeyToLowercase=False):
    info = None
    result = None
    try:
        import urllib2
        import json
        response = urllib2.urlopen(url)
        json_raw = response.read()
        json_object = json.loads(json_raw)
        info = json_object['beans'][0]
        result = {}
        for k,v in info.items():
            if convertKeyToLowercase:
                k = k.lower()
            if type(v) is unicode:
                v = str(v)
            result[k] = v    
    except Exception as e:
        logger.exception("Cannot read jmx metrics from url:"+url)
    return result

def getHadoopClusterInfoFromJsp(url, convertKeyToLowercase=False):
    info = None
    try:
        import urllib2
        response = urllib2.urlopen(url)
        body = response.read()
        info = {}
        info['version' if convertKeyToLowercase else 'Version'] = parseHTMLTag(body, 'Version: <td>', extraWord=False)
        info['total' if convertKeyToLowercase else 'Total'] = toBytes(parseHTMLTag(body, 'Configured Capacity'))
        info['total' if convertKeyToLowercase else 'Used'] = toBytes(parseHTMLTag(body, 'DFS Used'))
        info['free' if convertKeyToLowercase else 'Free'] = toBytes(parseHTMLTag(body, 'DFS Remaining'))
        info['nondfsusedspace' if convertKeyToLowercase else 'NonDfsUsedSpace'] = toBytes(parseHTMLTag(body, 'Non DFS Used'))
        info['percenUsed' if convertKeyToLowercase else 'PercenUsed'] = parseHTMLTag(body, 'DFS Used%')
        info['percentremaining' if convertKeyToLowercase else 'PercentRemaining'] = parseHTMLTag(body, 'DFS Remaining%')
        info['tag.hastate' if convertKeyToLowercase else 'tag.HAState'] = parseHTMLActiveNN(body)
    except Exception as e:
        logger.exception("Cannot parse dfshealth.jsp page from url:"+url)
    return info

def getHadoopClusterInfo(host, port=50070):
    #use wildcard to ensure the query works with Apache and CDH distros
    url = "http://%s:%d/jmx?qry=*adoop:service=NameNode,name=NameNodeInfo" % (host, port)
    info = getHadoopClusterInfoFromJmx(url)
    if not info:
         # if jmx is turned off, try to parse the jsp page
         url = "http://%s:%d/dfshealth.jsp" % (host, port)
         info = getHadoopClusterInfoFromJsp(url)
    return info

def getHadoopHAClusterInfo(host, port=50070):
    url = "http://%s:%d/jmx?qry=*adoop:service=NameNode,name=FSNamesystem" % (host, port)
    info = getHadoopClusterInfoFromJmx(url, True)
    if not info:
        url = "http://%s:%d/jmx?qry=*adoop:service=NameNode,name=FSNamesystemMetrics" % (host, port)
        info = getHadoopClusterInfoFromJmx(url, True)
    if not info:
         # if jmx is turned off, try to parse the jsp page
         url = "http://%s:%d/dfshealth.jsp" % (host, port)
         info = getHadoopClusterInfoFromJsp(url, True)
    return info
    
def isSecureHadoop(host, port=9000):
    hdl = HDFSDirLister()
    hdl.ls("hdfs://%s:%d/" % (host, port))
    return (hdl.error and hdl.error.find('Authentication is required')>0)

class HadoopCliJob:

   def __init__(self, env, validate=True):
      self.hadoop_cli = None
      if validate:
         validateHadoopHome(env)
         hadoop_home     = env.get("HADOOP_HOME")
         self.hadoop_cli = os.path.join(hadoop_home, "bin", "hadoop")
      self.env        = env
      self.process    = None
      self.rv         = None 
      self.input      = None
      self.starttime  = None  
      self.endtime    = None
      self.fscmd      = None
 
   def popen(self, cmd='fs', shell=False, *options):
        if self.process != None:
           raise HcException(HCERR0008, {'cmd':cmd, 'options':'' if not options else ','.join(options)})
        args = [self.hadoop_cli, cmd]
        if len(options) > 0:
            args.extend(options)
        self.fscmd = options[0]
        self.fsopts = [] if len(options) <= 1 else options[1:]
        self.starttime = time.time()
        self.process = subprocess.Popen(args, shell=shell, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.env)
   
   def fsShell(self, *options):
       self.popen('fs', False, *options)
       
   def moveFromLocal(self, src, dst):
       self.fsShell('-moveFromLocal', src, dst)

   def mv(self, src, dst):
       self.fsShell('-mv', src, dst)

   def rm(self, src, skipTrash=True):
       if skipTrash:
           self.fsShell('-rm', '-skipTrash', src)
       else:
           self.fsShell('-rm', src)
 
   def rmr(self, src, skipTrash=True):
       if skipTrash:
           self.fsShell('-rm', '-skipTrash', src)
       else:
           self.fsShell('-rmr', src)
 
   def cat(self, src):
       self.fsShell('-cat', src)

   def text(self, src):
       self.fsShell('-text', src)

   def get(self, src, localdst):
       self.fsShell('-get', src, localdst)
       
   def touchz(self, src):
       self.fsShell('-touchz', src)

   def ls(self, src):
       self.fsShell('-ls', src)

   def lsr(self, src):
       self.fsShell('-lsr', src)
   
   # make dirs including parents, used for pre hadoop 2.0 
   def mkdir(self, path):
       self.fsShell('-mkdir', path)
   
   # make dirs including parents, used for post hadoop 2.0
   def mkdir2(self, path):
       self.fsShell('-mkdir', '-p', path)
 
   def setrep(self, path, replication, wait=True, recursive=True):
       args = ['-setrep']
       if wait:
           args.append('-w')
       args.append(str(replication))
       if recursive:
           args.append('-R')
       args.append(path)
       self.fsShell(*args)
       
   def test(self, option, path):
       if option not in ['-e', '-z', '-d']:
           raise HcException(HCERR0000, {'error':"Invalid hadoop cli 'test' command option: %s, supported options: -e, -z, -d'" % str(option)})
       self.fsShell('-test', option, path)

   def isDir(self, path):
       self.test("-d", path)
       return self.wait() == 0

   def exists(self, path):
       self.test("-e", path)
       return self.wait() == 0

   def poll(self):
       if self.process == None:
          return self.rv
       return self.process.poll()
 
   def wait(self, raiseOnError=False):
       if self.process == None:
          return self.rv
       self.output = self.process.communicate(self.input) # blocks until cli job is done
       self.endtime = time.time()
       self.rv = self.process.returncode
       self.process = None
       logger.error("[---] Waiting here ... raisedOnError: {}".format(str(raiseOnError)))
       logger.error("[---] new Debug logs: \n Output: {}\nEndtime: {}\nrv: {}\n".format(self.output, self.endtime, self.rv))
       if self.rv != 0:
           # mask failure to remove a non-existant file or directory
           if self.fscmd == "-rm" or self.fscmd == "-rmr":
               if self.getStderr().lower().find('no such file or directory') > -1:
                   self.rv = 0
           # mask failure to not able to read empty file        
           elif self.fscmd == '-text':
               if self.getStderr().strip().find('text: null') > -1:
                   self.rv = 0        
       
       if raiseOnError and self.rv != 0:
           logger.error("Failed to run hadoop cli, cmd="+str(self.fscmd)+", options="+str(self.fsopts)+", error="+self.getStderr())
           logger.error("[---] Waiting since forever - Splunk Debugs")
           raise HcException(HCERR0001, {'cmd':self.fscmd, 'options':','.join(self.fsopts), 'error':self.getStderr()})
       
       return self.rv
       
   def getOutput(self):
       return self.output # (stdout, stderr)    

   def getStderr(self, removeDeprecated=True, removeWarning=True, removeLog4jError=True):
       data = self.output[1]
       if not (removeDeprecated or removeWarning or removeLog4jError):
           return data
       
       tmp = []
       logger.error(data)
       for l in data.decode("utf-8").split('\n'):
           if len(l.strip()) == 0:
               continue
           if removeDeprecated and l.find('DEPRECATED:') >= 0 and l.find('Warning: $HADOOP_HOME is deprecated') >= 0:
               continue
           if removeWarning and l.find('WARN util.NativeCodeLoader') >= 0:
               continue
           if removeLog4jError and l.find('log4j:ERROR') >= 0:
               continue
           if l.find('Unable to load realm mapping info from SCDynamicStore') >= 0:
               continue

           tmp.append(l)
      
       data = '\n'.join(tmp)
       return data  
   
   def getStdout(self):
       if self.process:
           return self.process.stdout
       return ''
   
   def getRuntime(self):
       if self.endtime == None or self.starttime == None:
          return 0
       return self.endtime - self.starttime

class LocalFsJob (HadoopCliJob):
   def __init__(self, env):
       HadoopCliJob.__init__(self, env, False)
       self.streaming = False
       
   def popen(self, cmd=None, shell=False, *options):
        if self.process != None:
            raise HcException(HCERR0008, {'cmd':cmd, 'options':'' if not options else ','.join(options)})
       
        self.fscmd = cmd[1:] if cmd.startswith('-') else cmd
        self.fsopts = [] if not options else options
        self.starttime = time.time()
        self.process = 'dummy'
        self.streaming = False
        
   def fsShell(self, *options):
       #walk over options and ensure that none of them starts with hdfs://
       clean_opt = []
       for o in options:
           if o.startswith('hdfs://'):
              raise HcException(HCERR0502, {'name':'path', 'value':o, 'error':"Unexpected option value, expecting local file system path"})
           elif o.startswith('file://'):
              o = o[7:]
           clean_opt.append(o.strip())
       
       self.popen(clean_opt.pop(0), False, *clean_opt)
           
   def wait(self, raiseOnError=False):    
       if self.process and not self.streaming:
           self.output = ['', '']
           self.rv = 0
           for line in self.getStdout():
               self.output[0] += line 
           self.endtime = time.time()
           self.process = None
 
       # mask failure to remove a non-existent file or directory
       if self.rv != 0 and (self.fscmd == "rm" or self.fscmd == "rmr") and self.getStderr().lower().find('no such file or directory') > -1:
          self.rv = 0
       
       if raiseOnError and self.rv != 0:
           raise HcException(HCERR0009, {'cmd':self.fscmd, 'options':','.join(self.fsopts), 'error':self.getStderr()})

       return self.rv
   
   def getStdout(self):
       self.streaming = True
       self.output = ['', '']
       self.rv = 0
       
       # note: pwd and grp modules only work on Unix 
       import shutil, glob 
       try:
           if self.fscmd == 'ls':
               for path in sorted(glob.glob(self.fsopts[0])):
                   if os.path.isfile(path):
                       yield formatLsOutput(path)
                   else:
                       for p in sorted(os.listdir(path)):
                           p = os.path.join(path, p)
                           yield formatLsOutput(p)
           elif self.fscmd == 'lsr':
               for path in sorted(glob.glob(self.fsopts[0])):
                   for dirpath, dirnames, filenames in os.walk(path):
                       for subdirname in sorted(dirnames):
                           p = os.path.join(dirpath, subdirname)
                           yield formatLsOutput(p)
                       for filename in sorted(filenames):
                           p = os.path.join(dirpath, filename)
                           yield formatLsOutput(p)
           elif self.fscmd == 'cat':
               for path in sorted(glob.glob(self.fsopts[0])):
                   if os.path.isdir(path):
                       self.output[1] = 'cat: %s: Is a directory' % path
                       self.rv = 255
                   elif not os.path.exists(path):
                       self.output[1] = 'cat: %s: No such file or directory' % path
                       self.rv = 255
                   else:
                       with open(path) as f:
                           for line in f:
                               yield line
           elif self.fscmd == 'text':
               for path in sorted(glob.glob(self.fsopts[0])):
                   if os.path.isdir(path):
                       self.output[1] = 'text: %s: Is a directory' % path
                       self.rv = 255
                   elif not os.path.exists(path):
                       self.output[1] = 'text: %s: No such file or directory' % path
                       self.rv = 255
                   elif path[-3:] == '.gz':
                       import gzip
                       with gzip.open(path) as f:
                           for line in f:
                               yield line
                   else:
                       with open(path) as f:
                           for line in f:
                               yield line
           elif self.fscmd == 'test':
               if self.fsopts[0] == '-d':
                   self.rv = 0 if os.path.isdir(self.fsopts[1]) else 255
               elif self.fsopts[0] == '-e':
                   self.rv = 0 if os.path.exists(self.fsopts[1]) else 255
           elif self.fscmd == 'setrep':
                pass
           elif self.fscmd == 'moveFromLocal' or self.fscmd == 'mv':
                shutil.move(self.fsopts[0], self.fsopts[1])
           elif self.fscmd == 'rm':
                # ignore -skipTrash option which is only meant to use with hadoop CLI
                path = self.fsopts[1] if self.fsopts[0] == '-skipTrash' else self.fsopts[0]
                os.unlink(path)
           elif self.fscmd == 'rmr':
                # ignore -skipTrash option which is only meant to use with hadoop CLI
                path = self.fsopts[1] if self.fsopts[0] == '-skipTrash' else self.fsopts[0]
                if os.path.isdir(path):
                   shutil.rmtree(path)
                else:
                   os.unlink(path)
           elif self.fscmd == 'get':
                shutil.copyfile(self.fsopts[0], self.fsopts[1])
           elif self.fscmd == 'mkdir':
                if not  os.path.exists(self.fsopts[0]):
                   os.makedirs( self.fsopts[0])
           elif self.fscmd == 'touchz':
                if not os.path.exists(self.fsopts[0]):
                    with open(self.fsopts[0], 'a+'):
                        os.utime(self.fsopts[0], None)
           else:
                raise HcException(HCERR0000, {'error':"command '%s' is not implemented yet" % self.fscmd})
       except Exception as e:
           logger.exception('Failed to execute shell function')
           self.output[1] = "Failed to run command '%s': %s" % (self.fscmd, str(e))
           self.rv = 255
       return
   
   def poll(self):
       if self.process == None:
          return self.rv
       else:# for job that did not open a process, just return 0  meaning it is done
           return 0 
       


class HDFSLsEntry:
    def __init__(self, parts):
        self.acl         = parts[0]
        self.replication = 0 if parts[1] == '-' else int(parts[1])
        self.user        = parts[2]
        self.group       = parts[3]
        self.size        = int(parts[4])
        self.date        = parts[5]
        self.time        = parts[6]
        self.path        = parts[7]
        # fix absolute paths, Hadoop 2.0 releases seem to output absolute urls
        # self.path is supposed to be an absolute path from root of HDFS, NOT a url 
        if self.path.startswith('hdfs://'):
           sl_idx = self.path.find('/', 8)
           if sl_idx > 0:
              self.path = self.path[sl_idx:] 

    def isdir(self):
        return self.acl.startswith('d')
 
    def __repr__(self):
       return "{acl=%s, replication=%d, user=%s, group=%s, size=%d, date=%s, time=%s, path=%s}" % (self.acl, self.replication, self.user, self.group, self.size, self.date, self.time, self.path)

# some special cases:
# if your kerberos principal is invalid, you may get the following output which is 8 parts which should not be parsed:
# 12/06/27 14:48:59 INFO security.UserGroupInformation: Initiating logout for foo@BAR.COM
# if your group name or path has space, you will have more than 8 parts
# -rwxrwxrwx+   3 foo SPLUNK\Domain Users       7943 2012-10-24 18:06 /path/to/export file name
ls_output_re = re.compile('([drwxtT-]+\+?)\s+([-\d+])\s+(\S+)\s+(.+)\s+(\d+)\s+(\d\d\d\d-\d\d-\d\d)\s+(\d\d:\d\d)\s+(.+$)')

class HDFSDirLister:
    def createHadoopCliJob(self, path, krb5_principal):
        return HadoopEnvManager.getCliJob(path, krb5_principal)
    
    def __init__(self, raiseAll=True):
        self.raiseAll = raiseAll
        self.error = None
       
    def ls(self, path, krb5_principal=None):
        hj = self.createHadoopCliJob(path, krb5_principal)
        hj.ls(path)
        return self._parseOutput(hj)  
 
    def lsr(self, path, krb5_principal=None):
        hj = self.createHadoopCliJob(path, krb5_principal)
        hj.lsr(path)
        return self._parseOutput(hj)  

    def _parseOutput(self, hj):
        for line in hj.getStdout():
           r = HDFSDirLister._parseLine(line)
           if r == None:
               continue
           yield r
        if hj.wait() != 0:
           self.error = hj.getStderr()
           if self.raiseAll:
              raise HcException(HCERR0010, {'path':hj.fsopts[0], 'error':self.error})
 
    @classmethod
    def _parseLine(cls, line):
        m = ls_output_re.search(line.decode('utf-8').strip())
        if m == None:
            return None
        # we assume user name does not have space, but group name or path could have spaces
        parts = [p.strip() for p in m.groups()]
        return HDFSLsEntry(parts)


class HDFSJobWaiter:
   def __init__(self, maxConcurrent=10):
      self.max_concurrent = maxConcurrent
      self.jobs = []
      self.jobs_total = 0
      self.total_time = 0
      self.errors = []

   def wait(self, running=0, raiseOnError=True, suh=None):
         logger.debug("waiting for %d remaining jobs, currently running %d, total ran %s" % (running, len(self.jobs), self.jobs_total))
         max_wait = self.max_concurrent - running
         i = 0
         while max_wait >= 0 and len(self.jobs) > 0:
             finished = []
             # poll jobs first and only wait on finished jobs, otherwise we could be blocked
             # waiting on some long running process while others have finished and become defunct
             for j in self.jobs:
                 rv = j.poll()
                 if rv == None: # not done yet
                    continue

                 try:
                     rv = j.wait(True) # this should return immediately
                 except Exception as e:
                     logger.exception('Failed to wait for CLI job to finish')
                     if raiseOnError:
                         raise e
                     else:
                         self.errors.append(str(e))
                 finally:
                     self.total_time += j.getRuntime()
                     finished.append(j)
              
             for j in finished:
                 self.jobs.remove(j)   
             max_wait -= len(finished)
             
             # if we need to wait on more processes and none of them is done, sleep for some time 
             if max_wait > 0 and len(finished) == 0:
                time.sleep(0.250)             
             
             if suh!=None and i%10==0:
                 suh.updateMovingStatus()
                 i += 1
                 
   def addJob(self, job, raiseOnError=True):
      if len(self.jobs) >= self.max_concurrent:
         self.wait(self.max_concurrent/2, raiseOnError)

      self.jobs.append(job)
      self.jobs_total += 1
  


class HDFSFileMover:
   def __init__(self):
       self.jobwaiter = HDFSJobWaiter(8)
       self.paths = {} 
       self.start = time.time()
       self.valid_dirs  = {} #dirs that we know already exist, assuming no-one deletes in the mean time

   def _mkdirp(self, dst, krb5_principal, raiseOnError=False):
       last_slash = dst.rfind('/')
       logger.error("[---] Splunk Debug is trying to create a directory")
       if last_slash < 0:
          return False

       dst_dir = dst[0:last_slash]
       # already exists
       if dst_dir in self.valid_dirs:
          return True

       cli_job = self.createHadoopCliJob(dst_dir, krb5_principal)
       logger.error("[---] Splunk Debug is trying to create this destination folder: {}".format(dst_dir))
       try:
          makeHdfsDir(cli_job, dst_dir, krb5_principal)
          logger.error("[---] Splunk Debug did make the dst folder")
       except:
          if raiseOnError:
             return False
          logger.error("Failed to make dir on hdfs, dst_dir="+str(dst_dir))
             
       self.valid_dirs[dst_dir] = 1
       return True

   def createHadoopCliJob(self, dst, krb5_principal):
       return HadoopEnvManager.getCliJob(dst, krb5_principal);
   
   def move(self, src, dst, krb5_principal=None, raiseOnError=False):
       if src in self.paths:
          return 

       # make destination dir
       self._mkdirp(dst, krb5_principal, raiseOnError)

       cli_job = self.createHadoopCliJob(dst, krb5_principal)
       cli_job.moveFromLocal(src, dst)
       self.jobwaiter.addJob(cli_job, False)
       self.paths[src] = dst

   def wait(self, raiseOnError=False):
       if len(self.paths) == 0:
          return
       self.jobwaiter.wait(0, raiseOnError)
       self.paths = {}

   def getErrors(self):
       return self.jobwaiter.errors

   def hasJobs(self):
       return len(self.paths) > 0 
  
   def getJobCount(self):
       return self.jobwaiter.jobs_total

   def getTotalTime(self):
       return self.jobwaiter.total_time
 
   def getWallTime(self):
       if self.jobwaiter.jobs_total > 0 :
          return time.time() - self.start
       return 0


def hdfsPathJoin(*args):
    if len(args) == 0:
       return None
    if len(args) == 1: 
       return args[0]
    
    result = args[0]
    prefix = ''
    if result.startswith('file://'):
        prefix = 'file://'
        result = result[7:]
    for a in args[1:]:
        if not result.endswith('/') and not a.startswith('/'):
            result += '/'
        if result.endswith('/') and a.startswith('/'):
            result = result[:-1]
        result += a

    return prefix+result


#TODO: this is just a test, must remove
if __name__ == '__main__':
   #path = 'file:///home/ledion/p4/coreapps/HdfsExport/current/package/'
   #path = 'file:///opt/mapr-nfs/'  
   #path = 'file:///opt/mapr-nfs/sveserv51-mapr.sv.splunk.com/user/redx/allan/'
   #path = 'file:///opt/mapr-nfs/sveserv51-mapr.sv.splunk.com/user/redx/allan/50/raw/ddf9e9fa4d6f22e416940ded28cbf1da.*.cursor*'
   #path = 'file:///opt/mapr-nfs/sveserv51-mapr.sv.splunk.com/user/redx/allan/50/raw/ddf9e9fa4d6f22e416940ded28cbf1da.1351915255.cursor'
   #path = 'file:///tmp/'
   #hj = HadoopEnvManager.getCliJob(path) 
   #hj.lsr(path)  
   #rv = hj.wait()
   #print 'rv:'+str(rv)
   #for line in hj.getOutput():
   #    print line
   path = 'file:///Users/hyan/export/50/internal/raw/20121207/00/hyan-mbp.local/b3bcabeb747afc2a77ee1ba11a47075f_1354867200_1354896000_19_0.raw.gz'
   path = 'file:///Users/hyan/code/splunk-p4/coreapps/HdfsExport/current/package/bin/a.zip'
   hj = HadoopEnvManager.getCliJob(path) 
   hj.text(path)
   for line in hj.getStdout():
       print (line)
   hj.wait(True) 
   
   #lister = HDFSDirLister()
   #for line in lister.lsr(path):
   #   print line
   
    









