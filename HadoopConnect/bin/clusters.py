import os, os.path
import hadooputils as hu
from util import *
from constants import *

class Cluster:
    def __init__(self, name, props=None):
        self.name = name
        self.props = props
        self.hadoop_cli = None
        if name.startswith('hdfs://'):
            self.name = name[len('hdfs://'):]
        if not self.getURI().startswith('file://'):
            if props and 'active_namenode' in props:
                self.host = props['active_namenode'] 
            elif self.name.find(':') > 0:    
                try:
                    self.host, self.namenode_ipc_port = self.name.split(':')
                    self.namenode_ipc_port = int(self.namenode_ipc_port)
                except:
                    raise HcException(HCERR2008, {'stanza':str(self.name), 'valid_format':'<host>:<port>'})
        if props:
            import splunk.util
            self.props['authorization_mode'] = 'true' if 'authorization_mode' in props and splunk.util.normalizeBoolean(props['authorization_mode']) else 'false'
            self.hadoop_cli = os.path.join(props['hadoop_home'], 'bin', 'hadoop') if 'hadoop_home' in props else None
   
    def getURI(self):
        if self.props and 'uri' in self.props and self.props.get('uri') != None and self.props.get('uri').strip() != '':
            return self.props.get('uri')
        return 'hdfs://' + self.name
 
    def validateHadoopVersion(self):
        if self.hadoop_cli == None or not os.path.exists(self.hadoop_cli):
            raise HcException(HCERR0012, {'path':str(self.hadoop_cli)})
        
        # get local hadoop version
        local_version = self.getLocalHadoopVersion()
        remote_version = self.getRemoteHadoopVersion()
        # make sure local version is same as remote version
        if remote_version.find(local_version) < 0:
            raise HcException(HCERR1517, {'local_version':local_version, 'remote_version':remote_version})
    
    def getHadoopInfo(self):
        import hadooputils as hu
        return hu.getHadoopClusterInfo(self.host, int(self.props['namenode_http_port']))
        
    def getRemoteHadoopVersion(self):
        # get remote hadoop version
        info = self.getHadoopInfo()
        if not info or 'Version' not in info:
            raise HcException(HCERR0014, {'host':self.host, 'port':self.props['namenode_http_port'], 'error':"\'Version\' keyword is not found"})
        remote_version = info['Version'].strip()
        return remote_version
    
    def openProcess(self, args, env):
        import subprocess
        return subprocess.Popen(args, shell=False, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    
    def validateHadoopHome(self, env):
        hu.validateHadoopHome(env) 
       
    def getLocalHadoopVersion(self):
        env = dict(os.environ)

        if self.props==None or 'hadoop_home' not in self.props:
            raise HcException(HCERR1511, {'field':'hadoop_home'})
        env['HADOOP_HOME'] = self.props['hadoop_home'] 
        # ERP-1104 newer hadoop 2 need these env variables
        env['HADOOP_PREFIX'] = env['HADOOP_HOME'] 
        env['HADOOP_COMMON_HOME'] = env['HADOOP_PREFIX'] 
        env['HADOOP_HDFS_HOME'] = env['HADOOP_PREFIX'] 
        
        if self.props==None or 'java_home' not in self.props:
            raise HcException(HCERR1511, {'field':'java_home'})
        env['JAVA_HOME'] = self.props['java_home']

        self.validateHadoopHome(env);

        args = [self.hadoop_cli, 'version']
        try:
            process = self.openProcess(args, env)
            output = process.communicate()
        except Exception as e:
            logger.exception('Failed to run process')
            raise HcException(HCERR0013, {'error':str(e)})
        if process.returncode != 0:
            raise HcException(HCERR0013, {'error':output[1]})
        local_version = output[0].split('\n')[0]
        local_version = local_version[len('Hadoop'):].strip()
        return local_version
            
    def save(self):
        # we will always do a ls in the end of the configuration process and throw up if version is not matched.
        #self.validateHadoopVersion()
        try:
            dir = self.getClusterDir()
            if not os.path.isdir(dir):
                os.makedirs(dir)
            self.saveXml()
        except Exception as e:
            logger.exception('Failed to create cluster xml configurations')
            raise HcException(HCERR1518, {'name':self.name, 'error':str(e)})    
        
    def remove(self):
        try:
            dir = self.getClusterDir()
            if os.path.isdir(dir):
                import shutil
                shutil.rmtree(dir)
        except Exception as e:
            raise HcException(HCERR1516, {'name':self.name, 'error':str(e)})
        
    def getClusterDir(self):
        # Use a relative path so that it works in SH pooling configuration as well
        return os.path.join('..', 'local', 'clusters', makeFileSystemSafe(self.name))
    
    def getHdfsSite(self):
        xml = os.path.join(self.getClusterDir(), 'hdfs-site.xml')
        if not os.path.exists(xml):
            return None

        with open(xml) as f:
            return f.read()
   
    def buildPropertyElement(self, configElement, name, value):
        from xml.etree.ElementTree import SubElement
        propertyElement = SubElement(configElement, 'property')
        nameElement = SubElement(propertyElement, 'name')
        nameElement.text = name
        valueElement = SubElement(propertyElement, 'value')
        valueElement.text = value
        
    def saveXml(self):
        from xml.etree import ElementTree
        from xml.etree.ElementTree import Element
        import splunk.util
        config = Element('configuration')
        # some version of hadoop needs to setup fs.default.name on client side. Otherwise, text command will not pick up the hdfs scheme in uri.
        if self.getURI().startswith('hdfs'):
            self.buildPropertyElement(config, 'fs.default.name', self.getURI())
        self.buildPropertyElement(config, 'hadoop.security.authentication', self.props['authentication_mode'])
        self.buildPropertyElement(config, 'hadoop.security.authorization', 'true' if splunk.util.normalizeBoolean(self.props['authorization_mode']) else 'false')
        if self.props['authentication_mode'] == 'kerberos':
            self.buildPropertyElement(config, 'dfs.namenode.kerberos.principal', self.props['kerberos_service_principal'])
        if 'auth_to_local' in self.props:
            self.buildPropertyElement(config, 'hadoop.security.auth_to_local', self.props['auth_to_local'])

        # create core-site.xml
        xmlfile = os.path.join(self.getClusterDir(), 'core-site.xml')
        tmpfile = xmlfile + '.tmp'
        xml = ElementTree.tostring(config, 'utf-8')
        with open(tmpfile, 'w') as f:
            f.write(xml)
        os.rename(tmpfile, xmlfile)
        
        # create hdfs-site.xml if needed
        if 'hdfs_site' in self.props and self.props['hdfs_site'] != None and len(self.props['hdfs_site'].strip()) > 0:
            xmlfile = os.path.join(self.getClusterDir(), 'hdfs-site.xml')
            tmpfile = xmlfile+'.tmp'
            with open(tmpfile, 'w') as f:
                f.write(self.props['hdfs_site'])
            os.rename(tmpfile, xmlfile)

def parseHdfsSiteXml(hdfs_site):
    ha_nameservice = None
    namenode_http_port = None
    active_namenode = None
    from xml.etree import ElementTree
    root = ElementTree.fromstring(hdfs_site)
    p = {}
    try:
        for pr in root.iter('property'):
            p[pr.findtext('name').strip()] = pr.findtext('value').strip()
    except:
        logger.exception('Failed to parse hdfs-site.xml')
        raise HcException(HCERR2009, {'error', 'Please make sure xml is normalized.'})

    try:
        nameservices = p['dfs.nameservices'].split(',')
    except:
        logger.error('Failed to parse hdfs_site xml value: '+str(hdfs_site))
        raise HcException(HCERR0502, {'name': 'hdfs_site', 'value': 'see HadoopConnect.log', 'error': 'dfs.nameservices property is required'})
        #raise HcException(HCERR2009, {'error': 'dfs.nameservices property is required'})
        
    # find HA nameservice info and list of HA namenode ids
    nns = []
    for nameservice in nameservices:
        ha_nameservice = nameservice
        pname = 'dfs.ha.namenodes.'+ha_nameservice.strip()
        nnids = p[pname] if pname in p else None 
        if nnids != None:
            nns = nnids.split(',')
            break
    if len(nns) == 0:
        logger.error('Failed to parse hdfs_site xml value: '+str(hdfs_site))
        raise HcException(HCERR0502, {'name': 'hdfs_site', 'value': 'see HadoopConnect.log', 'error': pname+' property is required'})
        #raise HcException(HCERR2009, {'error': pname+' property is required'})
    
    # find active namenode info
    import hadooputils as hu
    for nn in nns:
        nn = nn.strip()
        pname = 'dfs.namenode.http-address.'+ha_nameservice+'.'+nn
        httpAddress = p[pname] if pname in p else None
        if httpAddress == None:
            raise HcException(HCERR0502, {'name': 'hdfs_site', 'value': 'see HadoopConnect.log', 'error': pname+' property is required'})
            #raise HcException(HCERR2009, {'error': pname+' property is required'})
        nnHost, nnHttpPort = httpAddress.split(":")
        info = hu.getHadoopHAClusterInfo(nnHost, int(nnHttpPort))
        hastate_not_found = False
        if not info or info and 'tag.hastate' not in info:
            hastate_not_found = True
            logger.warn("Unable to parse hadoop server jmx/jsp information to get active HA server, will use this one as active HA server!") 
            #raise HcException(HCERR0017, {'host': nnHost, 'port': nnHttpPort, 'error': 'Cannot find ha state for the namenode'})
        if hastate_not_found or info['tag.hastate'].lower() == 'active':
            active_namenode = nnHost
            namenode_http_port = int(nnHttpPort)
            break
    
    # verify 'dfs.namenode.rpc-address.<nameservice>.<namenode id>' exists
    for nn in nns:
        nn = nn.strip()   
        pname = 'dfs.namenode.rpc-address.'+ha_nameservice+'.'+nn
        if pname not in p:
            raise HcException(HCERR0502, {'name': 'hdfs_site', 'value': 'see HadoopConnect.log', 'error': pname+' property is required'})
    
    # verify 'dfs.client.failover.proxy.provider.<nameservice>' exists
    pname = 'dfs.client.failover.proxy.provider.'+ha_nameservice
    if pname not in p:
        raise HcException(HCERR0502, {'name': 'hdfs_site', 'value': 'see HadoopConnect.log', 'error': pname+' property is required'})
    
    return ha_nameservice, active_namenode, namenode_http_port

