import os
import splunk.admin as admin
from clusters import *
from delegating_handler import DelegatingRestHandler
import constants
from errors import *
import util

required_args = ['uri']  # required on create
optional_args = ['namenode_http_port', 'hadoop_home', 'java_home', 'kerberos_principal', 'kerberos_service_principal', 'ha', 'hdfs_site', 'auth_to_local']


ENDPOINT = 'configs/conf-clusters'

class ClustersHandler(DelegatingRestHandler):
    
    def setup(self):
        self.appName = constants.APP_NAME
        self.userName = 'nobody'
        
        if self.requestedAction == admin.ACTION_LIST:
            self.supportedArgs.addOptArg('add_versions')
        elif self.requestedAction == admin.ACTION_EDIT or self.requestedAction == admin.ACTION_CREATE:
            req_args = required_args
            opt_args = optional_args
            
            for arg in opt_args:
                self.supportedArgs.addOptArg(arg)
            for arg in req_args:
                if self.requestedAction == admin.ACTION_EDIT:
                    self.supportedArgs.addOptArg(arg)
                else:
                    self.supportedArgs.addReqArg(arg)  

    def _isLocalURI(self, uri):
        return uri.startswith('file://') 

    def handleList(self, confInfo):
        self.delegate(ENDPOINT, confInfo, method='GET')
        # add some runtime information to the items
        for name, obj in confInfo.items():
            if self._isLocalURI(obj.get('uri', '')):
                continue
            cluster = Cluster(name, obj)
            obj['hdfs_site'] = cluster.getHdfsSite()
            obj['cluster_dir'] = cluster.getClusterDir()
            obj['cluster_cli'] = cluster.hadoop_cli
            obj['uri'] = cluster.getURI().rstrip('/')
            obj['authentication_mode'] = 'simple' if obj.get('kerberos_principal', '') == '' else 'kerberos'
            obj['authorization_mode']  = '0'      if obj.get('kerberos_principal', '') == '' else '1'
            if self.callerArgs.get('add_versions', [''])[0] == '1': 
               local = 'unknown'
               remote = 'unknown'
               try:
                   local = cluster.getLocalHadoopVersion() 
               except: pass
               try: 
                   remote = cluster.getRemoteHadoopVersion() 
               except: pass 
               obj['local_hadoop_version'] = local
               obj['remote_hadoop_version'] = remote
        
    def handleCreate(self, confInfo):
        if self.getEntities('admin/conf-clusters', 'name='+self.callerArgs.id):
            raise HcException(HCERR2007, {'stanza':str(self.callerArgs.id)})

        conf = self.getEntity('admin/conf-clusters', '_new')

        if self._isLocalURI(self.callerArgs.get('uri', [''])[0]):
            return self.handleLocalCreateOrEdit(confInfo, conf)
        
        cluster = self.validateHdfsClusterArgs(conf)
        self.callerArgs.id = cluster.name
        try:
            self.handleHdfsCreateOrEdit(cluster, confInfo)
        except Exception as e:
            logger.exception("Failed to handleCreate:")
            # remove created dir and xml file
            cluster.remove()
            # delete new stanza if exists
            if self.getEntities('admin/conf-clusters', self.callerArgs.id):
                self.deleteEntity('admin/conf-clusters', self.callerArgs.id)
            raise e

            
    def handleEdit(self, confInfo):
        conf = self.getEntity('admin/conf-clusters', self.callerArgs.id)

        uri = util.getProperty(self.callerArgs, 'uri', conf)
        if self._isLocalURI(uri):
            return self.handleLocalCreateOrEdit(confInfo, conf)
        
        cluster = self.validateHdfsClusterArgs(conf)
        hdfs_site_old = cluster.getHdfsSite() 
        try:
            self.handleHdfsCreateOrEdit(cluster, confInfo)
        except Exception as e:
            logger.exception("Failed to handleEdit")
            # rollback to previous hdfs-site.xml file if exists
            if not hdfs_site_old:
                cluster.props['hdfs_site'] = hdfs_site_old
                
            # rollback to previous core-site.xml file
            cluster.props['kerberos_service_principal'] = conf['kerberos_service_principal'] if 'kerberos_service_principal' in conf else None
            cluster.props['kerberos_principal']         = conf['kerberos_principal']         if 'kerberos_principal'         in conf else None
            if cluster.props['kerberos_service_principal'] != None and cluster.props['kerberos_service_principal'].strip() != '':
                cluster.props['authentication_mode'] = 'kerberos'
                cluster.props['authorization_mode'] = '1'
            else:
                cluster.props['authentication_mode'] = 'simple'
                cluster.props['authorization_mode'] = '0'
            cluster.saveXml()
            
            # rollback to previous stanza
            for k,v in self.callerArgs.items():
                self.callerArgs[k] = conf[k]
            self.delegate(ENDPOINT, confInfo)
            
            raise e
   
    def _ensureRequiredCreateArgs(self, args):
        if self.requestedAction == admin.ACTION_CREATE:
           for arg in args:
               if arg not in self.callerArgs or self.callerArgs.get(arg) == None or len(self.callerArgs.get(arg)) == 0 or self.callerArgs.get(arg)[0].strip() == '':
                   raise HcException(HCERR0501, {'argument': arg})


    def validateLocalClusterArgs(self, conf):
        import util
        uri = util.getProperty(self.callerArgs, 'uri', conf)
        if not self._isLocalURI(uri):
            raise HcException(HCERR0503, {'name':'uri', 'value':uri, 'accepted_values':'file://<path>'})
        import os.path
        if not os.path.isdir(uri[7:]):
            raise HcException(HCERR0502, {'name':'uri', 'value':uri, 'error':'path does not exist'})
        import splunk.entity as en
        clusters = en.getEntities('admin/conf-clusters', search='uri=file://*', namespace=self.appName, owner=self.userName, sessionKey=self.getSessionKey());
        for name, obj in clusters.items():
            if name != self.callerArgs.id and obj['uri'].rstrip('/') == uri.rstrip('/'):
                raise HcException(HCERR1515, {'path':uri, 'cluster':name})

    def validateHdfsClusterArgs(self, conf):
        ha_nameservice = None
        namenode_http_port = None
        active_namenode = None
        hdfs_site = None
        ha = util.getProperty(self.callerArgs, 'ha', conf, '')
        import splunk.util
        if len(ha) > 0 and splunk.util.normalizeBoolean(ha):
            if self.requestedAction == admin.ACTION_EDIT and 'hdfs_site' not in self.callerArgs:
                cluster = Cluster(self.callerArgs.id, None)
                self.callerArgs['hdfs_site'] = cluster.getHdfsSite()

            if 'hdfs_site' not in self.callerArgs or not self.callerArgs['hdfs_site'][0] or self.callerArgs['hdfs_site'][0].strip()=='':
                raise HcException(HCERR0501, {'argument': 'hdfs_site'})
    
            hdfs_site = util.getProperty(self.callerArgs, 'hdfs_site', conf)
            if not hdfs_site.strip().startswith('<configuration>'):
                hdfs_site = '<configuration>'+hdfs_site.strip()
            if not hdfs_site.strip().endswith('</configuration>'): 
                hdfs_site = hdfs_site.strip()+'</configuration>'
            
            try:
                ha_nameservice, active_namenode, namenode_http_port = parseHdfsSiteXml(hdfs_site)
            except HcException:
                logger.exception('Failed to parse hdfs_site')
                raise 
            except Exception:
                logger.exception('Failed to parse hdfs_site')
                raise HcException(HCERR2009, {'error':'please make sure xml is normalized'})

            if ha_nameservice != self.callerArgs.id:
                raise HcException(HCERR0502, {'name': 'id', 'value': self.callerArgs.id, 'error': 'clusters stanza name must be same as HA nameservice id'})
        else:        
            self._ensureRequiredCreateArgs(['namenode_http_port', 'hadoop_home', 'java_home'])
            namenode_http_port = int(util.getProperty(self.callerArgs, 'namenode_http_port', conf))

        hadoop_home = util.getProperty(self.callerArgs, 'hadoop_home', conf)
        java_home = util.getProperty(self.callerArgs, 'java_home', conf)
        
        authentication_mode = 'simple'
        authorization_mode  = '0'
        
        principal = util.getProperty(self.callerArgs, 'kerberos_principal', conf)
        kerberos_service_principal = util.getProperty(self.callerArgs, 'kerberos_service_principal', conf)
        if kerberos_service_principal != None and kerberos_service_principal.strip() != '':
            authentication_mode = 'kerberos'
            authorization_mode  = '1'
        
        auth_to_local = util.getProperty(self.callerArgs, 'auth_to_local', conf, '')
        
        props = {'namenode_http_port':namenode_http_port, 
                 'hadoop_home': hadoop_home, 
                 'java_home': java_home, 
                 'authentication_mode':authentication_mode, 
                 'authorization_mode':authorization_mode, 
                 'principal':principal, 
                 'kerberos_service_principal':kerberos_service_principal}
        if active_namenode != None:
            props['active_namenode'] = active_namenode
        if hdfs_site != None:
            props['hdfs_site'] = hdfs_site
        if auth_to_local != '':
            props['auth_to_local'] = auth_to_local
    
        cluster = Cluster(self.callerArgs.id, props)
        return cluster
        
    def handleLocalCreateOrEdit(self, confInfo, conf):
        self.validateLocalClusterArgs(conf)
        self.delegate(ENDPOINT, confInfo)
         
    def handleHdfsCreateOrEdit(self, cluster, confInfo):
        # 1) create local/clusters/<host_port> directory if not exists 2) verify hadoop version 3) create/update core-site.xml
        cluster.save()

        # remove fields we don't want to save in the conf file
        fields = ['name', 'uri', 'namenode_http_port', 'kerberos_principal', 'kerberos_service_principal', 'hadoop_home', 'java_home', 'ha', 'auth_to_local']
        for k in self.callerArgs.keys():
            if not k in fields:
               del self.callerArgs[k]
         
        # create/edit conf stanza
        self.delegate(ENDPOINT, confInfo)
        
        principal = cluster.props['principal'] if  cluster.props['authentication_mode'] == 'kerberos' else None
        import hadooputils as hu
        # verify kerberos_principal, keytab and kerberos_service_principal and ls works
        hu.validateConnectionToHadoop(self.getSessionKey(), principal, 'hdfs://'+self.callerArgs.id+'/')
    
    def handleRemove(self, confInfo):
        # delegate remove to /servicesNS/<user>/<app>/admin/conf-clusters
        conf = self.getEntity('admin/conf-clusters', self.callerArgs.id)
        self.delegate(ENDPOINT, confInfo, method='DELETE')
        # fix for SPL-61583
        uri = None
        if 'uri' in conf:
            uri = conf.get('uri')
        if(uri == None or uri.strip() == ''):
            uri = 'hdfs://'+self.callerArgs.id;
        if not self._isLocalURI(uri):  #remote cluster
           cluster = Cluster(self.callerArgs.id)
           cluster.remove()
 
    def handleCustom(self, confInfo):
        method = 'GET' if self.requestedAction == admin.ACTION_LIST else 'POST'
        self.delegate(ENDPOINT, confInfo, method=method, customAction=self.customAction)


admin.init(ClustersHandler, admin.CONTEXT_APP_ONLY)
 
