import splunk
import splunk.admin as admin
from delegating_handler import DelegatingRestHandler
from errors import *

required_args = ['uri', 'search']
optional_args = ['base_path', 'roll_size', 'minspan', 'maxspan', 'starttime', 'endtime', 'format', 'fields', 'partition_fields', 'kerberos_principal', 'parallel_searches', 'cron_schedule', 'compress_level'] 

ENDPOINT = 'configs/conf-export'

class HDFSExportHandler(DelegatingRestHandler):
    '''HDFSExportHandler is a MConfigHandler to manage jobs that will export data from a search command to Hadoop file system'''

    def setup(self):
        '''assign required and optional parameters for create and update'''
        if self.requestedAction in [admin.ACTION_CREATE, admin.ACTION_EDIT] and self.customAction == '':
            for arg in required_args:
                if self.requestedAction == admin.ACTION_CREATE:
                   self.supportedArgs.addReqArg(arg)
                else:
                   self.supportedArgs.addOptArg(arg)

            for arg in optional_args:
                self.supportedArgs.addOptArg(arg)

    def handleNew(self, confInfo):
        confnew = self.getEntity('admin/conf-export', '_new')
        confItem = confInfo[self.callerArgs.id]
        for key, val in confnew.items():
            confItem[key] = str(val) if val else ''
    
    def handleList(self, confInfo):
        '''Provide a list of export jobs with needed information about the search'''
        # hack to support unicode in our objects 
        admin.str = unicode
        
        # hack to get around external REST handlers not supporting _new
        # and conf-<file> handlers not exposing default stanza
        if self.callerArgs.id == '_new_ext' or self.callerArgs.id == "default":
            self.handleNew(confInfo)
            return
        
        # Read custom conf file - export.conf
        searchItems = self.getEntities(entityPath='saved/searches', search="name=ExportSearch:*", count=0)
        exportItems = self.getEntities(entityPath='admin/conf-export', count=0)
        for name, obj in exportItems.items():
            ssName = 'ExportSearch:%s' % name
            if not ssName in searchItems:
                continue

            confItem = confInfo[name]
            try:
                confItem.update(searchItems[ssName])
                del confItem['eai:acl']
            except KeyError:
                pass
            for key, val in obj.items():
                # for all required and optional args, skip those with None or empty string value
                if (key in required_args or key in optional_args) and (val == None or str(val).strip() == ''):
                    continue
                confItem[key] = str(val)
            acl = {}
            for k, v in obj[admin.EAI_ENTRY_ACL].items():
                if None != v:
                    acl[k] = v
            confItem.setMetadata(admin.EAI_ENTRY_ACL, acl)

    def handleCreate(self, confInfo):
        '''Create a new HDFS export job.  Save the export.conf information and a saved search'''
        name = self.callerArgs.id
        if name == "default":
            del self.callerArgs['search']
            self.handleEdit(confInfo)
            return
        
        if self.getEntities(ENDPOINT, 'name='+str(name)):
            raise HcException(HCERR2007, {'stanza':str(name)})

        self.validateArgs(admin.ACTION_CREATE)
        # validate kerberos principal before creating anything
        self.validatePrincipal()

        cron_schedule     = self.callerArgs.get('cron_schedule', ['0 * * * *'])[0]
        if 'cron_schedule' in self.callerArgs:
            del self.callerArgs['cron_schedule']
        
        # workaround for SPL-57177
        if ('base_path' in self.callerArgs):
            base_path = self.callerArgs['base_path'][0]
            if (base_path != '/'):
                self.callerArgs['base_path'] = [base_path.rstrip('/')]

        #save the export.conf stanza
        self.saveConf('export', name, self.callerArgs)

        #we also save a scheduled saved search to do the ongoing export
        params = {}
        params["name"]          = 'ExportSearch:%s' % name
        params['search']        = '| runexport name="%s"' %  name
        try:
            self.saveSearch(name, params, cron_schedule)
        except:
            # rollback
            # delete new stanza if exists
            if self.getEntities('admin/conf-export', name):
                self.deleteEntity('admin/conf-export', name)
            raise 
        
    def saveSearch(self, name, params, cron_schedule):
        from splunk.models.saved_search import SavedSearch
        saved_search = SavedSearch(self.appName, self.userName, sessionKey=self.getSessionKey(), **params)
        saved_search.schedule.is_scheduled = True
        saved_search.schedule.cron_schedule = cron_schedule
        # hack around saved search model not supporting these fields
        saved_search.entity.properties['description']   = 'Scheduled search for export job: %s' % name
        saved_search.entity.properties['is_visible']    = '0'
        self.persistSavedSearch(saved_search)

    def updateSearch(self, name, cron_schedule):
        saved_search = self.getSavedSearch(name)
        saved_search.schedule.cron_schedule = cron_schedule
        # Change the owner to nobody, so that REST call will be made to /servicesNS/nobody/HadoopConnect/...
        # instead of /servicesNS/admin/HadoopConnect/... or /servicesNS/<owner>/HadoopConnect/...
	if saved_search.entity and saved_search.entity.owner:
	    saved_search.entity.owner = 'nobody'
        self.persistSavedSearch(saved_search)

    def persistSavedSearch(self, saved_search):
        try:
            if not saved_search.save():
                raise HcException(HCERR1506, {'export':name, 'error':''})
        except Exception as e:
            logger.exception('Failed to save search object')
            if isinstance(e, splunk.RESTException):
                msg = e.get_extended_message_text()
            else:
                msg = str(e)
            raise HcException(HCERR1506, {'export':name, 'error':msg})

    def parseSearch(self, query):
        import splunk.search.Parser as Parser
        parsedSearch = Parser.parseSearch(str(query), sessionKey=self.getSessionKey())
        searchProps = parsedSearch.properties.properties
        if len(searchProps.get("reportsSearch", "")) > 0:
           raise Exception("Cannot export a reporting search")
        
    def handleEdit(self, confInfo):
        '''Edit the export.conf entry'''
        # handle "default" entity via properties endpoint because conf-<file> does not list default stanza
        if self.callerArgs.id == "default":
            from splunk.bundle import getConf
            try:
                myConf = getConf('export', sessionKey=self.getSessionKey(), namespace=self.appName, owner='nobody' )
                myConf.beginBatch()
                for k,v in self.callerArgs.data.iteritems():
                    if type(v) is list:
                        if len(v) == 0:
                            v = ''
                        elif v[0] != None:
                            v = v[0].strip()
                        myConf['default'][k] = v
                myConf.commitBatch()
            except Exception as e:
                logger.exception('Failed to update conf object')
                if isinstance(e, splunk.RESTException):
                    msg = e.get_extended_message_text()
                elif isinstance(e, HcException):
                    raise e
                else:  
                    msg = str(e)
                raise HcException(HCERR2003, {'conf':'export', 'stanza':str(self.callerArgs.id), 'error':msg})   
        else:
            self.validateArgs(admin.ACTION_EDIT)
            self.validatePrincipal()
            if 'cron_schedule' in self.callerArgs:
                self.updateSearch('ExportSearch:%s' % self.callerArgs.id, self.callerArgs.get('cron_schedule',[None])[0])
                del self.callerArgs['cron_schedule']
            self.saveConf('export', self.callerArgs.id, self.callerArgs)

    def handleRemove(self, confInfo):
        '''Delete the export.conf job and the associated saved search'''
        name = self.callerArgs.id
        searchName = 'ExportSearch:%s' % name
        
        self.deleteEntity('saved/searches', searchName)
        self.deleteEntity(ENDPOINT, name)
        
    def getSavedSearch(self, name):
       import splunk.models.saved_search as sm_saved_search
       ent = self.getEntity('saved/searches', name)
       ss = sm_saved_search.SavedSearch.manager()._from_entity(ent)
       ss.sessionKey = self.getSessionKey()
       return ss

    def handleCustom(self, confInfo):
        if not self.requestedAction == admin.ACTION_EDIT:
            raise HcException(HCERR2005, {'custom_action':self.customAction, 'eai_action':self.requestedAction, 'error':'Only edit action is allowed'}) 
           
        searchName = 'ExportSearch:' + self.callerArgs.id
        ss = self.getSavedSearch(searchName)

        if self.customAction == "pause":
           ss.schedule.is_scheduled = False
           ss.save()
        elif self.customAction == "resume":
           ss.schedule.is_scheduled = True
           ss.save()
           self.saveConf('export', self.callerArgs.id, {'status': 'done'})
        elif self.customAction == "force":
           url = ss.entity.getLink('reschedule')
           if url == None: 
              raise HcException(HCERR2005, {'custom_action':self.customAction, 'eai_action':self.requestedAction, 'error':'Could not find a reschedule entity link on the saved search object.'})
           self.simpleRequest(url, 'POST')
        else:
           raise HcException(HCERR2005, {'custom_action':self.customAction, 'eai_action':self.requestedAction, 'error':'Unknown action'})  
    
    def validateArgs(self, action):
        if action == admin.ACTION_CREATE:
            entityName = '_new'
        else:
            entityName = self.callerArgs.id
        conf = self.getEntity('admin/conf-export', entityName)

        import util
        fields = util.getProperty(self.callerArgs, 'fields', conf)
        for field in fields.split(','):
            if field == None or field.strip() == '':
                raise HcException(HCERR0504, {'name':'fields'})

        format = util.getProperty(self.callerArgs, 'format', conf)
        # make sure format is supported
        accepted_formats = ['raw', 'json', 'xml', 'csv', 'tsv']
        if format not in accepted_formats:
            raise HcException(HCERR0503, {'name':'format', 'value':format, 'accepted_values':','.join(accepted_formats)})
        if format in ['csv','tsv']:
            for field in fields.split(','):
                if field.find('*') >= 0:
                    raise HcException(HCERR0502, {'name':'fields', 'value':fields, 'error':"Wildcard is not allowed for format '%s'" % format})
        elif format == 'raw':
            fieldList = fields.split(',')
            if '_raw' not in fieldList or len(fieldList) > 1:
                raise HcException(HCERR0502, {'name':'fields', 'value':fields, 'error':"Only field '_raw' is allowed to be exported for raw output format"})
            
        search = util.getProperty(self.callerArgs, 'search', conf)
        # validate search
        if search == None or search == '': 
            raise HcException(HCERR0504, {'name':'search'}) 

        parallel_searches = util.getProperty(self.callerArgs, 'parallel_searches', conf, '1')
        # validate parallel searches
        if parallel_searches == 'max':
            valid = True
        else:
            try:
                valid = (int(parallel_searches) > 0)
            except:
                valid = False
        if not valid:
            raise HcException(HCERR0502, {'name':'parallel_searches', 'value':parallel_searches, 'error':"Must be a positive number or word 'max'"})
        
        # see if the search can be parsed....if it can't an exception will be shown the user.
        if not search[0] == '|' and not search.startswith('search'):
            search = 'search ' + search
        try:
            self.parseSearch(search)
        except Exception as e:
            raise HcException(HCERR0502, {'name':'search', 'value':search, 'error':str(e)})
        
        #check to see if the hdfs path supplied is valid
        uri = util.getProperty(self.callerArgs, 'uri', conf, '')
        bp = util.getProperty(self.callerArgs, 'base_path', conf, '')
        if not (uri.startswith('hdfs://') or uri.startswith('file://')):  
            raise HcException(HCERR0502, {'name':'uri', 'value':uri, 'error':'accepted shcemes: hdfs:// or file://'})
        if len(bp) == 0:
            raise HcException(HCERR0504, {'name':'base_path'})
        
        # validate compress_level
        compress_level = util.getProperty(self.callerArgs, 'compress_level', conf, '2')
        try:
            compress_level = int(compress_level)
            if compress_level < 0 or compress_level > 9:
                raise
        except:    
            raise HcException(HCERR0502, {'name':'compress_level', 'value':compress_level, 'error':'accepted values: an integer between 0 to 9 and 0 means no compression'})

    def validatePrincipal(self):
        principal = None
        if 'kerberos_principal' not in self.callerArgs:
            return
        else:
            principal = self.callerArgs['kerberos_principal']
        if principal != None and type(principal) is list:
            principal = principal[0]    
        if principal != None and len(principal.strip()) > 0 and principal != 'None':
            import hadooputils as hu
            return hu.validatePrincipalAndKeytab(principal.strip())
        raise HcException(HCERR0502, {'name':'kerberos_principal', 'value':str(principal), 'error':'Make sure principal is not None or empty string and is a valid value.'})
 
if __name__ ==  '__main__':
   admin.init(HDFSExportHandler, admin.CONTEXT_APP_AND_USER)
