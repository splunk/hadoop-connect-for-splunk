import os, os.path
import splunk.admin as admin
import splunk
import splunk.entity as en
import splunk.rest as rest
from errors import *

class DelegatingRestHandler(admin.MConfigHandler):
 
    def getEntity(self, entityPath, entityName, uri=None):
        try:
            return en.getEntity(entityPath, entityName, uri=uri, namespace=self.appName, owner=self.userName, sessionKey=self.getSessionKey())
        except Exception as e:
            logger.exception('Failed to get entity object')
            if isinstance(e, splunk.RESTException):
                msg = e.get_extended_message_text()
            else:  
                msg = str(e)
            raise HcException(HCERR2001, {'entity_path':'' if entityPath == None else entityPath, 'entity_name':'' if entityName == None else entityName, 'uri':'' if uri == None else uri,'error': msg})

        
    def getEntities(self, entityPath=None, search=None, uri=None, count=None, namespace=None, owner=None):
        try:
            namespace = self.appName if namespace == None else namespace
            owner = self.userName if owner == None else owner
	        logger.error("Entity Path: {}".format(entityPath))
        return en.getEntities(entityPath, namespace=namespace, owner=owner, search=search, uri=uri, sessionKey = self.getSessionKey(), count=count)
        except Exception as e:
            logger.exception('Failed to get entities object')
            if isinstance(e, splunk.RESTException):
                msg = e.get_extended_message_text()
            else:  
                msg = str(e)
            raise HcException(HCERR2002, {'entity_path':'' if entityPath == None else entityPath, 'search':'' if search == None else search, 'uri':'' if uri == None else uri, 'error':msg})
        
    def deleteEntity(self, entityPath, entityName):
        try:
            en.deleteEntity(entityPath, entityName,
                            namespace=self.appName,
                            owner=self.userName,
                            sessionKey = self.getSessionKey())
        except Exception as e:
            logger.exception('Failed to delete entity object')
            if isinstance(e, splunk.RESTException):
                msg = e.get_extended_message_text()
            else:  
                msg = str(e)
            raise HcException(HCERR2004, {'entity_path':'' if entityPath == None else entityPath, 'entity_name':'' if entityName == None else entityName, 'error':msg})
   

    def writeConfCtx(self, confName, stanzaName, settingsDict):
   	"""
	This version of writeConf uses /*/conf-<name> endpoint instead of /properties endpoint
	"""
	app  = self.context != admin.CONTEXT_NONE         and self.appName  or "search"
    	user = self.context == admin.CONTEXT_APP_AND_USER and self.userName or "nobody"
	try:
    	    path = "/servicesNS/%s/%s/admin/conf-%s/%s" % (user, app, confName, stanzaName)
	    rest.simpleRequest(path, sessionKey=self.getSessionKey(), postargs=settingsDict, raiseAllErrors=True)
	except splunk.ResourceNotFound:
	    path = "/servicesNS/%s/%s/admin/conf-%s" % (user, app, confName)
	    postargs=settingsDict
	    postargs['name']=stanzaName
	    rest.simpleRequest(path, sessionKey=self.getSessionKey(), postargs=postargs, raiseAllErrors=True)

    def saveConf(self, confName, stanzaName, settingsDict):
        try:
            self.writeConfCtx(confName, stanzaName, settingsDict)
        except Exception as e:
            logger.exception('Failed to write conf object')
            if isinstance(e, splunk.RESTException):
                msg = e.get_extended_message_text()
            else:  
                msg = str(e)
            raise HcException(HCERR2003, {'conf':confName, 'stanza':stanzaName, 'error':msg})   
     
    def simpleRequest(self, uri, method='GET', postargs=None, raiseAllErrors=True):
        try:
            response, content = rest.simpleRequest(uri, self.getSessionKey(), method=method, postargs=postargs, raiseAllErrors=raiseAllErrors)
        except Exception as e:
            raise HcException(HCERR2006, {'uri':uri, 'status':'', 'reason':str(e), 'response':''})    
    
        if response.status not in [200, 201] and raiseAllErrors:
            raise HcException(HCERR2006, {'uri':uri, 'status':response.status, 'reason':response.reason, 'response':json.dump(response)})
        return response, content
    
    # delegate to /servicesNS/<user>/<app>/admin/conf-principals
    def delegate(self, delegate_endpoint, confInfo, method='POST', customAction='', args={}):
        user = self.context == admin.CONTEXT_APP_AND_USER and self.userName or "nobody"
    
        if self.requestedAction == admin.ACTION_CREATE:
           uri = en.buildEndpoint(delegate_endpoint, namespace=self.appName, owner=user)
           args['name'] = self.callerArgs.id
        else:
           uri = en.buildEndpoint(delegate_endpoint, entityName=self.callerArgs.id, namespace=self.appName, owner=self.userName)
    
        if len(customAction) > 0:
           uri += '/' + customAction
    
        # replace None with empty string to avoid being ignored by python rest api. SPL-57111
        for k,v in self.callerArgs.items():
            if k.startswith('_'):
               continue
            if isinstance(v, list):
               args[k] = v[0] if v[0] != None else ''
            else:
               args[k] = v if v != None else ''
    
        if method == 'GET':
           app  = self.context != admin.CONTEXT_NONE         and self.appName  or "-"
           user = self.context == admin.CONTEXT_APP_AND_USER and self.userName or "-"
           
           thing = self.getEntities(None, None, uri, -1, app, user)
           for name, obj in thing.items():
               ci = confInfo[name]
               for key, val in obj.items():
                   if not key.startswith('eai:'):
                      ci[key] = str(val) if val else ''
              
               # fix perms
               if 'perms' in obj['eai:acl'] and not obj['eai:acl']['perms']:
                  obj['eai:acl']['perms'] = {}
    
               ci.copyMetadata(obj)
        else:
           serverResponse, serverContent = self.simpleRequest(uri, method, args)
    
