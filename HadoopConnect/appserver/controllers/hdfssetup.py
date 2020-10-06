import cherrypy
import logging
import os
import sys

import splunk.appserver.mrsparkle.controllers as controllers
from splunk.appserver.mrsparkle.lib.decorators import expose_page
from splunk.appserver.mrsparkle.lib.routes import route
import splunk.appserver.mrsparkle.lib.util as app_util

path = os.path.join(app_util.get_apps_dir(), __file__.split('.')[-2], 'bin')
if not path in sys.path:
    sys.path.append(path)

from HadoopConnect.models.export import HDFSExport
from HadoopConnect.models.cluster import Cluster 

logger = logging.getLogger('splunk')

class HDFSSetupController(controllers.BaseController):
    '''Splunk HDFS Setup Controller - provide configuration data for the app'''
    

    @expose_page(must_login=True, methods=['GET']) 
    @route('/:action=create', methods=['GET'])
    def create(self, action, **kwargs):
        '''Show the form to setup the Splunk HDFS app'''
        
        app = cherrypy.request.path_info.split('/')[3]
        user = cherrypy.session['user']['name']
        defaultExport = self.getNewHDFSExport(app, user)

        clusters = Cluster.all().filter_by_app(app)
        
        return self.render_template('/%s:/templates/export_defaults.html' % app, dict(form_content='fomasdafe', app=app, defaultExport = defaultExport, clusters = clusters))
        
    
    @expose_page(must_login=True, methods=['POST']) 
    @route('/:action=submit', methods=['POST'])
    def submit(self, action, **kwargs):
        '''Accept data to setup the Splunk HDFS app'''
        
        app = cherrypy.request.path_info.split('/')[3]
        user = cherrypy.session['user']['name']
        
        #remove the csrf protection...(perhaps this should be done for us?    
        del kwargs['splunk_form_key']
        
        #TODO make this a lib function or static method on the model and replace this and the one in the defaultExports.saveJob
        keys = kwargs.keys()
        partitions = []
        for k in keys:
            if k.startswith('partition_'):# and splunk.util.normalizeBoolean(kwargs.get(k, 'f')):
               partitions.append(k[len('partition_'):])
               del kwargs[k]

        kwargs['partition_fields'] = ','.join(partitions) if len(partitions) > 0 else 'None'
           
        kwargs['search'] = 'This should not get saved'
           
        defaultExport = HDFSExport(app, user,'default', **kwargs)
        defaultExport.metadata.sharing = 'app'
        defaultExport.metadata.owner = 'nobody'
        defaultExport.metadata.app = app
        
        logger.info("Submitted setup form with params: %s" % kwargs)

        if defaultExport.passive_save():
            if app_util.is_xhr():
                cherrypy.response.status = 200
                return ""
            raise cherrypy.HTTPRedirect(self.make_url(['app', app, 'config_clusters']), 303)
       
        if app_util.is_xhr():
            cherrypy.response.status = 404
        return self.render_template('/%s:/templates/export_defaults.html' % app, dict(form_content='fomasdafe', app=app, defaultExport = defaultExport))       
    
    #TODO make this a lib functiont that can be used in any controller?
    def getNewHDFSExport(self, app, user):
        #this is a workaround for external REST handlers not supporting _new
        id = HDFSExport.build_id('_new_ext', app, user)
        h = HDFSExport.get(id)
        h.name = ''
        return h

