import logging
import json
import sys 
import os

import cherrypy
import splunk
import splunk.auth as auth
import splunk.bundle as bundle
import splunk.util as util
import splunk.appserver.mrsparkle.lib.util as app_util
from splunk.appserver.mrsparkle.lib import jsonresponse
import controllers.module as module

from splunk.models.app import App 

try:
    APP_NAME = __file__.split(os.sep)[-5]
except:
    APP_NAME = 'HDFSExport'
    
dir = os.path.join(app_util.get_apps_dir(), APP_NAME, 'bin')
if not dir in sys.path:
    sys.path.append(dir)


logger = logging.getLogger('splunk.module.HDFSFTR')

class HDFSFTR(module.ModuleHandler):
    ''' 
    checks to see if app is configured or if user ignored last call to action 
    ingnore == same as configure but without actually configuring things 
    '''

    def generateResults(self, **kwargs):

        app_name = kwargs.get('client_app', APP_NAME)
        sessionKey = cherrypy.session.get('sessionKey') 
        user = cherrypy.session['user']['name']
        
        # if the current app doesn't exist... 
        app = App.get(App.build_id(app_name, app_name, user))

        if kwargs.get('set_ignore'):
            app.is_configured = True
            app.passive_save()
            return self.render_json({'has_ignored': True, 'errors': []})

        if app.is_configured:
            return self.render_json({'is_configured': True, 'errors': []})
        else:
            if self.is_app_admin(app, user):
                return self.render_json({'is_configured': False, 'is_admin': True, 'errors': []})
            else:
                return self.render_json({'is_configured': False, 'is_admin': False, 'errors': []})

    def is_app_admin(self, app, user):
        ''' 
        used to determine app administrator membership
        necessary because splunkd auth does not advertise inherited roles
        '''
        sub_roles = []
        admin_list = app.entity['eai:acl']['perms']['write'] 

        if '*' in admin_list:
            return True
        for role in auth.getUser(name=user)['roles']:
            if role in admin_list: 
                return True
            sub_roles.append(role)
        for role in sub_roles:
            for irole in auth.getRole(name=role)['imported_roles']:
                if irole in admin_list: 
                    return True
        return False 
      
    def render_json(self, response_data, set_mime='text/json'):
        ''' 
        clone of BaseController.render_json, which is
        not available to module controllers (SPL-43204)
        '''

        cherrypy.response.headers['Content-Type'] = set_mime

        if isinstance(response_data, jsonresponse.JsonResponse):
            response = response_data.toJson().replace("</", "<\\/")
        else:
            response = json.dumps(response_data).replace("</", "<\\/")

        # Pad with 256 bytes of whitespace for IE security issue. See SPL-34355
        return ' ' * 256  + '\n' + response
 
