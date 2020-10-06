import cherrypy
import logging
import os
import sys
import json

import splunk.appserver.mrsparkle.controllers as controllers
from splunk.appserver.mrsparkle.lib.decorators import expose_page
from splunk.appserver.mrsparkle.lib.routes import route
import splunk.appserver.mrsparkle.lib.util as app_util
from splunk.models.app import App

path = os.path.join(app_util.get_apps_dir(), __file__.split('.')[-2], 'bin')
if not path in sys.path:
    sys.path.append(path)

from HadoopConnect.models.cluster import Cluster
from HadoopConnect.models.principal import Principal
from HadoopConnect.models.export import HDFSExport

logger = logging.getLogger('splunk')


class ConfigController(controllers.BaseController):
    '''
    Controller for configuring hdfs clusters and their kerberos principals
    '''
    #TODO: This should be split into two controllers?

    @expose_page(must_login=True, methods=['GET'])
    @route('/:config=clusters/:action=list', methods=['GET'])
    def listClusters(self, config, action, **kwargs):
        '''provide a list of all the clusters for use with HDFS connect'''
        app = cherrypy.request.path_info.split('/')[3]
        user = cherrypy.session['user']['name']

        clusters = Cluster.all().filter_by_app(app)
        principals = Principal.all().filter_by_app(app)
        defaultExport = self.getNewHDFSExport(app, user)

        localClusters = []
        remoteClusters = []
        for cluster in clusters:
            uri = cluster.getURI()
            if uri.startswith('hdfs://'):
                remoteClusters.append(cluster)
            elif uri.startswith('file://'):
                localClusters.append(cluster)

        return self.render_template(
            '/%s:/templates/config_clusters.html' % app,
            dict(form_content='fomasdafe', app=app,
                 localClusters = localClusters, remoteClusters = remoteClusters,
                 principals=principals, defaultExport=defaultExport))

    @expose_page(must_login=True, methods=['GET'])
    @route('/:config=clusters/:action=add', methods=['GET'])
    def addCluster(self, config, action, **kwargs):
        '''add configuration for a single HDFS cluster'''
        app = cherrypy.request.path_info.split('/')[3]
        user = cherrypy.session['user']['name']
        selectedTab = 'remote'
        try:
            id = kwargs['id'] # will throw if id is not present
            cluster = Cluster.get(id)
            edit = True
            if cluster.uri.startswith('file://'):
                selectedTab = 'local'
        except:
            cluster = Cluster(app, user, name="")
            edit = False

        principals = Principal.all().filter_by_app(app)

        return self.render_template('/%s:/templates/add_cluster.html' % app,
                                    dict(form_content='fomasdafe', app=app,
                                         cluster=cluster,
                                         edit=edit,
                                         principals=principals,
                                         selectedTab = selectedTab))

    @expose_page(must_login=True, methods=['POST'])
    @route('/:config=clusters/:action=add', methods=['POST'])
    def submitCluster(self, config, action, **kwargs):
        '''add configuration for a single HDFS cluster'''
        app = cherrypy.request.path_info.split('/')[3]
        user = cherrypy.session['user']['name']

        errors = []
        if kwargs.get('secure', 0):
            #TODO: verify service principal is provided

            if kwargs.get('kerberos_principal') == 'add':
                principal = Principal(app, user, **{'name':kwargs.get('principal_name'), 'keytab_path': kwargs.get('principal_keytab_location')})
                if principal.passive_save():
                   kwargs['kerberos_principal'] = kwargs.get('principal_name')
                else:
                    errors += principal.errors
        else:
            if kwargs.get('kerberos_principal'):
                kwargs['kerberos_principal'] = ''
            if kwargs.get('kerberos_service_principal'):
                kwargs['kerberos_service_principal'] = ''

        id = kwargs.pop('id', None)
        type = kwargs.pop('type', None)
        if type == 'remote':
            kwargs['uri'] = 'hdfs://%s' % kwargs.get('name') if 'name' in kwargs else None
        elif type == 'local':
            kwargs['uri'] = 'file://%s' % kwargs.pop('local_mount')
        else:
            raise cherrypy.HTTPError(400, 'Expected cluster type parameter')

        try:
            cluster = Cluster.get(id)
            cluster.update(kwargs)
	    # Change the owner to nobody, so that REST call will be made to /servicesNS/nobody/HadoopConnect/...
	    # instead of /servicesNS/admin/HadoopConnect/... or /servicesNS/<owner>/HadoopConnect/...
            if cluster.entity and cluster.entity.owner:
                cluster.entity.owner='nobody'
            edit=True
        except:
            cluster = Cluster(app, user, **kwargs)
            edit=False

        # save stuff iff there were no errors while saving the principal
        if len(errors) == 0:
            logger.info("Saving cluster with args: %s " % kwargs)
            if cluster.passive_save():
                this_app = App.get(App.build_id(app, app, user))
                this_app.is_configured = True
                this_app.passive_save()
                if app_util.is_xhr():
                    cherrypy.response.status = 200
                    return ""
                raise cherrypy.HTTPRedirect(self.make_url(['app', app, 'config_clusters']), 303)

        principals = Principal.all().filter_by_app(app)
        principal_name = kwargs.get('principal_name', '')
        principal_keytab_location = kwargs.get('principal_keytab_location', '')
        cluster.errors += errors

        if app_util.is_xhr():
            cherrypy.response.status = 404
        return self.render_template('/%s:/templates/add_cluster.html' % app,
                                    dict(form_content='fomasdafe', app=app,
                                         cluster=cluster,
                                         edit=edit,
                                         principals=principals,
                                         principal_name=principal_name,
                                         principal_keytab_location=principal_keytab_location,
                                         selectedTab = type))

    @expose_page(must_login=True, methods=['POST'])
    @route('/:config=clusters/:action=delete', methods=['POST'])
    def deleteCluster(self, config, action, id, **kwargs):
        '''
        add configuration for a single principal used with a cluster's kerberos
        '''
        app = cherrypy.request.path_info.split('/')[3]

        cluster = Cluster.get(id)
        cluster.delete()
        raise cherrypy.HTTPRedirect(self.make_url(['custom', app, 'config',
                                                   'clusters', 'list']),
                                    303)


    ###########################################################
    ######## Principal actions
    ###########################################################


    @expose_page(must_login=True, methods=['GET'])
    @route('/:config=principals/:action=add', methods=['GET'])
    def addPrincipal(self, config, action, **kwargs):
        '''
        add configuration for a single principal used with a cluster's kerberos
        '''
        app = cherrypy.request.path_info.split('/')[3]
        user = cherrypy.session['user']['name']

        try:
            id = kwargs['id'] # will throw if id is not present
            principal = Principal.get(id)
        except:
            principal = Principal(app, user, name="")

        return self.render_template(
            '/%s:/templates/add_principal.html' % app,
            dict(form_content='fomasdafe', app=app, principal=principal))

    @expose_page(must_login=True, methods=['POST'])
    @route('/:config=principals/:action=add', methods=['POST'])
    def submitPrincipal(self, config, action, **kwargs):
        '''
        add configuration for a single principal used with a cluster's kerberos
        '''
        app = cherrypy.request.path_info.split('/')[3]
        user = cherrypy.session['user']['name']

        id = kwargs.pop('id', None)

        try:
            principal = Principal.get(id)
            principal.update(kwargs)
            # Change the owner to nobody, so that REST call will be made to /servicesNS/nobody/HadoopConnect/...
            # instead of /servicesNS/admin/HadoopConnect/... or /servicesNS/<owner>/HadoopConnect/...
            if principal.entity and principal.entity.owner:
                principal.entity.owner='nobody'
        except:
            principal = Principal(app, user, **kwargs)

        if principal.passive_save():
            if app_util.is_xhr():
                cherrypy.response.status = 200
                return ""
            else:
                raise cherrypy.HTTPRedirect(self.make_url(['app', app,
                                        'config_clusters']), 303)
        else:
            if app_util.is_xhr():
                cherrypy.response.status = 404
            return self.render_template(
                    '/%s:/templates/add_principal.html' % app,
                    dict(form_content='fomasdafe', app=app, principal=principal))

    @expose_page(must_login=True, methods=['POST'])
    @route('/:config=principals/:action=delete', methods=['POST'])
    def deletePrincipal(self, config, action, id, **kwargs):
        '''
        add configuration for a single principal used with a cluster's kerberos
        '''
        app = cherrypy.request.path_info.split('/')[3]

        principal = Principal.get(id)
        principal.delete()
        raise cherrypy.HTTPRedirect(self.make_url(['custom', app, 'config',
                                                   'clusters', 'list']),
                                    303)

    #TODO make this a lib functiont that can be used in any controller?
    def getNewHDFSExport(self, app, user):
        #this is a workaround for external REST handlers not supporting _new
        id = HDFSExport.build_id('_new_ext', app, user)
        h = HDFSExport.get(id)
        h.name = ''
        return h
