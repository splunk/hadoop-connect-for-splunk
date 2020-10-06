Splunk.namespace("Module");
Splunk.Module.HDFSFTR= $.klass(Splunk.Module, {

    ADMIN_CONFIGURE: $('.admin_configure'),

    NON_ADMIN_CONFIGURE:  $('.non_admin_configure'),

    initialize: function($super, container) {
        $super(container);
        
        // ERP-875 - add root_endpoint to images
        $('img', this.container).each(function(i, el) {
            var new_url = $(el).attr('src');
            // Only add root endpoints to absolute local urls
            if (!new_url || new_url.indexOf('/') !== 0)
                return;

            new_url = Splunk.util.make_url(new_url);
            $(el).attr('src', new_url);
        });

        this.logger = Splunk.Logger.getLogger("hadoop_ops_ftr.js");
        this.messenger = Splunk.Messenger.System.getInstance();
        this.popupDiv = $('.ftrPopup', this.container).get(0);
        this.redirectTo = this._params['configLink'];
        this.getResults();
    },

    renderResults: function(response, turbo) {
        if ((response.has_ignored && response.has_ignored===true)
                || (response.is_configured && response.is_configured===true)) {
            return true;
        }
        if (response.is_admin && response.is_admin===true) {
            this.ADMIN_CONFIGURE.show();
            this.popup = new Splunk.Popup(this.popupDiv, {
                cloneFlag: false,
                title: _("Welcome to Hadoop Connect!"),
                pclass: 'configPopup',
                buttons: [
                     {
                         label: _("Ignore"),
                         type: "secondary",
                         callback: function(){
                             this.setIgnored();
                             return true;
                         }.bind(this)
                     },
                     {
                         label: _("Configure"),
                         type: "primary",
                         callback: function(){
                             Splunk.util.redirect_to(this.redirectTo);
                         }.bind(this)
                     }
                 ]
             });
        } else {
            this.NON_ADMIN_CONFIGURE.show();
            this.popup = new Splunk.Popup(this.popupDiv, {
                cloneFlag: false,
                title: _("This App Needs Configuration"),
                pclass: 'configPopup',
                buttons: [
                    {
                        label: _("Continue"),
                        type: "primary",
                        callback: function(){
                            this.setIgnored();
                            return true;
                        }.bind(this)
                    }
                ]
           });
        }
    },

    setIgnored: function() {
        var params = this.getResultParams();
        if (!params.hasOwnProperty('client_app')) {
            params['client_app'] = Splunk.util.getCurrentApp();
        }
        params['set_ignore'] = true;
        var xhr = $.ajax({
                        type:'GET',
                        url: Splunk.util.make_url('module', Splunk.util.getConfigValue('SYSTEM_NAMESPACE'), this.moduleType, 'render?' + Splunk.util.propToQueryString(params)),
                        beforeSend: function(xhr) {
                            xhr.setRequestHeader('X-Splunk-Module', this.moduleType);
                        },
                        success: function() {
                            return true;
                        }.bind(this),
                        error: function() {
                            this.logger.error(_('Unable to set ignored flag'));
                        }.bind(this),
                        complete: function() {
                            return true;
                        }
        });

    }
});

