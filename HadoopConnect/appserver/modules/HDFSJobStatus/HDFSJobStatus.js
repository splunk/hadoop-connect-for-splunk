Splunk.Module.HDFSJobStatus = $.klass(Splunk.Module.JobStatus, {

    HDFS_CREATE_URI: Splunk.util.make_url('custom/HadoopConnect/exportjobs/create'),
    HDFS_CREATE_TITLE: "Create Scheduled HDFS Export",
    initialize: function($super, container){
        $super(container);
        this.scheduleHDFSBtn = $(".schedule-hdfs", this.container);
        this.scheduleHDFSBtn.click(this.onHDFSSchedule.bind(this));
        $.getScript(Splunk.util.make_url('/static/app/HadoopConnect/help_tooltip.js'));
        var stylesheet = Splunk.util.make_url('/static/app/HadoopConnect/help_tooltip.css');
        if (document.createStyleSheet) {
            document.createStyleSheet(stylesheet);
        } else {
            $('<link/>').attr({
                rel: 'stylesheet',
                type: 'text/css',
                href: stylesheet
            }).appendTo($('head'));
        }
    },
    buildCreateMenu: function($super){
        $super();
        //TODO: EOL This
        this.createMenuItemsDict.push({
            "label" : _("Scheduled HDFS export"),
            "style": "create-report",
            "enabledWhen" : "progress",
            "callback": function(event){
                return this.onHDFSExport(event);
            }.bind(this),
            "showInFilter" : ['buildexport'],
            "enabled" : ["done", "progress"]
        });

        this.updateMenu(self.lastEnabled);
    },
    /**
     * Handle a HDFS export create action. Spawns a popup window from  EAI, w00t!.
     *
     * @param {Object} event A jQuery event.
     */
    onHDFSSchedule: function(event){
        if(this.scheduleHDFSBtn.hasClass(this.DISABLED_CLASS_NAME)){
            return false;
        }
        event.preventDefault();
        var path = this.HDFS_CREATE_URI;
        var title = this.HDFS_CREATE_TITLE;

        var el = $('<div></div>').addClass('red-edit-popup').appendTo(document.body);
        $('<h2></h2>').text(title).appendTo(el);
        var content = $('<div></div>').addClass('content').appendTo(el);
        content.load([path, Splunk.util.propToQueryString({ search: this.getContext().get("search") })].join('?'), function() {
            applyContextHelp(content);
            $('input[name=datetime]', content).bind('change', function(evt) {
                $('input[name=starttime]', content).val($('input[name=datetime]', content).datepicker('getDate').valueOf() / 1000 );
            });
            var dateFormat = $.datepicker._defaults['dateFormat'];
            var defaultDate = new  Date();
            $("input[name=datetime]", content).datepicker({
                currentText: '',
                defaultDate: defaultDate,
                prevText: '',
                nextText: ''
            });
            $("input[name=datetime]", content).datepicker('setDate', defaultDate).trigger('change');
            $('select[name=format]').change(function(e){
                var isRaw = $(this).val() == 'raw', fields = $('input[name=fields]');
                if(isRaw) fields.val('_raw');
                fields.prop('disabled',isRaw);
            }).trigger('change');
        });

        var btn = $('<div></div>').addClass('ButtonRow').appendTo(el);
        var cancelButton = $('<a></a>').addClass('splButton-secondary');
        cancelButton.append($('<span></span>').text('Cancel')).appendTo(btn);
        var submitButton = $('<a></a>').addClass('splButton-primary');
        submitButton.append($('<span></span>').text('Save')).appendTo(btn);

        var popup = new Splunk.Popup(el, {
            cloneFlag: false,
            pclass: 'configPopup'
        });

        cancelButton.click(popup.destroyPopup.bind(popup));

        function submitExportJob(e) {
            if(e) e.preventDefault();
            var form = el.find('form');

            $.ajax({
                url: form.attr('action'),
                type: form.attr('method'),
                data: form.serialize(),
                success: function(data) {
                    content.html(data);
                    btn.remove();
                    $('.cancel', content).click(function(e){
                        e.preventDefault();
                        popup.destroyPopup();
                    });
                },
                error: function(xhr) {
                    var msgs = ['Unknown error occurred'];
                    if(xhr.status === 500) {
                        try {
                            var json = JSON.parse(xhr.responseText);
                            if(json && json.errors.length) {
                                msgs = json.errors;
                            }
                        } catch(e){}
                    }
                    content.find('p.error').remove();
                    $.each(msgs, function(i, msg){
                        $('<p class="error"></p>').text(msg).prependTo(content);
                    });
                }
            });
        }

        $('form', content).live('submit', submitExportJob);
        submitButton.click(submitExportJob);

        $('.splButton-primary.cancel', el).live('click', function(e){
            e.preventDefault();
            popup.destroyPopup();
        });

        return false;
    },

    onContextChange: function($super){
        $super();
        this.enableLinks(this.scheduleHDFSBtn);
    },
    onBeforeJobDispatched: function($super, search){
        $super();
        search.setMinimumStatusBuckets(300);
        this.disableLinks(this.scheduleHDFSBtn);
    },
    onJobProgress: function($super){
        $super();
        this.enableLinks(this.scheduleHDFSBtn);
    }
});
