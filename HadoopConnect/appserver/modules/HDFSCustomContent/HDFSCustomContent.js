Splunk.namespace('Splunk.Module');

Splunk.Module.HDFSCustomContent = $.klass(Splunk.Module, {
    initialize: function ($super, container) {
        $super(container);

        // ERP-875
        // Add any additional leading language and root endpoints to static
        // content links. See bug SPL-32106 for reasoning.
        $('a', this.container).each(function(i, el) {
            var new_url = $(el).attr('href');
            // Only add root endpoints to absolute local urls
            if (!new_url || new_url.indexOf('/') !== 0)
                return;

            new_url = Splunk.util.make_url(new_url);
            $(el).attr('href', new_url);
        });

        this.loadCustomContent();
        $(this.loadCustomCSS.bind(this));
    },
    loadCustomContent: function () {
        var url = Splunk.util.make_url(this.getParam('url'));
        $.ajax({
            url: url,
            cache: false,
            dataType: 'html',
            context: this,
            success: this.loadComplete,
            error: this.loadError
        });
    },
    loadComplete: function (data, status, xhr) {
        if ($.isReady) {
            this.applyContent(data);
        }
        else {
            $(this.applyContent.bind(this, data));
        }
    },
    applyContent: function (data) {
        this.container.find(this.getParam('destSelector')).html(data);
        this.loadCustomScripts();
    },
    loadCustomScripts: function () {
        var scripts = this.getParam('scripts');
        $.each(scripts, function (i, script) {
            $.getScript(Splunk.util.make_url(script));
        });
    },
    loadCustomCSS: function () {
        var stylesheets = this.getParam('stylesheets');
        $.each(stylesheets, function (i, href) {
            if (document.createStyleSheet) {
                document.createStyleSheet(Splunk.util.make_url(href));
            } else {
                $('<link/>').attr({
                    rel: 'stylesheet',
                    type: 'text/css',
                    href: Splunk.util.make_url(href)
                }).appendTo($('head'));
            }
        });
    },
    loadError: function () {
        $(function () {
            this.container.find('.custom-content').append($('<div class="error-message"></div>')
                    .text(_('Error loading content')));
        }.bind(this));
    }
});
