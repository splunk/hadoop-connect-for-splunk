Splunk.Module.HDFSFileBreadCrumb = $.klass(Splunk.Module.HDFSExtendedFieldSearch, {


    initialize: function($super, container) {
        $super(container);

    },

    applyContext: function($super, context) {
        $super(context);
        if (context.get('click.name') == this.fieldName) {
            return this.updateBreadCrumb(context.get('click.value'), true);
        }
        return false;
    },

    updateBreadCrumb: function(filepath, push) {
        this.href = filepath;

        console.log(filepath);
        console.log(this.getContext());
        console.log(this.getContext().get("search"));

        //we are an input field set the input too.
        this.setInputField(filepath);

        if (filepath){
            filepath = filepath.split('/');
        }
        else return;

        $("span.breadCrumb", this.container).html( "<span class='crumbs'></span>");

        var path;
        var section;
        var link;
        var stop = 3;
        if ($.trim($('.sel').html()) !== ''){
            console.log("|" + $('.sel').html() + "|");
            stop = 4;
        }
        while(filepath.length>=stop){
            path = filepath.join('/');
            section = filepath.pop();
            link = $('<a>',{
                text: section,
                title: section,
                href: path,
                click: function(){ return false;}
            });
            link.bind("click", this._onClick.bind(this));
            $('span.crumbs', this.container).prepend($('<span>').html(link));
            if (filepath.length >= 3) {
                $('span.crumbs', this.container).prepend($('<span> / </span>'));
            }
        }

        var context = this.getContext();
        context.set('form.' + this.fieldName, this.href);

        if (this.isPageLoadComplete() && push) {
            this.pushContextToChildren();
            return true;
        }
    },

    getIntention: function($super, value) {
        if(value && !/^['"].*['"]$/.test(value)) {
            value = ['"',value.replace(/"/g,'\\"'),'"'].join('');
        }
        return $super(value);
    },

    onContextChange: function($super) {
        $super();
        var context=this.getContext();

        if (context.get('click.name') == this.fieldName) {
            this.updateBreadCrumb(context.get('click.value'), false);
            return Splunk.Module.CANCEL;
        }
    },

    /**
     * override me to pass a modified - or refined - context to children
     * rather than the context this module was given.
     */
    getModifiedContext: function($super) {
        this.updateBreadCrumb(this.getReplacementValue(), false);
        return $super();
    },

    _onClick: function(evt) {
        this.updateBreadCrumb(decodeURIComponent(evt.target.href), true);
        return false;
    }


});
