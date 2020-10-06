
Splunk.Module.HDFSEntitySelectLister = $.klass(Splunk.Module.EntitySelectLister, {


    getListValue: function() {
        if (this.firstValue) {
            return this.firstValue;
        }
        var selectUsingValue = Splunk.util.normalizeBoolean(this.getParam('selectUsingValue'));
        if (selectUsingValue) {
            return $('select', this.container).val();
        }
        else
            return $('select option:selected', this.container).text();
    },

    onUserAction: function($super, event) {
        if (event){
            delete(this.firstValue);
        }
        $super(event);
    },

    onContextChange: function() {
        var location = this.getContext().get(this.getParam('settingToCreate'));
        if (location) {
            this.firstValue = location;
            this.setParam('selected', location);
	    this.selectSelected();
        }
    }
});
