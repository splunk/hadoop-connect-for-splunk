Splunk.Module.HDFSFieldHeader = $.klass(Splunk.Module.HDFSExtendedFieldSearch, {

    INTENTION_NAME_KEY: 'name',
    INTENTION_ARG_KEY: 'arg',
    STRING_REPLACE_VALUE_KEY: 'value',

    initialize: function($super, container) {
        $super(container);
        this.input = $('.DisplayField', this.container);
    },


    /**
     * Adds an intention to the context and returns it.
     */



    /**
     * Returns the value that should be set in the intention, based on the defined replacementMap.
     */
    getReplacementValue: function() {
        var val = $.trim(this.input.text());

        // This should return null only if the stringreplace intention is defined.
        // This replaces the null value from the intention
        // so that the parser is not trying to handle null values for things like addterm intentions.
        // Really, if intentions are to stick around, they should codified into proper classes.
        if (val == '' &&
            this._params['intention'].hasOwnProperty('name') &&
            this._params['intention']['name'] == 'stringreplace') {
            return null;
        }
        return val;
    },



    setInputField: function(searchStr) {
        this.logger.debug(this.moduleType, ".setInputField old=", this.input.text(), "new=", searchStr);
        this.input.text(searchStr);
    }

});
