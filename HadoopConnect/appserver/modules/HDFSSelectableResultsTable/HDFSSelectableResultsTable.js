Splunk.namespace("Module");

Splunk.Module.HDFSSelectableResultsTable = $.klass(Splunk.Module.SimpleResultsTable, {
    NONE_SELECTED: 'NONE_SELECTED',
    initialize: function($super, ct) {
        $super(ct);
        $('input[type=checkbox]', this.container).live('change', this.handleSelectionChange.bind(this));
        $('.toggle-all a', this.container).live('click', this.handleBulkChange.bind(this));
        this._allSelected = true;
    },
    handleSelectionChange: function(e) {
        var box = $(e.target);
        this.setAllSelected(false);
        this.updateSearchString(true);
    },
    handleBulkChange: function(e) {
        e.preventDefault();
        var t = $(e.target);
        if(t.is('.select-all')) {
            this.container.find(".selectable-value>input[type=checkbox]").attr('checked','checked');
            this.setAllSelected(true);
        } else if(t.is('.select-none')) {
            this.container.find(".selectable-value>input[type=checkbox]").removeAttr('checked','checked');
            this.setAllSelected(false);
        }
        this.updateSearchString(true);
    },
    setAllSelected: function(selected) {
        this._allSelected = selected;
        this.container.find('.toggle-all>a.select-all')[selected ? 'addClass' : 'removeClass']('active');
    },
    updatePagination: function() {
        var ctx = this.getContext(), count = parseInt(ctx.get('results.count')), offset = ctx.get('results.offset');
        this.container.find('.selectable-value').each(function(i){
            if(i >= offset && i< offset+count) {
                $(this).parent().show();
            } else {
                $(this).parent().hide();
            }
        });
    },
    restoreSelectionState: function() {
        if(this._selectionState) {
            var sel = {}, clearAll = false;
            for (var i = 0; i < this._selectionState.length; i++) {
                sel[this._selectionState[i]] = true;
            }
            this.container.find(".selectable-value>input[type=checkbox]").each(function(){
                if(!sel[$(this).data('selectable-value')]) {
                    $(this).removeAttr('checked');
                    clearAll = true;
                }
            });
            if(clearAll) {
                this.container.find('.toggle-all>input[type=checkbox]').removeAttr('checked');
            }
        }
    },
    updateSearchString: function(updateState) {
        var values = this.container.find(".selectable-value>input[type=checkbox]:checked").map(function(){ return $(this).data('selectable-value') });
        if(updateState)
            this._selectionState = this._allSelected ? null :  values;
        var field = this.getParam('selectSearchField'), searchString;
        if(!values.length) {
            //searchString = [ '(', field, '=', JSON.stringify(this.getParam('selectEmptySearchValue')||''), ')' ].join('');
            Splunk.Module.prototype.hideDescendants.call(this, this.NONE_SELECTED + this.moduleId);
            searchString = null;
        } else {
            Splunk.Module.prototype.showDescendants.call(this, this.NONE_SELECTED + this.moduleId);
            if(this._allSelected) values = ['*'];
            var parts = $.map(values, function(val){ return [ field, '=', JSON.stringify(val) ].join(''); });
            searchString = [ '(', parts.join(' OR '), ')' ].join('');
        }
        if(this._selection !== searchString) {
            this._selection = searchString;
            this.pushContextToChildren();
        }
    },
    getResultParams: function($super) {
        var params = $super();
        params['selectField'] = this.getParam('selectField');
        params['selectLabel'] = this.getParam('selectLabel');
        params['count'] = 1000;
        params['offset'] = 0;
        return params;
    },
    getModifiedContext : function() {
        var context = this.getContext();

        if(this._selection) {
            context.set('selection', this._selection);
        }

        context.set("results.offset", 0);
        context.set("results.upstreamPaginator", null);
        return context;
    },
    renderResults: function($super, results) {
        $super(results);
        this.restoreSelectionState();
        if(this._allSelected)
            this.container.find('.toggle-all>a.select-all').addClass('active');
        this.updateSearchString();
        this.updatePagination();
    },
    getResults: function($super) {
        this._curSID = this.getContext().get('search').job.getSearchId();
        $super();
    },
    pushContextToChildren: function($super, explicitContext) {
        ctx = explicitContext || this.getContext();
        var search = ctx.get('search');
        if(search && search.getTimeRange().isRealTime()) {
            search.job.cancel();
            this.resetContext();
            if(!this.container.find('p.error').length)
                $('<p></p>').addClass('error').text('This module does not support real-time searches. Please select a historical time-range.').appendTo(this.container);
        } else {
            return $super(explicitContext);
        }
    },
    onContextChange: function($super) {
        this._selection = null;
        var context = this.getContext();
        var search  = context.get("search");
        if (search && search.job.isDone()) {
            if(search.job.getSearchId() !== this._curSID) {
                this.getResults();
            }
        }
        this.updatePagination();
    },
    isReadyForContextPush: function($super) {
        if (!this._selection) {
            return Splunk.Module.CANCEL;
        }
        return $super();
    },
    hideDescendants: function(){},
    onEventRowClick: function(){},
    onRowClick: function(){}
});