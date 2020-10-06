//put Module in the namespace if it isnt already there.
Splunk.namespace("Module");

// A simple, but extensible, results table
Splunk.Module.HDFSSimpleFileTable = $.klass(Splunk.Module.SimpleResultsTable, {

    onRowClick: function($super, evt) {
        // browsers are annoying.  If you have say a tablecell, call it A,  and a link element, call it B.
        // and there's a mousedown on B,  then you move the mouse and mouseup on A.  Whether or not there's a selection,
        // firefox says this is not a click, therefore no onRowClick firing, therefore no problem. Happy user copy's and pastes.
        // if there's a mousedown on B, some moving of the mouse and mouseup on B.  ok. This sucks.  Even if the human's
        // intention is to select text,  the browser calls this a click.  So we explicitly check for a text selection.
        // if there's a text selection immediately at the moment of a click, then the click somehow managed to select text.
        // Operationally our UI interprets this to mean no click.  no harm no foul.
        if (this.getCurrentSelectedText().length > 0) {
            this.logger.debug("received a click event, or at least what the browser tells us is a click event, but there's a text selection right now so the browser's lying. Returning.");
            return false;
        }

        var el = $(evt.target);

        // let normal links through
        if (el.is('a')) {
            return true;
        }

        // skip the number column
        if (el.hasClass('pos')) return;

        //1. Sorting Clicks

        // when the little sorting arrows themselves arent clickable god kills a kitten.
        if (el[0].tagName == 'SPAN') el = el.parent();

        // Check if this is a click on a sorter.
        if (this.isSortingElement(el)) {
            var sortField = Splunk.util.trim(el.text());
            if (sortField == this.getSortField()) {
                this.setSortAsc(!this.getSortAsc());
            } else {
                this.setSortAsc(false);
                this.setSortField(sortField);
            }
            this.getResults();
            return;
        }

        // if this is a multivalue field and you're clicking the TD instead of a value, we bail..
        if (el[0].tagName == 'TD' && $(el).find("div.mv").length > 0) return false;

        // bail if somehow this isnt a td element
        if (el[0].tagName != 'TD' && el.parent()[0].tagName != 'TD') return;

        //3 hereafter we are definitely handling a drilldown click.
        // Make our child modules visible.
        this.showDescendants(this.DRILLDOWN_VISIBILITY_KEY + "_" + this.moduleId);
        // remove old highlighting.
        if (this._selection && this._selection.element) {
            this._selection.element.removeClass("selected");
        }
        //get the new selection
        this._selection = {};
        this._selection.name='Location';
        var value = $(evt.target).parents('tr').data('location');
        if($(evt.target).parents('tr').data('dir') !== 'True') {
            if(!/^['"].*['"]$/.test(value)) {
                value = ['"',value.replace(/"/g,'\\"'),'"'].join('');
            }
        }
        this._selection.value=value;
        // add new highlighting.
        if (this._selection && this._selection.element) {
            this._selection.element.addClass("selected");
        }

        // now that we have setup the selection state,
        // that selection state will be put onto the context
        // within getModifiedContext
        // All we do here is push.

        //if we are a file we would like to pass to children
        if ($(evt.target).parents('tr').data('dir') == "False") {
            this.pushContextToChildren();
        }
        //if we are a directory we want to go up
        else {
            this.passContextToParent(this.getModifiedContext());
        }

    },
    onBeforeJobDispatched: function($super, search) {
        console.log("dispatching");
        console.log(search);
        console.log(this.getNormalizedFields());
        $super(search);
    }
});
