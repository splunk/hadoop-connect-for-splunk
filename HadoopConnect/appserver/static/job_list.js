

    var curPage = 0;
    var pager = $(".REDJobs .Paginator");
    var itemsPerPage = parseInt(pager.data('count'));
    var pages;

    function updatePagerCount(from, to, total) {
        if(total) pager.find('span.count').html(['Showing <span>', from, '-', to, '</span> of ', total, total !== 1 ? ' results' : ' result'].join(''));
        else pager.find('span.count').html('');
    }

    function paginateJobs() {
        var items = $('.REDJobs .splTable tbody tr');
        var from = curPage * itemsPerPage,
                to = Math.min(curPage * itemsPerPage + itemsPerPage, items.length),
                total = items.length;
        updatePagerCount(from + 1, to, total);
        pager.find('li').not('.previous,.next').remove();
        pages = Math.ceil(items.length / itemsPerPage);
        if (pages && curPage >= pages) {
            curPage = 0;
            paginateJobs();
            return;
        }
        if (pages > 1) {
            for (var i = pages; i > 0; i--) {
                var a = $('<a></a>').text(String(i)).data('page', i - 1).attr('href', 'page-' + i);
                var li = $('<li></li>').addClass('page').append(a);
                if (i - 1 === curPage) {
                    a.addClass('active');
                    li.addClass('active');
                }
                li.insertAfter(pager.find('ul li.previous'));
            }
        }
        pager.find('li.previous>a')[ total === 0 || curPage === 0 ? 'addClass' : 'removeClass' ]('disabled');
        pager.find('li.next>a')[ total === 0 || curPage >= pages - 1 ? 'addClass' : 'removeClass' ]('disabled');

        items.each(function (i) {
            $(this)[ i >= from && i < to ? 'show' : 'hide']();
        });
    }

    $('.REDJobs .Paginator a').live('click', function (e) {
        e.preventDefault();
        var a = $(this);
        if (a.is('.disabled,.active')) {
            return;
        }
        if (a.is('.next>a')) {
            curPage = Math.min(curPage + 1, pages - 1);
        } else if (a.is('.previous>a')) {
            curPage = Math.max(0, curPage - 1);
        } else {
            var p = parseInt(a.data('page'));
            if (p !== void(0)) {
                curPage = p;
            }
        }
        paginateJobs();
        reloadJobs();
    });

    function showErrorMessage(msg) {
        var el = $('<p class="error"></p>');
        el.text(msg);
        $('.REDJobs .messages').prepend(el);
        setTimeout(function () {
            el.remove();
        }, 10000);
    }

    function reloadJobs() {
        $('.REDJobs .splTable tbody').load(Splunk.util.make_url('/custom/HadoopConnect/exportjobs/list') + '?_=' + new Date().getTime(), paginateJobs);
    }

    $('.REDJobs a.action').live('click', function (e) {
        e.preventDefault();

        if ($(this).is('.confirm')) {
            if (!confirm($(this).data('confirm-text'))) {
                return;
            }
        }
        var el = $(this);
        el.addClass('disabled');
        $.ajax({
            url: $(this).attr('href'),
            type: 'POST',
            data: {
                id: $(this).data('id')
            },
            dataType: 'json',
            success: function (data) {
                if (data && data.success) {
                    reloadJobs();
                } else {
                    el.removeClass('disabled');
                    showErrorMessage(data.error || _('ERROR'))
                }
            }
        });

    });

    $('.REDJobs a.edit').live('click', function(e){
        e.preventDefault();

        var el = $('<div></div>').addClass('red-edit-popup').appendTo(document.body);
        $('<h2></h2>').text('Edit Scheduled Export').appendTo(el);
        var content = $('<div></div>').addClass('content').appendTo(el);
        content.load($(this).attr('href'), function() {
            applyContextHelp(content);
            $('.datetime', content).bind('change', function(evt) {
                $('.starttime', content).val($(this).datepicker('getDate').valueOf() / 1000 );
            });
            var dateFormat = $.datepicker._defaults['dateFormat'];
            var defaultDate = new  Date();
            $(".datetime", content).datepicker({
                currentText: '',
                defaultDate: defaultDate,
                prevText: '',
                nextText: ''
            });
            var curDv = $('.starttime', content).val();
            if(curDv) {
                $(".datetime", content).datepicker('setDate', new Date(parseInt(curDv)*1000));
            }
        });

        var btn = $('<div></div>').addClass('ButtonRow').appendTo(el);
        var cancelButton = $('<a></a>').addClass('splButton-secondary');
        cancelButton.append($('<span></span>').text('Cancel')).appendTo(btn);
        var submitButton = $('<a></a>').addClass('splButton-primary');
        submitButton.append($('<span></span>').text('Save')).appendTo(btn);

        var editPopup = new Splunk.Popup(el, {
            cloneFlag: false,
            pclass: 'configPopup'
        });

        cancelButton.click(editPopup.destroyPopup.bind(editPopup));

        function submitExportJob(e) {
            if(e) e.preventDefault();
            var form = el.find('form');

            $.ajax({
                url: form.attr('action'),
                type: form.attr('method'),
                data: form.serialize(),
                success: function() {
                    editPopup.destroyPopup();
                    reloadJobs();
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

        submitButton.click(submitExportJob);
        $('form', el).submit(submitExportJob);
    });

    paginateJobs();
