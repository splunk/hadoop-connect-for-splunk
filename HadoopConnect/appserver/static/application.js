if (Splunk && Splunk.Module && Splunk.Module.IFrameInclude) {
    Splunk.Module.IFrameInclude = $.klass(Splunk.Module.IFrameInclude, {
        onLoad: function(event) {
            this.logger.info("IFrameInclude onLoad event fired.");

            this.resize();
            this.iframe.contents().find("body").click(this.resize.bind(this));
            this.iframe.contents().find("select, input").change(this.resize.bind(this));
            // need to fire resize on browser resize (might also need to throttle)
            $(window).resize(this.resize.bind(this));
        },

        resize: function(e) {
                this.logger.info("IFrameInclude resize fired.");

                var height = this.getHeight();
                if(height<1){
                    this.iframe[0].style.height = "auto";
                    this.iframe[0].scrolling = "auto";
                }else{
                    this.iframe[0].style.height = height + this.IFRAME_HEIGHT_FIX + 20 + "px";
                    this.iframe[0].scrolling = "no";
                }

        }

    });
}

$(function(){
    $('.HDFSEntitySelectLister').appendTo('.sel');
});

function asyncSubmit(event) {
    event.preventDefault();
    $this = $(event.target);
    var data = $this.serialize();
    $.ajax({
        type: 'POST',
        url: event.target.action,
        data: data,
        dataType: 'text'
    }).success(function(){
        top.location = $this.data('redirect');
    }).fail(function(response){
        setAsyncIframe(response.responseText);
    });
    return false;
}

function setAsyncIframe(text){
    $('.messages').html($(text).find('.messages')).trigger('click');
}
