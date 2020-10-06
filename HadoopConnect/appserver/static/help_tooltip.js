// this is where all tooltip help info comes from
var tooltip_content =
{
  'export_name': 'Name of the scheduled export job.'
, 'export_search': 'Search string for the data you want to export.'
, 'export_cluster': 'Configured HDFS cluster or local filesystem to which you want to export the data. '
, 'export_base_path': 'A sub-directory for the provided URI. Is used to create a base export destination.'
, 'export_all_new': 'The time from which you want to start pulling data for export jobs.'
, 'export_parallel_searches': 'Number of parallel searches to use when running the export job.'
, 'export_format': 'The format of the exported data.'
, 'export_fields': 'Comma-separated list of the fields to export.'
, 'export_frequency': 'How often the export job runs.'
, 'export_last_completed': 'The last time this export job ran.'
, 'export_next_run': 'The next time this export job will run.'
, 'export_partition': 'The fields by which to partition your search results. A field value corresponds to a new path segment.'
, 'export_status' : 'The status of the export job.'
, 'export_complete': 'Fraction of the exportable data that has already been exported.'
, 'export_load_factor': 'How much of the available system capacity used by the last time the job ran. Computed as (export duration) / (exported time range)'
, 'export_actions': 'Available actions that you can take on an export job.'
, 'export_maxspan': 'Maximum processing time for one search job export, in seconds.'
, 'export_minspan': 'Minimum processing time for one search job export, in seconds.'
, 'export_roll_size': 'Maximum compressed file size for exports, in MB.'

, 'cluster_namenode' : 'Host and IPC port of the cluster\'s NameNode. For example: namenode.hadoop.example.com:8020'
, 'cluster_hadoop_home': 'Path to the Hadoop command line utilities to use when communicating with this cluster.'
, 'cluster_java_home': 'Path to the Java installation to use when communicating with this cluster.'
, 'cluster_secure': 'Whether the cluster is secured using Kerberos.'
, 'cluster_actions': 'The available actions that you can take on a cluster.'
, 'cluster_http_port': 'The NameNode\'s HTTP port.'
, 'cluster_service_principal': 'The Kerberos principal that the HDFS service runs as. This is the value of dfs.namenode.kerberos.principal in core-site.xml.'
, 'cluster_default_principal': 'The default Kerberos principal to use when communicating with the cluster.'

, 'cluster_local_name': 'A unique name for the Hadoop filesystem.'
, 'cluster_local_mount': 'Local path where the Hadoop filesystem is mounted.'

, 'kerberos_principal' : 'The fully qualified name of the Kerberos principal. For example: exporter@domain.example.com.'
, 'kerberos_actions' : 'Available actions that can be taken on a Kerberos principal.'
, 'kerberos_keytab_file': 'Location (in the Splunk server) of the keytab file for this principal.'
};

function applyContextHelp(el) {
    el = el || $(document.body);

   // replace place holders
    if(tooltip_content){
        var question_mark_img = '<img class="tooltip" src="' + 
                                Splunk.util.make_url('/static/app/HadoopConnect/images/question_mark_icon_small_grey.png') + 
                                '"/>';

        $(".help[title^='$']", el).each(function(index, value){
           var title = $(this).attr('title'), newTitle = title;
           if(title[0] == '$'){
                      title = title.substr(1);
              if(title in tooltip_content)
              newTitle = tooltip_content[title];
              $(this).removeAttr('title');
           }
           $(this).data('title', newTitle);
           $(this).append(question_mark_img);
        });
    }

    //if they leave the help icon lets try to cancel the callback to add the tooltip
    $('.tooltip', el).bind("mouseleave", function(evt){
        var $target = $(evt.target);
        clearTimeout($target.data('tooltip_content_callback'));
    });

    $('.tooltip', el).bind("mouseenter", function(evt){
        var $target = $(evt.target);

        //In order to ensure they are actually hovering add the help after 50ms
        var callback = setTimeout( function(){
            var $tooltip = $('<span class="tooltip_content"><img src="'+
                             Splunk.util.make_url('/static/app/HadoopConnect/images/question_mark_icon_small_grey_whitering.png') +
                             '"/></span>');
            var $text = $target.parent().data('title')||$target.parent().attr('title');
            $tooltip.append($text);
            $('body').append($tooltip);
            var offset = $target.offset();
            $tooltip.offset({ top: offset.top - 3 , left: offset.left + 3 });
            $('.tooltip_content').bind("mouseleave", function(evt){
                $(this).remove();
            });
        }, 50);
        //We need to keep track of the callback so we can cancel if they leave the help icon
        $target.data('tooltip_content_callback', callback);
    });
}

$(function(){  applyContextHelp(document.body); });
