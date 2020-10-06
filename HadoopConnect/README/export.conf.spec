[<export-name>]

uri = <string>
* URI of the HDFS cluster where to export the data.
* Example: hdfs://bigdata.example.com/

base_path = <string>
* Path where to export the data to. This path is appended to the URI to form the home location
* of the export data. 
* Example: /home/data/export 

partition_fields = <string list>
* Comma delimited list of fields to partition the export data by. Events with common field values
* would be placed in the same path. Currently the only supported fields are: time, host, source
* Example: time,host

search = <search-string>
* The search whose events are to be exported
* Example: index=BigData

roll_size = <unsigned int>
* Maximum file size (MB) before moving data to HDFS. It is recommended that this be slightly 
* less that the block size
* Example: 63

minspan = <unsigned int>
* The minimum time, seconds, that must have elapsed since last export before triggering a new 
* export. 

maxspan = <unsigned int>
* The maximum index time range that one export job must process. It is recommended that this 
* value is kept under 86400 (24 hours) to ensure that in case of a search error we do not roll
* back a lot of work. 

starttime = <epoc time>
* If no cursor is present use this as the starting time for the export. Setting this to 0 means 
* start exporting data from now.  

endtime = <unsigned int>
* The export end time. Set to 0 for never ending exports


kerberos_principal = <string>
* The kerberos principal to use when communicating with the cluster. Set this only iff the cluster
* is using kerberos.

parallel_searches = <unsigned int> | max
* The number of parallel searches to spawn for exporting data. Parallel searches will search 
* disjoint sets of indexers thus guaranteeing no overlap of processed data. 

replication = <int>
* The replication factor to use for a specific export job. Use 0 to denote the default replication
* level specified in the cluster. Defaults to 0 

format = <string>
* The data output format for a specific export job. Supported formats are: raw, json, xml, csv, tsv
 
fields = <string list>
* Comma separated list of fields to export. Can contain wildcarded (*) fields if the format allows it.
* NOTE: format csv/tsv do not allow wildcard fields
*       format raw only supports _raw field   

compress_level = <int>
* The compression level of the export data files. Valid values are between 0 to 9. 0 means no compression.
* Default value: 2

##############################################
#  export job status variables
##############################################

status = initializing | searching | renaming | done | failed 
* Indicates the current phase of the export job 

status.jobs = <string>
* The number of export searches spawned

status.jobs.psid = <string>
* The search id of scheduled search job

status.jobs.sids = <string>
* Comma delimited list of search ids of all export searches

status.jobs.progress = <float>
* Overall progress info of all export searches
 
status.jobs.runtime = <float>
* The total amount of time for which all parallel export searches have been running

status.jobs.errors = <string>
* User friendly error message if status is failed

status.jobs.earliest = <string>
* Earlist possible index time of search result of current export process

status.jobs.latest = <string>
* Latest possible index time of search result of current export process
                        
status.jobs.starttime = <epoc time>
* Current export process start time

status.jobs.endtime = <string>
* Current export process end time 

status.earliest = <epoc time>
* Earlist possible index time of all export data at all time
                        
status.latest = <epoc time>
* Latest possible index time of all export data at all time
                        
status.load = <float>
* Last 10 successful jobs load factors, where load = total execution time / export time range


