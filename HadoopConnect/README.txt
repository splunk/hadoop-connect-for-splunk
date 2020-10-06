
INTRO
=======================
The purposes of this app are:
	- reliable event delivery from Splunk to HDFS 
	- explore and visualize HDFS data 
	- read data from HDFS into Splunk at search
        - import and index HDFS data - available only in Splunk 5.x

REQUIREMENTS: DEPENDENCIES
=======================
- Splunk 4.3.x or later
- Hadoop command line utilities
-- Oracle Java 6u27 or later
-- Please put Splunklib Python SDK into the lib folder before building the application.

FEATURE: RELIABLE EVENT DELIVERY
=======================
The goal of this feature is to enable reliable and timely delivery of the events indexed in Splunk into HDFS giving user the ability 
to partition the data in HDFS using any fields present in the event. Partitioning data makes it easier for MR jobs to select the data 
they need to process. Event delivery to HDFS is based on scheduled searches which return continuous and non-overlapping sets of data 
which are to be exported. This method has minimal impact on Splunk's indexing capability and provides the flexibility of saved searches
to control when the export happens. The export works by processing chunks of data received from the search and creating compressed 
files which are transfered to HDFS once they reach a size of ~64MB. The files use a temporary extenssion (.hdfs) while an export is in 
progress, this temporary extenssion is removed when the export completes succesfully (or deleted in case of failure). 


Partitioning
-----------------------
The app exposes some out of the box partitioning variables: date, hour, host, source, sourcetype. 
However you can use *any* field to compute a partitioning path into the events/results of the 
search to be exported into a special field called: _dstpath. For example if you want to export your
search results into a path that looks like: <base-path>/<date>/<hour>/<app> you can use the following search:

search ..... | eval _dstpath=strftime(_time, "%Y%m%d/%H") + "/" + app
 

File naming convention
-----------------------
Final files:
<export-name-hash>_<export-earliest-time>_<export-latest-time>_<file-id>.<format>.gz

Temporary files:
<export-name-hash>_<export-earliest-time>_<export-latest-time>_<file-id>.<format>.gz.hdfs

Cursor files, only written in base path location
Final
<export-name-hash>_<export-latest-time>.cursor

Searching
<export-name-hash>_<export-latest-time>.cursor.0

Renaming
<export-name-hash>_<export-latest-time>.cursor.1


What searches can be exported?
-----------------------
The results/events of streaming (ie non reporting) searches can be
continuously exported to HDFS. You can choose to export the events in their
raw format or preprocess them in Splunk first and export only a subset of the
their fields in a format of your choice (json, xml, csv, tsv).


Multiple Hadoop clusters, running different Hadoop versions, Kerberos
----------------------
This app can communicate with different clusters, potentially running
different versions of Hadoop and potentially Kerberized. To achieve this we 
recommend that you use the UI tools, but you can configure it manually by:
  1. adding an entry in clusters.conf for the cluster you need to use, specifying the
     the path to hadoop command line utilities in hadoop_home
  2. set java_home to point to the value of JAVA_HOME to be used when communicating with this cluster   
  3. creating core-site.xml for the cluster in $SPLUNK_HOME/etc/apps/HadoopConnect/local/clusters/<host>_<ipc port>/
  4. if the cluster is kerberized read the next section

Configure a Kerberized HDFS cluster
----------------------
The app supports communication with Kerberized clusters. The following
requirements MUST be met in order for this to be successful:

1. Kerberos client utilities must be installed in the system where the app is running 
   (ie. the following commands must work: kinit, klist ....)
   (example install command: # yum install krb5-workstation krb5-libs krb5-auth-dialog)

2. /etc/krb5.conf must be configured to point to the right KDC (talk to your
   Kerberos administrator for how to set this file up)

3. You have the credentials of a Kerberos principal

4. ensure that you can get a Kerberos ticket by running:
   $ kinit <kerberos-principal>
   <enter password>
   $ klist
   <valid ticket output> 

5. You must generate a keytab file for every Kerberos principal you want to use.
   (see ktutil for how to generate one, or ask your Kerberos admin to generate
    one for you)

6. Ensure that you can get a TGT for a principal using the keytab file by running
   kinit -k -t <path-to-keytab-file> <principal>
   <make sure your are NOT prompted for a password, and no errors occur>

7. Hadoop command line utilities must be the same version as the secure cluster 
   you want to connect to 

You are now ready to configure Kerberized support, we recommend that you use
the UI tools provided by the app, but you can configure it manually by:

1. follow steps in previous section to configure the cluster. 
   IMPORTANT: make sure core-site.xml is configured to support access to a kerberized cluster 
   (look at default/clusters/secure.example.com_9000/core-site.xml for an example)

2. Specify a default kerberos_principal in clusters.conf for your cluster  

3. upload/copy the keytab of any Kerberos principal that would ever use the cluster
  (including the default kerberos_principal you specified in clusters.conf) into
   $SPLUNK_HOME/etc/apps/HadoopConnect/local/principals/<fs-safe-principal-name>
   a principal name is made fs safe by replacing / with __


FEATURE: EXPLORATION
=======================
Browse:
   The app supports exploration of HDFS via the use of the hdfs search command
   (see below). A number of views that make it easier to explore the contents of
   HDFS put a UI on top of this search command. 
Read: 
   Through the use of hdfs command you can load data from HDFS into Splunk
   search. This is extremely useful for visualizing the results of MapReduce
   jobs. e.g. | hdfs read hdfs://namenode:port/mr/results-00000 | sort 10 -term
   and choose to visualize the results as a column chart. 


FEATURE: INDEX HDFS DATA INTO SPLUNK
=======================
Only supported in Splunk 5.x. The app includes a modular input for monitoring
files that reside in HDFS just like files in the local filesystem. To
configure them from the UI simply go to: Manager >> Data inputs >> HDFS 


SEARCH COMMANDS
=======================
runexport  - triggers and manages an export job, handles cursor and failure conditions, renames files to final destination. Meant to be used by scheduler only.
exporthdfs - partitions, formats, compresses and writes data to HDFS (by first staging it on local disk using a specified max local disk space) 
hdfs       - explore the contents of HDFS, you can read data and metadata at search time
See: default/searchbnf.conf for more details

LOGS
=======================
The app logs into the following dir: $SPLUNK_HOME/var/logs/splunk/

export_metrics.log - export metrics data 
HadoopConnect.log - any non metric logs, such as errors or warnings are logged here




