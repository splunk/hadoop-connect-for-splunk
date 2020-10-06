
[<host>:<ipc port>]
* For remote HDFS cluster, the stanza name should be contain the host and the ipc port that the Hadoop cluster is running 
  Example: hadoop.example.com:8020
* For a locally mounted Hadoop cluster, the stanza name has no special format.

uri = <string>
* Fully qualified cluster base uri path to where the export data is stored.
* For remote HDFS cluster, the uri must starts with "hdfs://" and the format is "hdfs:<host>:<ipc port>"
* For a locally mounted Hadoop cluster, the uri must start with "file://"

namenode_http_port = <int>
* The port in which the namenode is listening for HTTP requests 
* Defaults to: 50070
* Not valid for local mounted Hadoop cluster.
 
hadoop_home = <string>
* Path to the Hadoop command line utilities that the app should use when communicating with 
* this cluster. If set to the string "$HADOOP_HOME" the environment variable present while 
* starting splunk will be used
* Defaults to empty string
* Not valid for local mounted Hadoop cluster.

java_home = <string>
* Path to Java home to use when communicating with this cluster. If set to the string "$JAVA_HOME" 
* the environment variable present while starting splunk will be used
* Defaults to empty string

kerberos_principal = <string>
* Fully qualified name of the Kerberos principal to use when communicating with this cluster.
* Valid only if the cluster is kerberized and core-site.xml in the clusters directory for 
* this cluster ($SPLUNK_HOME/etc/apps/local/HadoopConnect/clusters/<host>_<ipc_port>/core-site.xml)
* is present and dictates that kerberos be used 
* Defaults to: empty string
* Not valid for local mounted Hadoop cluster.

kerberos_service_principal = <string>
* Fully qualified name of the Kerberos principal that the HDFS service is running as.
* This needs to be the same value as dfs.namenode.kerberos.principal in core-site.xml of your 
* Hadoop cluster.
* Defaults to: empty string
* Not valid for local mounted Hadoop cluster.

ha = <bool>
* Boolean number, 1 means the cluster is a High Availability cluster
* Defaults to: 0
* Not valid for local mounted Hadoop cluster.

hdfs_site = <string>
* The relevant Hight Availability configuration section of the hdfs_site.xml of the Hadoop cluster
* Defaults to: empty string
* Not valid for local mounted Hadoop cluster.

auth_to_local = <string>
* The kerberos principal names to local OS user names.
* Defaults to: empty string
* Not valid for local mounted Hadoop cluster.
