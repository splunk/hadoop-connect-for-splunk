
[hdfs://<hdfs resource>]
* <hdfs resource> is the HDFS resource that you want indexed.

sourcetype = <sourcetype>
* Optional.  Set to override the default value of "hdfs".

whitelist = <regular expression>
* If set, files from this path are monitored only if they match the specified regex.

blacklist = <regular expression>
* If set, files from this path are NOT monitored if they match the specified regex.
* If whitelist also matches, blacklist takes precedence.

