# name of the app
APP_NAME = 'HadoopConnect'

# default tmp_dir for local chunk file
DEFAULT_HDFS_TMP_DIR_NAME = 'hadoop_tmp'

# default tmp_dir for local chunk file for c++ dump command
DEFAULT_DUMP_TMP_DIR_NAME = 'dump'

# default roll file extension
# Add this extension back if it doesn't work
DEFAULT_ROLL_EXTENSION = '.hdfs'
# DEFAULT_ROLL_EXTENSION = ''

# local chunk file name before rolling
LOCAL_CHUNK_FILE_NAME = 'chunk.local'

# default local file roll size in MB
DEFAULT_ROLL_SIZE = 63

# default max allowable local disk usage size in MB
DEFAULT_MAX_LOCAL_SIZE = 1024
 
# default BufferedFile buffer size
DEFAULT_BUFFER_SIZE = 65536

# default percentage of unflushed data size when disk usage reach to limit
# flush_size = disk_usage - usage_limit * unfluch_percent / 100
# e.g. if usage limit is 1GB, unflush_percent is 80%, now assume we used 1.1GB disk space,
# then 1.1GB - 1GB * 80% = 0.3GB data should be flushed  
DEFAULT_UNFLUSH_PERCENT = 80

# invalid index
INVALID_INDEX = None

# tgt default renew ttl in seconds
DEFAULT_TGT_RENEW_TTL = 1800
