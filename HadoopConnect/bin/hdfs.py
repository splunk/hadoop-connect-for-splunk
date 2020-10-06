import sys, time, os, re, urlparse, md5, csv, gzip
import xml.dom.minidom, xml.sax.saxutils
import logging
import tarfile, gzip
from hadooputils import *
from constants import APP_NAME
import splunk.bundle

# set up logging suitable for splunkd consumption
logging.root
logging.root.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logging.root.addHandler(handler)

HDFS_PREFIX = "hdfs://"
SCHEME = """<scheme>
    <title>HDFS</title>
    <description>Get data from Hadoop's HDFS.</description>
    <streaming_mode>xml</streaming_mode>
    <use_external_validation>true</use_external_validation>

    <endpoint>
        <args>
            <arg name="name">
                <title>Resource name</title>
                <description>An HDFS resource name without the leading hdfs://.  For example, for the file at hdfs://hdfs-server:9000/dir/file.txt specify hdfs-server:9000/dir/file.txt.  You can also monitor files within a sub-directory (for example 'hdfs-server:9000/dir').</description>
            </arg>
            <arg name="whitelist">
                <title>White list regex</title>
                <description>If set, files from this path are monitored only if they match the specified regex.</description>
                <required_on_create>false</required_on_create>
            </arg>
            <arg name="blacklist">
                <title>Black list regex</title>
                <description>If set, files from this path are NOT monitored if they match the specified regex.  The black list takes precedence over the white list.</description>
                <required_on_create>false</required_on_create>
            </arg>
        </args>
    </endpoint>
</scheme>
"""

# prints XML error data to be consumed by Splunk and exits
def print_error_and_exit(s):
    print "<error><message>%s</message></error>" % xml.sax.saxutils.escape(s)
    sys.exit(2)

def validate_conf(config, key):
    if key not in config:
        raise Exception, "Invalid configuration received from Splunk: key '%s' is missing." % key

def get_child_node_data(config, parent_node, key_name):
    nodes = parent_node.getElementsByTagName(key_name)
    if len(nodes) > 0:
        child_node = parent_node.getElementsByTagName(key_name)[0]
        if child_node and child_node.firstChild and \
           child_node.firstChild.nodeType == child_node.firstChild.TEXT_NODE:
            config[key_name] = child_node.firstChild.data

# read XML configuration passed from splunkd
def get_config():
    config = {}

    try:
        # read everything from stdin
        config_str = sys.stdin.read()

        # parse the config XML
        doc = xml.dom.minidom.parseString(config_str)
        root = doc.documentElement
        conf_node = root.getElementsByTagName("configuration")[0]
        if conf_node:
            stanza = conf_node.getElementsByTagName("stanza")[0]
            if stanza:
                stanza_name = stanza.getAttribute("name")
                if stanza_name:
                    logging.debug("XML: found stanza=\"%s\"" % stanza_name)
                    config["name"] = stanza_name

                    params = stanza.getElementsByTagName("param")
                    for param in params:
                        param_name = param.getAttribute("name")
                        logging.debug("XML: found param=\"%s\"" % param_name)
                        if param_name and param.firstChild and \
                           param.firstChild.nodeType == param.firstChild.TEXT_NODE:
                            data = param.firstChild.data
                            config[param_name] = data
                            logging.debug("XML: %s=\"%s\"" % (param_name, data))

        get_child_node_data(config, root, "checkpoint_dir")
        get_child_node_data(config, root, "session_key")

        if not config:
            raise Exception, "Invalid configuration received from Splunk."

        # just some validation: make sure these keys are present (required)
        validate_conf(config, "name")
        validate_conf(config, "checkpoint_dir")
        validate_conf(config, "session_key")
    except Exception, e:
        raise Exception, "Error getting Splunk configuration via STDIN: %s" % str(e)

    return config

def init_stream():
    sys.stdout.write("<stream>")

def fini_stream():
    sys.stdout.write("</stream>")

def send_data(source, buf):
    sys.stdout.write("<event unbroken=\"1\"><data>")
    sys.stdout.write(xml.sax.saxutils.escape(buf))
    sys.stdout.write("</data>\n<source>")
    sys.stdout.write(xml.sax.saxutils.escape(source))
    sys.stdout.write("</source></event>\n")
    sys.stdout.flush()

def send_done_key(source):
    sys.stdout.write("<event unbroken=\"1\"><source>")
    sys.stdout.write(xml.sax.saxutils.escape(source))
    sys.stdout.write("</source><done/></event>\n")
    sys.stdout.flush()

def get_encoded_csv_file_path(checkpoint_dir, conf_stanza):
    # encode the URI (simply to make the file name recognizable)
    name = ""
    for i in range(len(conf_stanza)):
        if conf_stanza[i].isalnum():
            name += conf_stanza[i]
        else:
            name += "_"

    # trim the length in case the name is too long
    name = name[:30]

    # MD5 the URL
    m = md5.new()
    m.update(conf_stanza)
    name += "_" + m.hexdigest() + ".csv.gz"

    return os.path.join(checkpoint_dir, name)

# number of expected columns in the CSV file
Checkpointer_COLS = 3
# in seconds, what is the minimum time before we sync checkpoints to disk
Checkpointer_min_sync_interval_sec = 10

class Checkpointer:
    class Item:
        def __init__(self, total_bytes, completed):
            self.total_bytes = total_bytes
            self.completed = completed

        def get_total_bytes(self):
            return self.total_bytes

        def is_completed(self):
            return self.completed

    def __init__(self, checkpoint_dir, conf_stanza):
        self.chkpnt_file_name = get_encoded_csv_file_path(checkpoint_dir, conf_stanza)
        self.chkpnt_dict = {}
        # load checkpoint into memory
        self._load()
        self.last_chkpnt_time = 0.0

    # returns a Checkpointer.Item object
    def load_item(self, item):
        return self.chkpnt_dict.get(item, Checkpointer.Item(0, False))

    # write a checkpoint item
    def save_item(self, item, total_bytes, completed = False):
        self.chkpnt_dict[item] = Checkpointer.Item(total_bytes, completed)

        # sync to disk immediately on "completed"; otherwise, sync to disk
        # no more frequently than the minimum interval
        if completed or time.time() > self.last_chkpnt_time + Checkpointer_min_sync_interval_sec:
            self.sync()

    # syncs to disk all checkpoint info immediately
    def sync(self):
        tmp_file = self.chkpnt_file_name + ".tmp"
        f = None
        try:
            f = gzip.open(tmp_file, "wb")
        except Exception, e:
            logging.error("Unable to open file='%s' for writing: %s" % \
                self.chkpnt_file_name, str(e))

        writer = csv.writer(f)

        for key, item in self.chkpnt_dict.iteritems():
            writer.writerow([key, str(item.total_bytes), str(item.completed)])

        f.close()
        os.rename(tmp_file, self.chkpnt_file_name)
        self.last_chkpnt_time = time.time()

    def _load(self):
        f = self._open_checkpoint_file("rb")
        if f is None:
            return

        reader = csv.reader(f)
        line = 1
        for row in reader:
            if len(row) >= Checkpointer_COLS:
                # the first column of the row is the item name;
                # store it in the dict
                try:
                    b = None
                    if row[2].lower() == "true":
                        b = True
                    elif row[2].lower() == "false":
                        b = False
                    else:
                        # invalid value
                        raise Exception

                    self.chkpnt_dict[row[0]] = Checkpointer.Item(int(row[1]), b)
                except:
                    logging.error("The CSV file='%s' line=%d appears to be corrupt." % \
                        (self.chkpnt_file_name, line))
                    raise
            else:
                logging.warn("The CSV file='%s' line=%d contains less than %d rows." % \
                    (self.chkpnt_file_name, line, Checkpointer_COLS))
            line += 1

        f.close()

    def _open_checkpoint_file(self, mode):
        if not os.path.exists(self.chkpnt_file_name):
            return None
        # try to open this file
        f = None
        try:
            f = gzip.open(self.chkpnt_file_name, mode)
        except Exception, e:
            logging.error("Error opening '%s': %s" % (self.chkpnt_file_name, str(e)))
            return None
        return f

# should we process this URI based on regex settings; blacklist takes precedence
def should_process(config, file_uri):
    if "blacklist" in config and re.search(config["blacklist"], file_uri):
        logging.debug("Will not process uri='%s' because of blacklist regex='%s'" %
            (file_uri, config["blacklist"]))
        return False
    if "whitelist" in config and not re.search(config["whitelist"], file_uri):
        logging.debug("Will not process uri='%s' because of whitelist regex='%s'" %
            (file_uri, config["whitelist"]))
        return False
    logging.debug("Will process uri='%s' (whitelist='%s' blacklist='%s')" %
        (file_uri, config.get("whitelist", ""), config.get("blacklist", "")))
    return True

# extract the host:port part only from the HDFS URI
def get_hdfs_host_port(uri):
    return urlparse.urlparse(uri).netloc

# makes sure that the regexes compile correctly; if not, they are removed
# from the config object
def validate_regex(config, conf_key):
    if conf_key in config:
        try:
            re.compile(config[conf_key])
        except:
            logging.error("%s regex '%s' under stanza '%s' is not valid.  Ignoring." % \
                (conf_key, config[conf_key], config["name"]))
            del config[conf_key]

def run():

    config = get_config()
    hdfs_uri = config["name"]
    session_key = config["session_key"]

    # make sure the regexes are valid
    validate_regex(config, "whitelist")
    validate_regex(config, "blacklist")

    HadoopEnvManager.init(APP_NAME, 'nobody', session_key)

    logging.debug("Servicing uri=%s" % hdfs_uri)
    hj = HadoopEnvManager.getCliJob(hdfs_uri)
    if not hj.exists(hdfs_uri):
        logging.error("HDFS resource=%s does not exist." % hdfs_uri)
        return

    chk = Checkpointer(config["checkpoint_dir"], hdfs_uri)

    # see if the hdfs uri refers to a directory of file only; prints error
    # end exits if the hfds URI is not valid or cannot be accessed
    if(hj.exists(hdfs_uri)):
        init_stream()

        # extract the host:port part only from the HDFS URI
        hdfs_host_port = get_hdfs_host_port(hdfs_uri)

        while True:
            logging.debug("Checking for files in directory %s" % hdfs_uri)

            hlister = HDFSDirLister()
            for l in hlister.lsr(hdfs_uri):

                # can only process files
                if l.isdir():
                    continue

                # check the file's uri against the white/blacklist
                file_uri = "hdfs://" + hdfs_host_port + l.path
                if not should_process(config, file_uri):
                    continue
                if not chk.load_item(file_uri).is_completed():
                    logging.debug("Requesting uri=%s" % file_uri)
                    total_bytes = process_file_uri(file_uri)
                    chk.save_item(file_uri, total_bytes, completed = True)

            # check every 60 seconds for new entries
            time.sleep(60)
        fini_stream()
    else:
        if not chk.load_item(hdfs_uri).is_completed():
            # there is no checkpoint for this URL: process
            init_stream()
            total_bytes = process_file_uri(hdfs_uri)
            fini_stream()
            chk.save_item(hdfs_uri, total_bytes, completed = True)
        else:
            logging.info("URL %s already processed.  Skipping." % hdfs_uri)

def process_file_uri(hdfs_uri):

    hj = HadoopEnvManager.getCliJob(hdfs_uri)

    #ERP-275 handle tar/tgz/tar.gz/tar.bz2 
    if need_data_translator(hdfs_uri):
       hj.cat(hdfs_uri)
       translator = get_data_translator(hdfs_uri, hj.process.stdout)
    else: # anything else goes  thru text
       hj.text(hdfs_uri)
       translator = FileObjTranslator(hdfs_uri, hj.process.stdout)

    cur_src = ""
    buf = translator.read()
    bytes_read = len(buf)
    while len(buf) > 0:
        if cur_src and translator.source() != cur_src:
            send_done_key(cur_src)
        cur_src = translator.source()

        send_data(translator.source(), buf)

        buf = translator.read()
        bytes_read += len(buf)

    if cur_src:
        send_done_key(cur_src)

    hj.process.wait() # ensure we don't leave zombie processes around
    translator.close()
    sys.stdout.flush()

    logging.info("Done reading uri=\"%s\". Read bytes=%d" % (hdfs_uri, bytes_read))
    return bytes_read

# Handles file reading from tar archives.  From the tarfile module:
# fileobj must support: read(), readline(), readlines(), seek() and tell().
class TarTranslator():
    def __init__(self, src, tar):
        self.tar = tar
        self.member = self.tar.next()
        self.member_f = self.tar.extractfile(self.member)
        self.translator = None
        self.base_source = src
        if self.member:
            self.src = self.base_source + ":" + self.member.name
            if self.member_f:
                self.translator = get_data_translator(self.src, self.member_f)

    def read(self, sz = 8192):
        while True:
            while self.member and self.member_f is None:
                self.member = self.tar.next()
                if self.member:
                    self.member_f = self.tar.extractfile(self.member)
                    self.src = self.base_source + ":" + self.member.name
                    self.translator = get_data_translator(self.src, self.member_f)

            if not self.member:
                return "" # done

            buf = self.translator.read(sz)
            if len(buf) > 0:
                return buf
            self.member_f = None
            self.translator = None

    def close(self):
        self.tar.close()

    def source(self):
        return self.src

class FileObjTranslator():
    def __init__(self, src, fileobj):
        self.src = src
        self.fileobj = fileobj

    def read(self, sz = 8192):
        return self.fileobj.read(sz)

    def close(self):
        return self.fileobj.close()

    def source(self):
        return self.src

class GzipFileTranslator():
    def __init__(self, src, fileobj):
        self.src = src
        self.fileobj = fileobj

    def read(self, sz = 8192):
        return self.fileobj.read(sz)

    def close(self):
        return self.fileobj.close()

    def source(self):
        return self.src

def need_data_translator(url):
    return url.endswith("tar") or url.endswith("tar.gz") or url.endswith("tar.bz2") or url.endswith("tgz")  

def get_data_translator(url, fileobj):
    if url.endswith(".tar"):
        return TarTranslator(url, tarfile.open(None, "r|", fileobj))
    elif url.endswith(".tar.gz") or url.endswith(".tgz"):
        return TarTranslator(url, tarfile.open(None, "r|gz", fileobj))
    elif url.endswith(".tar.bz2"):
        return TarTranslator(url, tarfile.open(None, "r|bz2", fileobj))
    elif url.endswith(".gz"):
        # it's lame that gzip.GzipFile requires tell() and seek(), and our
        # "fileobj" does not supply these; wrap this with the object that is
        # used by the tarfile module
        return GzipFileTranslator(url, tarfile._Stream("", "r", "gz", fileobj, tarfile.RECORDSIZE))
    else:
        return FileObjTranslator(url, fileobj)

def do_scheme():
    print SCHEME

def get_validation_data():
    val_data = {}

    # read everything from stdin
    val_str = sys.stdin.read()

    # parse the validation XML
    doc = xml.dom.minidom.parseString(val_str)
    root = doc.documentElement

    item_node = root.getElementsByTagName("item")[0]
    if item_node:

        name = item_node.getAttribute("name")
        val_data["name"] = name

        params_node = item_node.getElementsByTagName("param")
        for param in params_node:
            name = param.getAttribute("name")
            logging.debug("Found param %s" % name)
            if name and param.firstChild and \
               param.firstChild.nodeType == param.firstChild.TEXT_NODE:
                val_data[name] = param.firstChild.data

    get_child_node_data(val_data, root, "session_key")

    return val_data

# Trims the scheme part of a URI: "hdfs://host:port/sadsad" -> "host:port/sadsad"
def _trim_uri_scheme(uri):
    loc = uri.find("://")
    if loc > -1:
        # extract the string past the "://"
        return uri[loc + 3:]
    return uri  # not a URI with a scheme: return same

# Attempt to normalize a path to a form that makes it easier to compare.
# For example, this:
#   hdfs://server-host:9000/2222222/../33333333//444444/55555/
# becomes:
#   server-host:9000/33333333/444444/55555
def _normalize_path(path_str):
    path_str = "/" + _trim_uri_scheme(path_str)
    return os.path.relpath(path_str, "/")

# are first_uri, second_uri sub/super-sets of each other
def is_sub_or_superset_uri(first_uri, second_uri):
    # trim the scheme part of the URI's
    first  = _normalize_path(first_uri)
    second = _normalize_path(second_uri)

    # same length: simply string-compare them
    if len(first) == len(second):
        return first == second

    # see which one is shorter/longer
    if len(first) > len(second):
        shorter = second
        longer = first
    else:
        shorter = first
        longer = second

    # if shorter is a subset of longer and shorter appears to be a
    # directory path or root, it is a subset
    if longer.startswith(shorter) and longer[len(shorter)] == "/":
        return True

    return False

# Make sure that adding hdfs_uri to the config won't cause same files to be
# indexed more than ones; may throw Exception
def validate_uri_for_duplicate_data(session_key, hdfs_uri):
    inputs = splunk.bundle.getConf("inputs", session_key)

    if hdfs_uri in inputs:
        # if the item already exists in the config (i.e., this is an edit),
        # don't worry about validating (only validate upon creating new items)
        return

    # so, it's an edit
    for stanza in inputs.keys():
        if stanza.startswith(HDFS_PREFIX) and \
           is_sub_or_superset_uri(hdfs_uri, stanza):
            print_error_and_exit("Unable to save stanza \"%s\" because " \
                "it is a sub/super-set of the existing input stanza \"%s\", " \
                "and adding it may cause duplicate data to be indexed." % \
                (hdfs_uri, stanza))

def validate_arguments():
    val_data = get_validation_data()
    name = val_data["name"]
    if name.startswith(HDFS_PREFIX):
        print_error_and_exit("When putting the resource name, please omit the "
            "\"%s\" prefix." % HDFS_PREFIX)
    hdfs_uri = HDFS_PREFIX + name

    try:
        session_key = val_data["session_key"]

        # make sure hdfs_uri isn't a sub/super-set of any stanza in the config
        # file to avoid the possibility of indexing duplicate data
        validate_uri_for_duplicate_data(session_key, hdfs_uri)

        HadoopEnvManager.init(APP_NAME, 'nobody', session_key)
        hj = HadoopEnvManager.getCliJob(hdfs_uri)
        if not hj.exists(hdfs_uri):
            print_error_and_exit("The resource cannot be found.")
    except Exception, e:
        print_error_and_exit("Unable to validate resource: %s" % str(e))

def usage():
    print "usage: %s [--scheme|--validate-arguments]"
    sys.exit(2)

def test_Checkpointer_verify_item(i, is_compl, total_bytes):
    assert i.is_completed() == is_compl
    assert i.get_total_bytes() == total_bytes

def test_Checkpointer_generate_string(prefix, stanza, item, completed_act, completed_exp, total_bytes_act, total_bytes_exp):
    return "%s stanza=%s item=%s completed-actual=%s completed-expected=%s total_bytes-actual=%d total_bytes-expected=%d" % \
        (prefix, stanza, item, completed_act, completed_exp, total_bytes_act, total_bytes_exp)

def test_Checkpointer_verify(stanza, item, is_compl, total_bytes):
    chk = Checkpointer(".", stanza)
    i = chk.load_item(item)
    try:
        test_Checkpointer_verify_item(i, is_compl, total_bytes)
    except AssertionError, e:
        logging.error(test_Checkpointer_generate_string("ASSERT FAIL:", stanza, \
            item, i.is_completed(), is_compl, i.get_total_bytes(), total_bytes))
        raise
    logging.info(test_Checkpointer_generate_string("Success:", stanza, \
        item, i.is_completed(), is_compl, i.get_total_bytes(), total_bytes))

def test_Checkpointer():
    stanza = "hdfs://my-server:21312/a/path"

    chkpnt_file = get_encoded_csv_file_path(".", stanza)
    try:
        logging.info("Removing '%s'..." % chkpnt_file)
        os.unlink(chkpnt_file)
    except:
        pass

    chk = Checkpointer(".", stanza)

    item1 = "hdfs://my-server:21312/a/path/111"
    i = chk.load_item(item1)
    test_Checkpointer_verify_item(i, False, 0)

    chk.save_item(item1, 111, True)

    test_Checkpointer_verify(stanza, item1, True, 111)

    item2 = "hdfs://my-server:21312/a/path/222"
    chk.save_item(item2, 222, True)

    item3 = "hdfs://my-server:21312/a/path/333"
    chk.save_item(item3, 333)
    chk.sync()

    item4 = "hdfs://my-server:21312/a/path/444"
    chk.save_item(item4, 0)
    chk.sync()
    test_Checkpointer_verify(stanza, item4, False, 0)
    chk.save_item(item4, 444, True)

    # try some odd characters
    item5 = "hdfs://my-server:21312/a/path/\" dslfkj ,,,'\"asd"
    chk.save_item(item5, 555, True)

    test_Checkpointer_verify(stanza, item1, True,  111)
    test_Checkpointer_verify(stanza, item2, True,  222)
    test_Checkpointer_verify(stanza, item3, False, 333)
    test_Checkpointer_verify(stanza, item4, True, 444)
    test_Checkpointer_verify(stanza, item5, True, 555)

def test_whiteblacklist_verify(path, is_true, w_regex = "", b_regex = ""):
    config = {}
    if w_regex:
        config["whitelist"] = w_regex
    if b_regex:
        config["blacklist"] = b_regex

    logging.info("Checking path=%s is_true=%s w_regex='%s' b_regex='%s'..." % \
        (path, str(is_true), w_regex, b_regex))
    assert should_process(config, path) == is_true

def test_whiteblacklist():
    test_whiteblacklist_verify("qweqwe", True)
    test_whiteblacklist_verify("qweqwe", True,  w_regex = "eq")
    test_whiteblacklist_verify("blahbl", False, w_regex = "eq")
    test_whiteblacklist_verify("qweqwe", False,                 b_regex = "eq")
    test_whiteblacklist_verify("qweqwe", False, w_regex = "eq", b_regex = "eq")

def test_duplicate_validation_verify(base_uri, test_against):
    logging.info("Verifying against \"%s\":" % base_uri)
    for (expected_value, uri) in test_against:
        logging.info("    Expecting pass=%5s, uri=\"%s\"" % (str(expected_value), uri))
        # passes if *not* a sub/super-set; that's why we check "!= expected_value"
        assert is_sub_or_superset_uri(base_uri, uri) != expected_value

def test_duplicate_validation():
    # first, test _normalize_path
    logging.info("Testing _normalize_path...")
    assert _normalize_path("hdfs://server-host:9000") == "server-host:9000"
    assert _normalize_path("hdfs://server-host:9000/") == "server-host:9000"
    assert _normalize_path("hdfs://server-host:9000/file") == "server-host:9000/file"
    assert _normalize_path("hdfs://server-host:9000/dir/file") == "server-host:9000/dir/file"
    assert _normalize_path("hdfs://server-host:9000/2222222/../33333333//444444/55555/") == \
                           "server-host:9000/33333333/444444/55555"
    assert _normalize_path("hdfs://server-host:9000/2222222/../33333333//444444/55555") == \
                           "server-host:9000/33333333/444444/55555"
    logging.info("Testing _normalize_path finished.")

    logging.info("Testing if validation passes correctly...")
    base_uri = "hdfs://sveserv52-vm1.sv.splunk.com:9000/home/foo/"
    test_against = (
        (True,  "hdfs://sveserv52-vm1.sv.splunk.com:9000/home/bar/"),
        (True,  "hdfs://sveserv52-vm1.sv.splunk.com:9000/home/bar"),
        (False, "hdfs://sveserv52-vm1.sv.splunk.com:9000/home/foo"),
        (True,  "hdfs://sveserv52-vm1.sv.splunk.com:9000/home/foo_file"),
        (False, "hdfs://sveserv52-vm1.sv.splunk.com:9000/home/foo/file"),
        (False, "hdfs://sveserv52-vm1.sv.splunk.com:9000/"),
        (False, "hdfs://sveserv52-vm1.sv.splunk.com:9000"),
        (True,  "hdfs://another-host:9000/home/"),
    )
    test_duplicate_validation_verify(base_uri, test_against)
    # try the same base_uri minus the tailing slash
    base_uri = "hdfs://sveserv52-vm1.sv.splunk.com:9000/home/foo"
    test_duplicate_validation_verify(base_uri, test_against)

    base_uri = "hdfs://sveserv52-vm1.sv.splunk.com:9000/home/foo/dir1/dir2/file"
    test_against = (
        (True,  "hdfs://sveserv52-vm1.sv.splunk.com:9000/home/bar/"),
        (True,  "hdfs://sveserv52-vm1.sv.splunk.com:9000/home/foo_file"),
        (False, "hdfs://sveserv52-vm1.sv.splunk.com:9000/home/foo"),
        (False, "hdfs://sveserv52-vm1.sv.splunk.com:9000/home/foo/"),
        (False, "hdfs://sveserv52-vm1.sv.splunk.com:9000"),
        (True,  "hdfs://another-host:9000/home/"),
    )
    test_duplicate_validation_verify(base_uri, test_against)

    base_uri = "hdfs://sveserv52-vm1.sv.splunk.com:9000/"
    test_against = (
        (True,  "hdfs://another-host:9000/home/bar/"),
        (False, "hdfs://sveserv52-vm1.sv.splunk.com:9000/home/foo_file"),
        (False, "hdfs://sveserv52-vm1.sv.splunk.com:9000/home/foo"),
        (False, "hdfs://sveserv52-vm1.sv.splunk.com:9000/home/foo/"),
        (False, "hdfs://sveserv52-vm1.sv.splunk.com:9000"),
        (False, "hdfs://sveserv52-vm1.sv.splunk.com:9000/"),
    )
    test_duplicate_validation_verify(base_uri, test_against)
    # try the same base_uri minus the tailing slash
    base_uri = "hdfs://sveserv52-vm1.sv.splunk.com:9000"
    test_duplicate_validation_verify(base_uri, test_against)

def test():
    test_Checkpointer()
    test_whiteblacklist()
    test_duplicate_validation()
    logging.info("DONE.")

if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1] == "--scheme":
            do_scheme()
        elif sys.argv[1] == "--validate-arguments":
            # do external validation
            validate_arguments()
        elif sys.argv[1] == "--test":
            # unit tests
            test()
        else:
            usage()
    else:
        # just request data from HDFS
        run()

    sys.exit(0)

