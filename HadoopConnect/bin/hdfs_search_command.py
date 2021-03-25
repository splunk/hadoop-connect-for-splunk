"""
External search command for reading data from HDFS. 

Usage:
  |hdfs <directive> (<hdfs-path>)+ <directive-options>| ... <other splunk commands here> ...
  directives:
  ls    - list the contents of the given hdfs path(s)
  lsr   - recursively list the contents of the given hdfs path(s)
  read  - read the contents of a given file
  rm    - remove the given path
  rmr   - recursively remove the given path (dir) 
   
"""

import csv,sys,time,os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from splunklib.searchcommands import GeneratingCommand, Option, validators
from splunklib.searchcommands import Configuration, dispatch
from splunklib import client
import splunk.entity as entity

import splunk.util, splunk.Intersplunk as isp
from urllib.parse import urlparse
from hadooputils import * 
from constants import *
from util import *
import inspect
try:
    from cStringIO import StringIO
except:
    from io import StringIO

def csv_escape(s):
    return str(s).replace('"', '""')


@Configuration()
class HDFSSearchCommand(GeneratingCommand):

    delim = Option(
        doc='''
        **Syntax:** **delim=<delimiter>****
        **Description:** applicable to: read. Delimiter character to split the read lines into fields''',
        require=False)

    fields = Option(
        doc='''
        **Syntax:** **fields=<fields>****
        **Description:** applicable to: read. Comma delimited list of field names to assign to the segments created by the delimiter splitting 
        ''',
        require=False)    


    def __init__(self, outputfile=sys.stdout):
        super(HDFSSearchCommand, self).__init__()
        self.directives = {'ls': self.handle_ls, 'lsr' : self.handle_lsr, 'read': self.handle_read} #, 'rmr': self.handle_rmr, 'rm': self.handle_rm}
        self.raiseAll = True           
        self.messages = {}
        self.keywords = []
        self.settings = {}
        self.info     = SearchResultsInfo() # we'll read this if splunk passes it along
        self.output_chunk_size = 64*1024
        self.outputfile = outputfile
        self.clusters   = {}
    
    def _addMessage(self, msgType, msg):
        #msg = "HCERR1007: Error in 'hdfs' command: " + msg
        msg = "Error in 'hdfs' command: " + msg
        if msgType in self.messages:
           self.messages[msgType].append(msg)
        else:
           self.messages[msgType] = [msg]
           
        self.info.addMessage(msgType, msg)

        if msgType == 'WARN':
           logger.warn(msg)
        elif msgType == 'ERROR':
           logger.error(msg)

    def _validateURI(self, uri):
        if len(self.clusters) == 0:
            msg = None
            #1. hit the clusters endpoint 
            import splunk.entity as entity
            logger.error("Namespace: {}".format(self.namespace))
            logger.error("Owner: {}".format(self.owner))
            logger.error("sessionKey: {}".format(self.sessionKey))
            logger.error("Splunk Debug is here first")
            try:
                self.clusters = entity.getEntities('admin/conf-clusters', namespace=self.namespace, owner=self.owner, sessionKey=self.sessionKey)
                logger.error(self.clusters)
            except Exception as e:
                logger.exception('Failed to get entities admin/conf-clusters')
                msg = str(e)
                if isinstance(e, splunk.RESTException):
                    msg = e.get_extended_message_text()
            if msg != None:
                raise HcException(HCERR2002, {'entity_path':'admin/conf-clusters', 'search':'', 'uri':'', 'error':msg})
        
        found = False
        for name, cluster in self.clusters.items():
            cluster_uri = cluster.get('uri', '')
            if cluster_uri == '':
               continue
            if not uri.startswith(cluster_uri):
               continue
            found = True
            break

        if not found:
           raise HcException(HCERR1005, {'uri':uri})       
    
    def createHDFSDirLister(self):
        return HDFSDirLister(self.raiseAll)
    
    def _list_files (self, base_path, file_re):
        result = []
        hdfs_uri = ''
	    
        # find hdfs uri
        if base_path.startswith('hdfs://'):
            fs = base_path[8:].find('/')
            if fs > 0:
                hdfs_uri = base_path[:8+fs] 

        hls = self.createHDFSDirLister()
        # process the output of lsr - add absolute paths to results
        for f in hls.lsr(base_path):
            if f.isdir(): 
                continue
            if not file_re.search(f.path):
                continue 
            result.append(hdfs_uri + f.path)

        # we get here iff self.raiseAll == False and there was an error
        if hls.error:
            self._addMessage('WARN', hls.error)

        return result

    def streamResults(self, body_str, outputfile=sys.stdout):
        header_str = ''
        header_io = StringIO()
        #logger.error("[---] Splunk Debug is printing header_io: {}".format(header_io))
        self.info.serializeTo(header_io)
        header_str = header_io.getvalue()
        #logger.error("[---] Splunk Debug is printing header_str: {}".format(header_str))
        #logger.error("[---] Splunk Debug is printing body_str: {}".format(body_str))
        outputfile.write("splunk %s,%u,%u\n" % ("8.0", len(header_str), len(body_str)))
        outputfile.write(header_str)
        outputfile.write(body_str)
        logger.error("[---] Splunk Debug is done printing outputfile: {}".format(outputfile))

    def createHadoopCliJob(self, path):
        return HadoopEnvManager.getCliJob(path)
        
    
    def _read_hdfs_file(self, absfile, delim=None, fields=None):
        logger.error("[---] Splunk Debug is entering read hdfs file")
        body_io = StringIO()
        w = csv.writer(body_io)
        result_count = 0

        hj = self.createHadoopCliJob(absfile)
      
        # make sure path exists and it's a file and no wildcard
        if absfile.find('*') >= 0:
           raise HcException(HCERR1008, {'path':absfile})
        if not hj.exists(absfile):
           raise HcException(HCERR0016, {'path':absfile})
        if hj.isDir(absfile):
           raise HcException(HCERR1006, {'dir':absfile})
          
        hj.text(absfile) # this will read .gz files as well
	    
        cols = ['_raw', 'source', 'host'] 
        need_header = True
        
        delim = None if (delim != None and len(delim) == 0) or fields == None else delim
        if delim != None:
           cols.extend(fields)

        p = urlparse(absfile) 
        
        escaped_path = csv_escape(p.path)
        escaped_host = csv_escape(p.netloc)	 
        temp_dict = {'source':escaped_path, 'host':escaped_host}
        results = []
        logger.error("[---] New Splunk Debug: reached here\n Temp Dict: {}".format(temp_dict))
        for line in hj.getStdout():  
            # if need_header:
            #     w.writerow(cols)
            #     need_header = False
            
            line = line.strip() 
            if len(line) == 0:
               continue            

            # csv escape and write each row individually
            temp_dict['_raw'] = csv_escape(line)
            # body_io.write('"') 
            # body_io.write(csv_escape(line))
            # body_io.write('","') 
            # body_io.write(escaped_path) 
            # body_io.write('","') 
            # body_io.write(escaped_host) 
        
            # TODO: Support delimiter and fields
            # try to parse (just split) the line based on delimiter 
            if delim != None:
               parts = line.split(delim)
               # make sure parts contains at least as many entries as fields
               tmp = len(parts)
               while tmp < len(fields):
                   parts.append('')
                   tmp +=1
                
               for p in parts[:len(fields)]:
                  body_io.write('","')
                  body_io.write(csv_escape(p))     

            body_io.write('"\n')
            logger.error("[---] Splunk Debug - Results size: {}\n Length of Results: {}, Printing the temp dictionaries: {}".format(sys.getsizeof(results), len(results), temp_dict))
            results.append(temp_dict)
            temp_dict = {'source':escaped_path, 'host':escaped_host} 
            result_count += 1
            
            # check to see if we need to flush the buffer out
            # if body_io.tell() >= self.output_chunk_size:
            #     logger.error("[---] Splunk Debug Body IO is more than the output_chunk_size. Result count rn: {}".format(result_count))
            #     logger.error("Body to be written\n*********************************\n {}".format(body_io.getvalue()))
            #     self.streamResults(body_io.getvalue(), self.outputfile)
            #     body_io.truncate(0)
            #     result_count = 0
            #     need_header = True

            
            if sys.getsizeof(results) >= self.output_chunk_size:
                logger.error("[---] Splunk Debug Body IO is more than the output_chunk_size. Result count rn: {}".format(result_count))
                # self.streamResults(body_io.getvalue(), self.outputfile)
                yield results
                # yield body_io.getvalue()
                results = []
                result_count = 0
                # need_header = True
        # flush any remaining result
        # if result_count > 0:
        #     self.streamResults(body_io.getvalue(), self.outputfile)
        if result_count>0:
            yield results
        # ensures we don't leave defunct processes around
        hj.wait(True)

    def _list_dir(self, absfile, recursive=True):
        logger.error("[---] Splunk Debug Inside list dir!")
        body_io = StringIO()
        w = csv.writer(body_io)
        result_count = 0

        hls = self.createHDFSDirLister()

        #TODO: handle other data formats here (csv, json, etc)
        cols = ['type', 'acl', 'replication', 'size', 'user', 'group', 'date', 'time', 'path', 'source', '_time', '_raw']
        need_header = True

        # CLI complains when doing hadoop fs -ls hdfs://namenode:8020 (without a trailing /)
        if(absfile.startswith('hdfs://') and absfile.count('/') == 2 and not absfile.endswith('/')):
           absfile += '/' 
        logger.error("[---] Splunk Debug is doing hls lsr")
        gen = hls.lsr(absfile) if recursive else  hls.ls(absfile)
        logger.error("[---]Splunk Debug is Now printing gen: {}".format(gen))
        results = []
        for f in gen:
            # if need_header:
            #     w.writerow(cols)
            #     need_header = False
            #     #2009-10-05 23:23"
            #     ft = time.time()
            #     try:
            #         ft = time.mktime(time.strptime(f.date + ' ' +  f.time, '%Y-%m-%d %H:%M'))
            #     except:
            #         pass
            #     raw = f.path
            # rowa = {}
            # logger.error("Type of f: {}".format(type(f)))
            rowa = {key:value for key, value in f.__dict__.items() if not key.startswith('__') and not callable(key)}
            ft = time.time()
            try:
                ft = time.mktime(time.strptime(f.date + ' ' +  f.time, '%Y-%m-%d %H:%M'))
                rowa["_time"] = ft
            except:
                pass
            try:
                raw = f.path
                rowa["_raw"] = raw
            except:
                pass    
            rowa["source"] = absfile
            rowa["type"] = "dir" if f.isdir() else "file"
            results.append(rowa)
            # r = ["dir" if f.isdir() else "file", f.acl, f.replication, f.size, f.user, f.group, f.date, f.time, f.path, absfile, ft, raw]
            # logger.error("[---] Splunk Debug is Printing the rowa: {}".format(rowa))
            # logger.error("[---] Splunk Debug is Printing the row: {}".format(f))
            # logger.error("[---] Splunk Debug is Printing the row r: {}".format(r))            
            # w.writerow(r)
            result_count += 1

            # check to see if we need to flush the buffer out
            # if body_io.tell() >= self.output_chunk_size:
            #     logger.error("[---] Splunk Debug is calling Stream Results to check if we need to flush the buffer")
            #     self.streamResults(body_io.getvalue(), self.outputfile)
            #     body_io.truncate(0)
            #     result_count = 0
            #     need_header = True
        logger.error("[---] Splunk Debug is flushing any remaining result. Result Count: {}".format(result_count))
	    # flush any remaining result
        if result_count > 0:
            logger.error("[---] Splunk Debug should be Streaming results since result count is greater than 0")
            yield results
            # return body_io.getvalue()
            # self.streamResults(body_io.getvalue(), self.outputfile)
        logger.error("[---] Splunk Debug finished flushing the remaining, now checking for errors")
        logger.error("[---] Splunk Debug found some hls error: {}".format(hls.error))
        if hls.error and not self.raiseAll:
            logger.error("[---] Splunk Debug is setting up a warning with the hls error")
            self._addMessage('WARN', hls.error)

    def handle_ls(self):
        logger.error("Printing the keywords: {}".format(self.keywords))
        result = {}
        for p in self.keywords:
            result[p] = self._list_dir(unquote(p, unescape=True), False)    
        return result

    def handle_lsr(self):
        logger.error("Printing the keywords: {}".format(self.keywords))
        result = {}
        for p in self.keywords:
            result[p] = self._list_dir(unquote(p, unescape=True), True)
        logger.error("[---] Splunk Debug has Finished checking the directories")
        return result

    def handle_rmr(self):
        for p in self.keywords:
            hj = self.createHadoopCliJob(p)
            hj.rmr(p)

    def handle_rm(self):
        for p in self.keywords:
            hj = self.createHadoopCliJob(p)
            hj.rm(p)

    def handle_read(self):
        params = ('delim', 'fields')
        logger.error("[---] Splunk Debug is printing self.delim: {}".format(self.delim))
        logger.error("[---] Splunk Debug is printing self.fields: {}".format(self.fields))
        delim=''
        fields = []
        try:
            delim = unquote(self.delim).decode('string_escape')  # converts \t -> tab
        except:
            logger.info("No delimiter present")    
        try:
            fields = [x.strip() for x in unquote(self.fields).split(",")]
        except:
            logger.info("No fields present")    
        
        logger.error("[---] Splunk Debug is printing delim: {}".format(delim))
        logger.error("[---] Splunk Debug is printing fields: {}".format(fields))
        logger.error("[---] Splunk Debug is printing self keywords: {}".format(self.keywords))
        temp_res = {}
        for p in self.keywords:
            temp_res[p] = []
        for p in self.keywords:
            try:
                results = self._read_hdfs_file(unquote(p, unescape=True), delim, fields)
                logger.error("[---] Splunk Debug Results from read hdfs file: {}".format(results))
                # for event in results:
                #     logger.error("[---] Splunk Debug is printing the dictionaries: {}".format(event))
                temp_res[unquote(p, unescape=True)].append(results)
            except Exception as e:
                 if self.raiseAll:
                    raise
                 msg = toUserFriendlyErrMsg(e)
                 self._addMessage('WARN', msg)
        logger.error("[---] Splunk Debug Temp Res: {}".format(temp_res))
        return temp_res                                 

    def _main_impl(self):
        fields = ('delim', 'fields')
        if len(self.keywords) == 0:
            raise HcException(HCERR0505)

        d = self.keywords.pop(0)
        if not d in self.directives:
            raise HcException(HCERR0506, {'directive':d, 'accepted_values':','.join(self.directives.keys())}) 
        
        if len(self.keywords) == 0:
            raise HcException(HCERR0501, {'argument':'uri'})
        for k in self.keywords:
            logger.error("Splunk Debug Printing keywords: {}".format(k))
            if k.startswith(fields):
                continue
            self._validateURI(k)
            if k.startswith('file://'):
                  k = k[7:] 
            elif not k.startswith('hdfs://'):
                raise HcException(HCERR0503, {'name':'uri', 'value':k, 'accepted_values':'hdfs://<path>'})
        logger.error("[---] Splunk Debug Going to the directives!")
        results = self.directives[d]()
        logger.error("[---] Splunk Debug is back from directives. Results: {}".format(results))
        return (results,d)        
    # def _main_impl(self):
    #     if len(self.keywords) == 0:
	#        raise HcException(HCERR0505)

    #     d = self.keywords.pop(0)
    #     if not d in self.directives:
	#        raise HcException(HCERR0506, {'directive':d, 'accepted_values':','.join(self.directives.keys())}) 
        
    #     if len(self.keywords) == 0:
	#        raise HcException(HCERR0501, {'argument':'uri'})
    #     for k in self.keywords:
    #         self._validateURI(k)
    #         if k.startswith('file://'):
    #               k = k[7:] 
    #         elif not k.startswith('hdfs://'):
    #             raise HcException(HCERR0503, {'name':'uri', 'value':k, 'accepted_values':'hdfs://<path>'})

    #     self.directives[d]()

    def main(self):
           logger.error("[---] Splunk Debug printing isp Inspect Results: {}".format(inspect.stack()[1])) 
           results, dummyresults, self.settings = isp.getOrganizedResults()
           self.keywords, self.argvals = isp.getKeywordsAndOptions()
           logger.error("[---] Splunk Debug splunklib results: {}".format(self._metadata))
           # in Splunk pre 5.0 we don't get the info, so we just read it from it's standard location
           infoPath = self.settings.get('infoPath', '')
           logger.error("[---] Splunk Debug printing isp stuff inside hsc: {}".format(isp.getOrganizedResults()))
           logger.error("[---] Splunk Debug printing isp keywords and argvals inside hsc: {}".format(isp.getKeywordsAndOptions()))
           if len(infoPath) == 0:
              infoPath = os.path.join(getDispatchDir(self.settings.get('sid'), self.settings.get('sharedStorage', None)), 'info.csv')
           self.info.readFrom(infoPath)


           self.raiseAll = splunk.util.normalizeBoolean(unquote(self.argvals.get('raiseall', 'f')))
           self.sessionKey = self.settings.get('sessionKey', None)
           self.owner      = self.settings.get('owner',      None)
           self.namespace  = self.settings.get('namespace',  None)
           self.krb5_principal = unquote(self.argvals.get('kerberos_principal', '')).strip()


           if len(self.krb5_principal) == 0:
              self.krb5_principal = None
           HadoopEnvManager.init(APP_NAME, 'nobody', self.sessionKey, self.krb5_principal)

           self._main_impl()
    def generate(self):
        logger.error("[---] Splunk Debug Streaming metadata: {}".format(self._metadata))
        # metadata = json.loads(self._metadata.searchinfo)
        metadata = self._metadata
        
        self.keywords = metadata.searchinfo.args
        logger.error("[---] Splunk Debug Streaming args: {}".format(self.keywords))

        sid = metadata.searchinfo.sid
        logger.error("[---] Splunk Debug Search ID: {}".format(sid))
        infoPath = os.path.join(metadata.searchinfo.dispatch_dir, 'info.csv')
        logger.error("[---] Streaming infoPath: {}".format(infoPath))
        self.info.readFrom(infoPath)
        # self.raiseAll = splunk.util.normalizeBoolean(unquote(self.keywords.get('raiseall', 'f')))
        try:
            self.sessionKey = metadata.searchinfo.session_key
        except:
            self.sessionKey = None
        try:
            self.owner = metadata.searchinfo.owner
        except:
            self.owner = None
        try:
            self.namespace = metadata.searchinfo.namespace
        except:
            self.namespace = None
        try:
            self.krb5_principal = unquote(self.keywords.get('kerberos_principal', '')).strip()
        except:
            self.krb5_principal = None

        self.raiseAll = splunk.util.normalizeBoolean(unquote('f'))
        logger.error("[---] Splunk Debug Streaming session key: {}".format(self.sessionKey))
        logger.error("[---] Splunk Debug Streaming owner: {}".format(self.owner))
        logger.error("[---] Splunk Debug Streaming namespace: {}".format(self.namespace))
        logger.error("[---] Splunk Debug Streaming krb5 principal: {}".format(self.krb5_principal))
        HadoopEnvManager.init(APP_NAME, 'nobody', self.sessionKey, self.krb5_principal)

        results, directive = self._main_impl()
        logger.error("[---] Splunk Debug is printing results from main implementation: {}".format(results))
        logger.error("[---] Splunk Debug is printing the directive: {}".format(directive))
        ### Printing the result here once
        for p in self.keywords:
            for generator in results[p]:
                if directive=='read':
                    logger.error("[---] Splunk Debug The directive is read.")
                    for gen in generator:
                        for event in gen:
                            #logger.error("[---] Splunk Debug The event is {}".format(event))
                            yield event
                else:
                    logger.error("[---] Splunk Debug printing the LS Results: {}".format(generator))
                    for event in generator:
                        yield event
        # for result in results.keys():
        #     for event in results[result]:
        #         yield event
        
if __name__ == "__main__":
    dispatch(HDFSSearchCommand, sys.argv, sys.stdin, sys.stdout, __name__)
    # dispatch(HDFSSearchCommand)


# if __name__ == '__main__':
#    rv   = 0
#    hdfs = HDFSSearchCommand()     
#    #TODO: improve error messaging, here - right now the messages just go to search.log 
#    try:
#         hdfs.main()
#    except Exception as e:
#          logger.exception('Failed to run hdfs command')
#          import traceback
#          stack =  traceback.format_exc()

#          if hdfs.info != None:
#             msg = toUserFriendlyErrMsg(e) 
#             #msg = "HCERR1007: Error in 'hdfs' command: " + msg
#             msg = "Error in 'hdfs' command: " + msg
#             hdfs.info.addErrorMessage(msg)
#             logger.error("sid=%s, %s\nTraceback: %s" % (hdfs.settings.get('sid', 'N/A'), str(e), str(stack)))
#          else:
#             print "ERROR %s" % str(e).replace('\n', '\\n')
#          print >> sys.stderr, "ERROR %s\nTraceback: %s" % (str(e), str(stack))
#          rv = 1
#    finally:
#         try:
#             if hdfs.info != None:
#                hdfs.streamResults('') # just write out the info
#         except Exception as e:
#             logger.exception("Failed to update search result info")
#    sys.exit(rv)



