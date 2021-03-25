import md5,re,time,splunk.mining.dcutils as dcu
import splunk.Intersplunk as isp
import splunk.search
import splunk.entity as entity
from constants import *
from util import *
from hadooputils import *
from errors import *

class InfoSearchException(Exception):
  def __init__(self, msg):
    Exception.__init__(self, msg)

class RenameException(Exception):
    def __init__(self, e):
        self.e = e
        
class StatusUpdateHandler(object):
    def __init__(self):
        self.name = None
        self.namespace = None
        self.owner = None
        self.sessionKey = None
        # a key with None or empty string will not be updated, to reset the value, we have to set to a one space string
        self.content = {'status'                :   'initializing', # possible status: searching, renaming, done, failed
                        'status.jobs'           :   0,              # the number of export searches spawned
                        'status.jobs.psid'      :   ' ',            # the search id of scheduled search job
                        'status.jobs.sids'      :   ' ',            # a comma delimited list of search ids of all export searches
                        'status.jobs.progress'  :   0.0,            # overall progress info of all export searches 
                        'status.jobs.runtime'   :   0.0,            # the total amount of time for which all parallel export searches has been running
                        'status.jobs.errors'    :   ' ',            # a human readable error message if status is failed
                        'status.jobs.earliest'  :   ' ',            # lower bound of the search index_times disjunction of current export process
                        'status.jobs.latest'    :   ' ',            # upper bound of the search index_times disjunction of current export process
                        'status.jobs.starttime' :   time.time(),    # current export process start time
                        'status.jobs.endtime'   :   ' ',            # current export process end time 
                        'status.earliest'       :   None,           # lower bound of the search index_times disjunction of all time
                        'status.latest'         :   None,           # upper bound of the search index_times disjunction of all time
                        'status.load'           :   None            # list of last 10 successful jobs load factors, where load = total execution time / export time range
                        }


    def initCommonFields(self, jobs, et, lt):
        self.content['status.jobs'] = len(jobs)
        self.content['status.jobs.sids']      = ','.join([job.id for job in jobs])
        self.content['status.jobs.earliest']  = et
        self.content['status.jobs.latest']    = lt
    
    def updateFetchingStatus(self, jobs):
        self.content['status'] = 'searching'
        p = 0
        t = 0
        for job in jobs:
            p += job.doneProgress
            t += job.runDuration
        # job.doneProgress will hang there for some time when progress reaches to 1.0, but we have to move files after inner search is done. So we scale it to 95% done   
        self.content['status.jobs.progress'] = (p/len(jobs))*0.95
        #logger.info('doneProgress:%f, progress:%f, earliestTime:%s, latestTime:%s, cursorTime:%s' % 
         #           (job.doneProgress, self.content['status.jobs.progress'], job.earliestTime, job.latestTime, job.cursorTime))
        self.content['status.jobs.runtime'] = t
        self.updateStatus()
    
    def updateMovingStatus(self):
        self.content['status'] = 'renaming'
        self.content['status.jobs.progress'] += 0.01
        if self.content['status.jobs.progress'] > 1.0:
            self.content['status.jobs.progress'] = 0.999
        self.updateStatus()
        
    def updateDoneStatus(self, et, lt):
        self.content['status'] = 'done'
        self.content['status.latest'] = lt
        self.content['status.jobs.endtime'] = time.time()
        self.content['status.jobs.earliest'] = et
        self.content['status.jobs.latest']   = lt
        
        # update earliest if not done yet 
        try:
            en = self.getExportConf()
            if 'status.earliest' not in en or en['status.earliest']==None or en['status.earliest']=='': 
                self.content['status.earliest'] = et
            currentLoad = '%.4f' % float((self.content['status.jobs.endtime']-self.content['status.jobs.starttime'])/(lt-et))
            if 'status.load' not in en or en['status.load']==None or en['status.load']=='':
                self.content['status.load'] = currentLoad
            else:
                load = en['status.load'].split(',')
                load.insert(0, currentLoad)
                if len(load) > 10:
                    load.pop(len(load)-1)
                self.content['status.load'] = ','.join(load)
        except:
            logger.exception('Failed to update done status')    
                
        self.content['status.jobs.progress'] = 1.0
        self.updateStatus()
    
    # this method is good for mock up and make testing updateDoneStatus easy 
    def getExportConf(self):
        return entity.getEntity('admin/conf-export', self.name, namespace=self.namespace, owner=self.owner, sessionKey=self.sessionKey)
        
    def updateFailedStatus(self, error):
        self.content['status'] = 'failed'
        self.content['status.jobs.errors'] = error
        self.updateStatus()
    
    def updateStatus(self):
        if self.name!=None and self.namespace!=None and self.owner!=None:            
            try:
                # get entity first, so we get the canonical context 
                base = entity.getEntity('admin/conf-export', self.name, namespace=self.namespace, owner=self.owner, sessionKey=self.sessionKey)

                en = entity.Entity('admin/conf-export', self.name, self.content, self.namespace, self.owner)
                entity.setEntity(en, self.sessionKey, uri=base.id)
            except:
                logger.exception('Failed to update status')    
    
#/something/or/other/b73c2d22763d1ce2143a3755c1d0ad3a.1334040111.cursor.0
cursor_re = re.compile('([a-zA-z0-9]+)\.(\d+).cursor(?:\.(\d+))?$')
final_cursor_re = re.compile('([a-zA-z0-9]+)\.(\d+).cursor$')

def getIndexTimeDisjunction(name, et, lt):
    disjuncts = []
    while et < lt:
        level = 10
        
        while et % level == 0 and et + level < lt:
            level = level*10

        level = level/10
        disjuncts.append('_indextime=%d%s' % (et/level, level > 1 and '*' or ''))
        et += level
    return '(' + ' OR '.join(disjuncts) + ')'

def getDestination(uri, basepath):
    result = uri
    if result.endswith('/') and not result.endswith('://'):
       result = result[:-1] 

    if not basepath.startswith('/'):
       result += '/'

    result += basepath 
    return result

def getRelevantMessages(searchJob):
    result = []
    sid = searchJob.id
    for level,msgs in searchJob.messages.items():
        if level.lower() == 'error': 
            result.extend(msgs)
            continue
        for m in msgs:
            if m.startswith('Unable to distribute to peer'):
                result.append(m)
            #TODO: look for other relevant messages
    
    return result

def getExportId(name):
    m = md5.new()
    m.update(name)
    return m.hexdigest()

def getCursorFilename(name, lt):
    return getExportId(name) + '.' + str(lt) + '.cursor'

def getTmpFileSuffix():
    return DEFAULT_ROLL_EXTENSION
    #return ".hdfs"

def getTmpFileRegex(name):
    import re
    suffix = getTmpFileSuffix().replace('.', '\\.')
    return re.compile('/' + getExportId(name) + '.*' + suffix + '$') 

def getintarg(args, ej, name, defval):
    s = unquote(args.get(name, ej.get(name, None)))
    try:
       return int(s)
    except:
       return defval

def cleanSearchString(search):
    s = search.lstrip()
    if s.startswith('|'):
       s = s.lstrip('| ')
       if not s.startswith('search '):
          raise HcException(HCERR1507, {'search':search})
    if s.startswith('search '):
       s = s[7:]
    return s.lstrip()

def containsEvalSearch(search):
    return (re.compile('\|\s*eval\s+_dstpath\s*=').search(search) != None)
        
def populateEvalSearch(ej):
    if 'partition_fields' not in ej or ej['partition_fields'] == None or ej['partition_fields'].strip() in ['', 'None']:
        return ''
    
    fields =[]
    partition_fields = ej['partition_fields'].split(',')
    
    if '_time' in partition_fields:
        fields.append('strftime(_time, "%Y%m%d/%H")')
    if 'date' in partition_fields:
        fields.append('strftime(_time, "%Y%m%d")')
    if 'hour' in partition_fields:
        fields.append('strftime(_time, "%H")')    
    if 'host' in partition_fields:
        fields.append('host')
    if 'sourcetype' in partition_fields:
        fields.append('sourcetype')
    if 'source' in partition_fields:
        fields.append('source')
    return '| eval _dstpath=' + ' + "/" + '.join(fields)


def optimizeExport(ej, lt, now, minspan, maxspan, currspan, hdfs_bytes, moved_files, roll_size):
    #TODO: implement me
    pass

def buildSearchString(splunk_servers, index_times, search, evalSearch, sid, basefilename, name, roll_size, dst, principal, format, export_fields, required_fields, compress_level=2, useDumpCmd=False):
    query = 'search ' + splunk_servers + ' ' + index_times + ' ' + cleanSearchString(search) + evalSearch + (' | fields _dstpath host source %s | ' % required_fields)
    if useDumpCmd:
        query += ('dump basefilename=%s rollsize=%d format=%s fields="%s" parentsid=%s exportname="%s" compress=%d | movehdfs dst=%s %s parentsid=%s exportname="%s"' 
            % (basefilename, roll_size, format, export_fields, sid, name, compress_level, dst, principal, sid, name))
    else:
        query += ('exporthdfs parentsid=%s basefilename=%s exportname="%s" rollsize=%d dst=%s %s format=%s fields="%s" compress=%d' 
            % (sid, basefilename, name, roll_size, dst, principal, format, export_fields, compress_level))
    logger.error("[---] Splunk Debug's new query from buildSearchString: {}".format(query))
    return query

class RunExport(object):        
    def getExportJob(self, name, owner, namespace, sessionKey):
        try:
            return entity.getEntity('admin/conf-export', name, namespace=namespace, owner=owner, sessionKey=sessionKey)
        except Exception as e:
            logger.exception("Failed to get stanza '%s' from export.conf", name)
            msg = str(e)
            import splunk
            if isinstance(e, splunk.RESTException):
                #msg = e.get_message_text()
                msg = e.get_extended_message_text() 
            raise HcException(HCERR1510, {'export':name, 'error':msg})

    def createHadoopCliJob(self, path, krb5_principal=None):
        return HadoopEnvManager.getCliJob(path, krb5_principal)
        
    def createHDFSDirLister(self):
        return HDFSDirLister()
    
    # save the cursor somewhere in dst
    def createCursor(self, name, lt, dst, krb5_principal=None):
        # initial state
        filename = getCursorFilename(name, lt) + '.0'
        path = hdfsPathJoin(dst, filename)
        hj = self.createHadoopCliJob(path, krb5_principal)
        hj.touchz(path)
        hj.wait(True)
        return path
    
    def incrCursorState(self, path, krb5_principal=None):
        m = cursor_re.search(path) 
        if m == None or m.group(3) == None:
           raise HcException(HCERR1502, {'cursor':path, 'error':"Invalid cursor path"})
        
        id = int(m.group(3))
        id += 1
        new_path = path[:path.rindex('.')] + '.' + str(id)
        hj = self.createHadoopCliJob(path, krb5_principal)
        hj.mv(path, new_path)
        hj.wait(True)
        return new_path

    def finalizeCursor(self, path, krb5_principal=None):
        m = cursor_re.search(path)
        if m == None or m.group(3) == None:
           raise HcException(HCERR1502, {'cursor':path, 'error':"Invalid cursor path"})
        
        # strip the last #
        new_path = path[:path.rindex('.')]
        hj = self.createHadoopCliJob(path, krb5_principal)
        hj.mv(path, new_path)
        hj.wait(True)
        return new_path

    def removeFile(self, path, krb5_principal=None):
        hj = self.createHadoopCliJob(path, krb5_principal)
        hj.rm(path)
        hj.wait(True)

    def getCursors(self, name, dst):
        filename = getCursorFilename(name, '*') + '*'
        abspath = hdfsPathJoin(dst, filename)
        return self.parseCursorSearchOutput(abspath)

    def parseCursorSearchOutput(self, abspath, krb5_principal=None):
        result = []
        hj = self.createHadoopCliJob(abspath, krb5_principal)
        hj.ls(abspath)
        logger.error("[---] Parse Cursor Search Output: Splunk Debug")
        logger.error("[---] Abspath: {}".format(abspath))
        # ignore missing cursor files/dirs 
        if hj.wait() != 0 and hj.getStderr().find('No such file or directory') < 0:
            raise HcException(HCERR0001, {'cmd':hj.fscmd, 'options':','.join(hj.fsopts), 'error':hj.getStderr()})
        
        lines = hj.getOutput()[0].split('\n')
        logger.error("[---] Printing lines: {}".format(lines))
        for l in lines:
            if l != '':
               lse = HDFSDirLister._parseLine(l)
               m = None    
               if lse != None:
                   m = cursor_re.search(lse.path)
               if m != None:
                   result.append({'md5': m.group(1), 'time': int(m.group(2)), 'state': m.group(3), 'path': lse.path })
        #logger.error("[---] Splunk Debug parsecursorsearch Results: {}".format(result))
        return result

    def getLastCursorTime(self, name, dst, defaultTime=1000000000):
        cursors = self.getCursors(name, dst)
        result = defaultTime
        for c in cursors:
            if c['state'] == None:  #final cursor
               if c['time'] > result:
                  result = c['time']  
        return result

    def listTmpFiles(self, name, dst):
        logger.error("[---] Splunk Debug Printing name: {}".format(name))
        logger.error("[---] Splunk Debug print the DST: {}".format(dst))
        hj = self.createHDFSDirLister()
        result = []
        tmprex = getTmpFileRegex(name)
        logger.error("[---] Splunk Debug print the tmprex: {}".format(tmprex))
        for l in hj.lsr(dst):
            file = l.path 
            if tmprex.search(file) != None:
               result.append(l)
        return result
    
    def updateReplication(self, name, replication, base, dst, krb5_principal, cursor_replication=3):
        # update replications of all data per replication factor
        # replication == 0 means default replication level, 
        # ie leave the replication level at whatever the cluster default is
        if replication != 0:
           hj = self.createHadoopCliJob(dst, krb5_principal)
           hj.setrep(dst, replication, False) 
           hj.wait(True)

        # update cursor replication if different than data 
        if cursor_replication != replication:
           for c in self.getCursors(name, dst):
               p = hdfsPathJoin(base, c['path'])
               hj = self.createHadoopCliJob(p, krb5_principal)
               hj.setrep(p, cursor_replication, False, False)
               hj.wait(True)
        
    def renameTmpFiles(self, name, dst, suh=None, tmpfiles=None, krb5_principal=None, replication=3):
        # walk the dst tree and rename the tmp files to their final dst
        if tmpfiles == None:
           tmpfiles = self.listTmpFiles(name, dst)
    
        logger.info("Renaming %d temporary files in HDFS..." % len(tmpfiles))
        logger.error("[---] Splunk Debug Renaming %d temporary files in HDFS..." % len(tmpfiles))
        base     = getBaseURI(dst)
        suffix   = getTmpFileSuffix()
        jobwaiter = HDFSJobWaiter()
        logger.error("[---] Splunk Debug printing base: {}".format(base))
        logger.error("[---] Splunk Debug printing jobwaiter object: {}".format(jobwaiter))
        logger.error("[---] Splunk Debug printing suffix: {} and len of suffix: {}".format(str(type(suffix)), str(len(suffix))))
        for f in tmpfiles:
            try:
                src = hdfsPathJoin(base, f.path)
                # Original code is the line below - 4 lines below is the new change
                # path = src[:-len(suffix)]
                if len(suffix)!=0:
                    path = src[:-len(suffix)]
                else:
                    break
                logger.error("[---] Splunk Debug's Src: {} \n Path: {}".format(src,path))
                hj = self.createHadoopCliJob(src, krb5_principal)
                hj.mv(src, path)
                jobwaiter.addJob(hj)
            except Exception as e:
                logger.error("Exception failed: {}".format(e))
                
        try:            
            jobwaiter.wait(suh=suh)
        except Exception as e:
            logger.exception('Failed in waiting for jobwaiter to finish')
            raise RenameException(str(e))

        self.updateReplication(name, replication, base, dst, krb5_principal)
             
        return jobwaiter.jobs_total, jobwaiter.total_time
    
    def removeTmpFiles(self, name, dst, krb5_principal=None, tmpfiles=None):
        # walk the dst tree and remove the tmp files
        if tmpfiles == None:
            tmpfiles  = self.listTmpFiles(name, dst)
        return self.removeFiles(dst, tmpfiles, krb5_principal)
    
    def removeFiles(self, dst, files, krb5_principal=None):
        base      = getBaseURI(dst)
        jobwaiter = HDFSJobWaiter()
        for f in files:
            src = hdfsPathJoin(base, f.path)
            hj = self.createHadoopCliJob(src, krb5_principal)
            hj.rm(src)
            jobwaiter.addJob(hj)

        jobwaiter.wait()
        
        return jobwaiter.jobs_total, jobwaiter.total_time
    
    def cleanupRenameState(self, name, dst, cursor, tmpFiles=None, finalFiles=None):
        rm_total_files = 0
        rm_total_time = 0
        if(tmpFiles==None or finalFiles==None):
            listFiles, tmpFiles, finalFiles = self.listFilesFromWAL(name,dst)
        rm_files, rm_time = self.removeFiles(dst, tmpFiles)
        rm_total_files += rm_files
        rm_total_time += rm_time
        rm_files, rm_time = self.removeFiles(dst, finalFiles)
        rm_total_files += rm_files
        rm_total_time += rm_time
        self.removeWAL(name, dst)
        self.removeFile(cursor)
        return rm_total_files, rm_total_time
        
    def cleanup(self, name, dst, sid, child_sid, ex=None, replication=3):
        rm_total_files = 0
        rm_total_time = 0
        fn_total_files = 0
        fn_total_time = 0
        logger.error("BEFORE GETTING CURSORS")
        cursors = self.getCursors(name, dst)
        logger.error("AFTER GETTING CURSORS")
        logger.error("[---] Splunk Debug printing Cursors: {}".format(cursors))
        # check cursors and attempt to do recovery (either delete tmp files or rename them)
        max_final = 0
        base = getBaseURI(dst)
        for c in cursors:
            if c['state'] == None: # final cursor
               if max_final < c['time']:
                  max_final = c['time']
               continue 
            
            #1. look for .0 cursor files -> remove all the tmp files and revert cursor
            if int(c['state']) == 0:
               rm_files, rm_time = self.removeTmpFiles(name, dst)
               rm_total_files += rm_files
               rm_total_time += rm_time
               self.removeFile(hdfsPathJoin(base, c['path']))
               self.removeWAL(name, dst, False)
            #2. look for .1 cursor files -> either delete all or rename remaining files
            elif int(c['state']) == 1:
               listFiles, tmpFiles, finalFiles = self.listFilesFromWAL(name, dst)
               # delete all files generated since last export if some file listed in WAL is missing
               if(len(listFiles) != len(tmpFiles)+len(finalFiles)):
                   total_files, total_time = self.cleanupRenameState(name, dst, hdfsPathJoin(base, c['path']), tmpFiles, finalFiles)
                   rm_total_files += total_files
                   rm_total_time += total_time
               else: # otherwise, no file missing, we can safely finish renaming process and finalize cursor    
                   fn_files, fn_time = self.renameTmpFiles(name, dst, replication=replication)
                   fn_total_files += fn_files
                   fn_total_time += fn_time              
                   self.finalizeCursor(hdfsPathJoin(base, c['path']))
                   # remove WAL
                   self.removeWAL(name, dst)
            else:
               #TODO: figure out what to do with unknown state cursor
               pass 
    
        # remove old final cursors
        for c in cursors:
            if c['state'] == None and c['time'] < max_final: 
               self.removeFile(hdfsPathJoin(base, c['path']))
        
        if rm_total_files>0 or fn_total_files>0:       
            logger.info("Cleanup process was run: exportname=%s, sid=%s, child_sid=\"%s\", throwMsg=%s"  % (name, sid, ','.join(child_sid), str(ex)))
            logger_metrics.info("group=export, exportname=\"%s\", sid=%s, child_sid=\"%s\", cleanup_rm_files=%d, cleanup_rm_time=%.3f, cleanup_moved_files=%d, cleanup_move_time=%.3f" 
                                % (name, sid, ','.join(child_sid), rm_total_files, rm_total_time, fn_total_files, fn_total_time))
    
        if ex != None:
           raise ex
    
        # list cursors again and ensure that no partial cursors are left behind
        cursors = self.getCursors(name, dst)
        logger.error("Listing cursors again")
        for c in cursors:
            if c['state'] != None: 
               raise HcException(HCERR1504, {'cursor':c['path']})
 

    # wait for the search jobs to finish
    def waitForSearches(self, jobs, info, sid, name, suh=None, period=1):
        counter = 0
        msg = []
        canceled = False 
        waitJobs = jobs
        doneJobs = []
        
        while not canceled and len(waitJobs) > 0:
           time.sleep(period)
    
           counter += period
           #every 10 seconds, update export status
           if counter % 10 == 0 and suh!=None:
              suh.updateFetchingStatus(jobs)
              #every two minutes, log the job status so we know it is alive
              if counter % 120 == 0:
                  logger.info("waiting for the count=%d search jobs to complete: exportname=%s, sid=%s, child_sid=\"%s\" " % ( len(waitJobs), name, sid, ','.join([sj.id for sj in waitJobs]) ) )
    
           for sj in waitJobs:
              if canceled:
                 break
    
              done = True
              try:
                  if not sj.isDone and not sj._cachedProps['isFinalized'] and not sj._cachedProps['isZombie'] and not sj._cachedProps['isFailed']:
                     msg.extend(getRelevantMessages(sj))
                     if len(msg) > 0:
                        sj.cancel()
                        canceled = True
                     else:
                        done = False
              except Exception as e:
                  # handle external search canceling, or some other errors
                  m = 'Failed to cancel a unsuccessful search job, sid=%s: %s' % (sj.id, str(e)) 
                  logger.exception(m)
                  msg.append(m)
                  canceled = True
                  try:
                     sj.cancel()
                  except:  pass
    
              if done:
                 doneJobs.append(sj)
    
           for sj in doneJobs:
               if sj in waitJobs:
                  waitJobs.remove(sj)
    
        # if one of the searches failed, we kill all the other ones 
        if canceled:
           for sj in waitJobs:
               try:
                   sj.cancel()
               except: pass
               doneJobs.append(sj)
               msg.append("Canceling export search sid=%s because one of it's siblings was canceled" % sj.id)
           waitJobs = []    
        else:
           for sj in doneJobs:
               # ensure that search completed successfully
               msg.extend(getRelevantMessages(sj))
               
               # finalized/zombied/failed searches are treated as if they had generated an error 
               if sj.isFinalized:
                  msg.append('Export search job sid=%s, was finalized unexpectedly, reverting export!' % sj.id)
    
               if sj.isZombie:
                  msg.append('Export search job sid=%s, was found in a zombie state, reverting export!' % sj.id)
    
               if sj.isFailed:
                  msg.append('Export search job sid=%s, was found in a failed state, reverting export!' % sj.id)
    
        if len(msg) > 0:
           for m in msg:
               #m = "HCERR1508: Error in 'runexport' command: " + m
               m = "Error in 'runexport' command: " + m
               info.addErrorMessage(m)
        
        return msg


    def validateRunExport(self, namespace, owner, sessionKey, argvals, sid):
    
        if not 'name' in argvals:
           raise HcException(HCERR0501, {'argument':'name'})
    
        # ensure that this search is being ran by the scheduler !!!
        if not sid.startswith('scheduler_') and not unquote(argvals.get('forcerun', '')) == '1':
           raise HcException(HCERR1503)
    
    def listFilesFromWAL(self, name, dst):
        tmpFiles = []
        finalFiles = []
        # get file list from WAL
        listFiles = self.readWAL(name, dst)
        # ensure all files listed in WAL is either a temp file or a final file, but not both
        if len(listFiles) > 0:
            hj = self.createHadoopCliJob(listFiles[0])
            for f in listFiles:
                if(hj.exists(f)):
                    tmpFiles.append(f)
                else:
                    tf = f[:-len(getTmpFileSuffix())]
                    if(hj.exists(tf)):
                        finalFiles.append(tf)
        return listFiles, tmpFiles, finalFiles
    
    def getWALPaths(self, name, dst):
        wal_filename = getExportId(name) + '.wal'
        wal_hdfs_path = hdfsPathJoin(dst, wal_filename)
        wal_local_path = os.path.join(self.tmp_dir, wal_filename)
        return wal_hdfs_path, wal_local_path
        
    def removeWAL(self, name, dst, raiseOnError=True):
        wal_hdfs_path, wal_local_path = self.getWALPaths(name, dst)
        hj = self.createHadoopCliJob(wal_hdfs_path)        
        hj.rm(wal_hdfs_path)
        hj.wait(raiseOnError)

        if os.path.exists(wal_local_path):
            os.remove(wal_local_path)
     
    def writeWAL(self, name, dst, tmpfiles):
        wal_hdfs_path, wal_local_path = self.getWALPaths(name, dst)
        with open(wal_local_path, 'w') as f:
            for tf in tmpfiles:
                f.write(tf.path+'\n')
 
        hj = self.createHadoopCliJob(wal_hdfs_path)
        logger.error("[---] WRITING WAL: Splunk Debug")        
        hj.moveFromLocal(wal_local_path, wal_hdfs_path)
        hj.wait(True)
        
    def readWAL(self, name, dst):
        files = []
        wal_hdfs_path, wal_local_path = self.getWALPaths(name, dst)
        hj = self.createHadoopCliJob(wal_hdfs_path)
        if not hj.exists(wal_hdfs_path):
            return files
       
        # copy wal file from hdfs to local filesystem 
        if os.path.exists(wal_local_path):
            os.remove(wal_local_path)
        
        hj = self.createHadoopCliJob(wal_hdfs_path)
        hj.get(wal_hdfs_path, wal_local_path)
        hj.wait(True)
        
        # read and parse wal file
        with open(wal_local_path) as f:
            line = f.readline()
            if line:
                #WAL contains paths absolute from the root of HDFS, not full URLs
                abspath = hdfsPathJoin(dst, line.strip())
                files.append(abspath)
        os.remove(wal_local_path)

        return files
    
    def getFieldsHeaderFilePaths(self, name, dst, format):
        header_filename = getExportId(name) + '_fields_header.' + format
        header_hdfs_path = hdfsPathJoin(dst, header_filename)
        header_local_path = os.path.join(self.tmp_dir, header_filename)
        return header_hdfs_path, header_local_path
        
    def readExportFieldsFromHdfs(self, name, dst, format):
        fields = []
        header_hdfs_path, header_local_path = self.getFieldsHeaderFilePaths(name, dst, format)
        hj = self.createHadoopCliJob(header_hdfs_path)
        if not hj.exists(header_hdfs_path):
            return fields
        
        # copy header file from hdfs to local filesystem
        if os.path.exists(header_local_path):
            os.remove(header_local_path)

        hj = self.createHadoopCliJob(header_hdfs_path)
        hj.get(header_hdfs_path, header_local_path)
        hj.wait(True)
        
        # read header file and convert it to list
        with open(header_local_path) as f:
            fields = f.readline().split(self.getDelimiter(format))
        os.remove(header_local_path)

        return fields
        
    def writeExportFieldsToHdfs(self, name, dst, fields, format):
        header_hdfs_path, header_local_path = self.getFieldsHeaderFilePaths(name, dst, format)
        with open(header_local_path, 'w') as f:
            f.write(self.getDelimiter(format).join(fields))

        hj = self.createHadoopCliJob(header_hdfs_path)
        logger.error("[---] WRITING export fields: Splunk Debug")                
        hj.moveFromLocal(header_local_path, header_hdfs_path)
        hj.wait(True)
    
    def getDelimiter(self, format):
        if format == 'csv':
            return ','
        elif format == 'tsv':
            return '\t'
        else:
            raise HcException(HCERR0503, {'name':'format', 'value':format, 'accepted_values':'csv,tsv'})
            
    def verifyExportFields(self, name, dst, format, fields):
        format = format.strip()
        fields = fields.strip()
        supported_formats = ['csv', 'tsv', 'json', 'xml', 'raw']
        if format == '':
            raise HcException(HCERR0502, {'name':'format', 'value':format, 'error':"Cannot be None, empty or all white space string"})
        if fields == '':
            raise HcException(HCERR0502, {'name':'fields', 'value':fields, 'error':"Cannot be None, empty or all white space string"})
       
        if format not in supported_formats:
           raise HcException(HCERR0503, {'name':'format', 'value':format, 'accepted_values':','.join(supported_formats)}) 
 
        export_fields = [x.strip() for x in fields.split(',')]
        required_fields = ','.join(export_fields)  #this could be a subset of fields specified in previous exports
        if format == 'csv' or format == 'tsv':
            # ensure no wildcard fields 
            for f in export_fields: 
                if f.find('*') >= 0:
                    raise HcException(HCERR0502, {'name':'fields', 'value':fields, 'error':"Wildcarded fields are not allowed for format '%s'" % format})
                
            # read export fields from HDFS, return empty list if header file not exists
            last_fields = self.readExportFieldsFromHdfs(name, dst, format)
            
            if len(last_fields) == 0: # exporting for the first time
                self.writeExportFieldsToHdfs(name, dst, export_fields, format)
            else:
                # check no fields are ever added to the export field list (it it ok to remove a field, we will fill up an empty value)
                for field in export_fields:
                    if field not in last_fields:
                        raise HcException(HCERR0502, {'name':'fields', 'value':fields, 'error':"Field '%s' is not presented in fields exported last time. last export fields: %s" % (field, str(last_fields))})
                # for csv/tsv format, always pass the same field list to exporthdfs 
                export_fields = last_fields
        return ','.join(export_fields), required_fields        

            
    def main(self, results, settings, info, suh):
        keywords, argvals = isp.getKeywordsAndOptions()
        sessionKey = settings.get("sessionKey", None)
        owner      = settings.get("owner",      None)
        namespace  = settings.get("namespace",  None)
        
        sid = info.get('_sid', '')
        child_sid = []
        suh.content['status.jobs.psid'] = sid
        logger.error("[---] Splunk Debug printing SID inside main: {}".format(sid))
        self.dispatch_dir = getDispatchDir(sid, settings.get('sharedStorage', None))
        self.tmp_dir = os.path.join(self.dispatch_dir, DEFAULT_HDFS_TMP_DIR_NAME)
        if not os.path.exists(self.tmp_dir):
           os.makedirs(self.tmp_dir)
 
        self.validateRunExport(namespace, owner, sessionKey, argvals, sid)
        
        name = unquote(argvals['name']).strip()
        ej = self.getExportJob(name, owner, namespace, sessionKey)
    
        suh.name       = ej.name
        suh.namespace  = ej.namespace # use export job's namespace and owner
        suh.owner      = ej.owner
        suh.sessionKey = sessionKey
    
        # initialize hadoop env manager, including secure cluster info
        # the init is lazy and the manager caches hadoop env for better perf
        krb5_principal = unquote(argvals.get('kerberos_principal', ej.get('kerberos_principal', '')))
        if krb5_principal != None:
           krb5_principal = krb5_principal.strip()
        if krb5_principal != None and len(krb5_principal) == 0:
           krb5_principal = None


        HadoopEnvManager.init(APP_NAME, 'nobody', sessionKey, krb5_principal)
        
        # for remote export:
        # ensure the hadoop env is setup correctly, this will throw if 
        # HADOOP/JAVA_HOME are not set correctly
        # for local export:
        # simple return a copy of os.environ
        HadoopEnvManager.getEnv(ej['uri'], krb5_principal)
   
    
        replication = getintarg(argvals, ej, 'replication', 3)
        
        # build and dispatch search 
        search = ej['search']
        dst    = getDestination(ej['uri'], ej['base_path'])
    
        # make dst directory early on
        hj = self.createHadoopCliJob(dst, krb5_principal)
        makeHdfsDir(hj, dst)
        self.cleanup(name, dst, sid, child_sid, replication=replication)
        logger.error("[---] Splunk Debug is Checking for variables")
        min_start_time = 1243832400  # 6/1/2009, ~release date of 4.0
    
        # go back one minute to account for slight difference in search head & indexer time
        # TODO: make this offset configurable 
        now = int(time.time())
        now = now - 60 
    
        # some export jobs args can be overriten by runexport cmd args
        compress_level = getintarg(argvals, ej, 'compress_level', 2)
        if compress_level < 0 or compress_level > 9:
            raise HcException(HCERR0503, {'name':'compress_level', 'value':compress_level, 'accepted_values':'a integer between 0 to 9, 0 means no compression'})
        roll_size = getintarg(argvals, ej, 'roll_size', DEFAULT_ROLL_SIZE)
        maxspan   = getintarg(argvals, ej, 'maxspan',   365*24*3600)   # do not export more than this time range at once, seconds, 0 == unlimited
        minspan   = getintarg(argvals, ej, 'minspan',   600)           # do not export less that this time range at once, seconds  
        starttime = getintarg(argvals, ej, 'starttime', min_start_time) 
        lt        = getintarg(argvals, ej, 'endtime',   now) 
        jc        = unquote(argvals.get('parallel_searches', ej.get('parallel_searches', '1')))
        format    = unquote(argvals.get('format', ej.get('format', 'raw')))
        fields    = unquote(argvals.get('fields', ej.get('fields', '_raw')))

        logger.error("[---] Splunk Debug done with variables")
        # interpret 0s:
        # for lt -> 0 means now, 
        # for starttime means latest possible time, ie lt - minspan 
        if lt == 0:
           lt = now
        if starttime == 0:
           starttime = lt - minspan - 1
    
        if starttime < min_start_time:
           starttime = min_start_time
    
        et = self.getLastCursorTime(name, dst, starttime)
        logger.error("[---] Splunk Debug printing the ET: {}".format(et))
        #TODO: figure out how to handle the first time run  
        if maxspan > 0 and et + maxspan < lt: # clamp lt if maxspan is given
           lt = et + maxspan
        logger.error("Printing et and lt: {} {}".format(et,lt))
        if lt-et < minspan:
           suh.content['status'] = 'done'
           suh.updateStatus() 
           logger.error("[---] Splunk Debug Minspan problems")
           raise InfoSearchException("Time range since last successful export is less than minspan. Not starting export job. (et=%d, lt=%d, range=%d, minspan=%d)" % (et, lt, int(lt-et), minspan))

        logger.error("[---] Splunk Debug verifying export fields")
        export_fields, required_fields = self.verifyExportFields(name, dst, format, fields)
        logger.error("[---] Done verifying")
        index_times = getIndexTimeDisjunction(name, et, lt )
    
        # check that all enabled peers are Up
        peers     = entity.getEntities('search/distributed/peers', count="-1", search="disabled=0", namespace=namespace, owner=owner, sessionKey=sessionKey) 
        peersDown = []
        peersAll  = ['local']    
    
        for pn, po in peers.items():
            if po.get('status', '').lower() != 'up':
               peersDown.append(po.get('peerName', ''))
            peersAll.append(po.get('peerName', ''))        
    
        if len(peersDown) > 0:
            names = ', '.join(peersDown)
            raise HcException(HCERR1505, {'peer_count':len(peersDown), 'peers':names}) 
    
        #TODO: ensure that no other search is running concurrently 
    
       
        # create a number of searches depending on jobcount 
        basefilename = "%s_%d_%d" % (getExportId(name), et, lt)
        evalSearch = '' if containsEvalSearch(search) else populateEvalSearch(ej)
        principal  = '' if krb5_principal == None else 'kerberos_principal="'+krb5_principal+'"'
        logger.error("[---] Splunk Debug print the basefilename: {}".format(basefilename))
        searches = []
    
        if jc.lower().strip() == 'max':
           jc = len(peersAll)
        else:
           try:
               jc = int(jc)
           except:
               jc = 1   
        
        useDumpCmd = False
        splunkVersion = None
        splunkMajorVersion = None
        try:
            splunkVersion = settings.get('splunkVersion', '')
            logger.info('splunkVersion:'+str(splunkVersion))
            i = splunkVersion.find(".")
            if i < 0:
                splunkMajorVersion = int(splunkVersion)
            else:
                splunkMajorVersion = int(splunkVersion[:i])
            logger.debug('splunkMajorVersion:'+str(splunkMajorVersion))
            useDumpArg = False if 'useDump' in argvals and argvals.get('useDump') == '0' else True
            useDumpCmd = splunkMajorVersion >= 6 and useDumpArg
        except:
            logger.exception('Failed to parse splunk version:' + str(splunkVersion))
        logger.info('useDumpCmd:'+str(useDumpCmd))
        logger.error('[---] Splunk Debug printing useDumpCmd:'+str(useDumpCmd))
        if jc == 1 or len(peersAll) == 1:  
           searches.append( buildSearchString('', index_times, search, evalSearch, sid, basefilename, name, roll_size, dst, principal, format, export_fields, required_fields, compress_level, useDumpCmd) )
           #log the export jobs parameters
           logger.error('[---] Splunk Debug: export args: starttime=%d, endtime=%d, minspan=%d, maxspan=%d, roll_size=%d, basefilename=%s, format=%s, fields="%s", search="%s", %s' 
                       % (starttime, lt, minspan, maxspan, roll_size, basefilename, format, export_fields, search, principal))
           logger.info('export args: starttime=%d, endtime=%d, minspan=%d, maxspan=%d, roll_size=%d, format=%s, fields="%s", search="%s", %s' 
                       % (starttime, lt, minspan, maxspan, roll_size, format, export_fields, search, principal))
        else:
           i = 0
           for pl in splitList(peersAll, jc):
              splunk_servers = ' OR '.join(['splunk_server="'+ss+'"' for ss in pl])
              bfn = basefilename + '_' + str(i)
              searches.append( buildSearchString('(' + splunk_servers + ')', index_times, search, evalSearch, sid, bfn, name, roll_size, dst, principal, format, export_fields, required_fields, compress_level, useDumpCmd) ) 
              #log the export jobs parameters
              logger.info('export args: starttime=%d, endtime=%d, minspan=%d, maxspan=%d, roll_size=%d, format=%s, fields="%s", search="%s", %s' 
                          % (starttime, lt, minspan, maxspan, roll_size, format, export_fields, search, principal))
              i += 1


        logger.error("[---] Splunk Debug Searches created: {}".format(searches))
        cpath  = self.createCursor(name, lt, dst)
    
        # spawn the searches 
        sj_st = float(time.time())  # search job start time 
        sjobs = []
        for search in searches:
            logger.info('search:'+search)
            logger.error('[---] Splunk Debug is printing each search:'+search)
            sj = splunk.search.dispatch(search, namespace=namespace, owner=owner, sessionKey=sessionKey)
            sjobs.append(sj)
            child_sid.append(sj.id)
            info.addInfoMessage('spawned search: sid=%s, search=%s' % (sj.id, search))
    
        suh.initCommonFields(sjobs, et, lt)
        suh.updateFetchingStatus(sjobs)
        logger.error("Updating fetch status")
        # shut these guys up - too chatty!!!
        for chatty in ['splunk.search', 'splunk.rest']:
            chatty_logger = logging.getLogger(chatty)
            chatty_logger.setLevel(logging.INFO)
    
        # wait for all the searches to complete, succeeds iff all search jobs complete without errors
        logger.error("[---] Splunk Debug Waiting for searches.")
        msg =  self.waitForSearches(sjobs, info, sid, name, suh)
        logger.error("[---] Splunk Debug Waiting for searches is done. {}".format(msg))
        if len(msg) > 0 :
           logger.error("Failed while waiting for search to complete:" + str(msg)) 
           # this raises an exception
           self.cleanup(name, dst, sid, child_sid, HcException(HCERR1512, {'error_count':len(msg), 'child_sids':','.join(child_sid), 'errors':json.dumps(msg)}))

    
        sj_et = float(time.time()) #search job end time
    
        suh.updateMovingStatus()
        
        tmpfiles = self.listTmpFiles(name, dst) 
        # write tempfiles to WAL, which lives in HDFS too
        self.writeWAL(name, dst, tmpfiles) 

        # at this point we know that we have all the results and they are in tmp files and logged in WAL
        # we increment the cursor's state so that recovery can move any remaining chunks
        cpath = self.incrCursorState(cpath)
        # move files to their final destination
        moved_files = 0
        move_time = 0 
        logger.error("[---] Splunk Debug printing cpath: {}".format(cpath))
        logger.error("[---] Splunk Debug Moving tmp files Name: {}".format(name))
        logger.error("[---] Splunk Debug Moving tmp files Dst: {}".format(dst))
        logger.error("[---] Splunk Debug Moving tmp files Suh: {}".format(suh))
        logger.error("[---] Splunk Debug Moving tmp files tmpfiles: {}".format(tmpfiles))
        try:
            moved_files, move_time = self.renameTmpFiles(name, dst, suh, tmpfiles, replication=replication)
        except RenameException as e:
            self.cleanupRenameState(name, dst, cpath)
            raise e
             
        # finalize the cursor's state
        cpath = self.finalizeCursor(cpath)
        # remove WAL
        self.removeWAL(name, dst)
        
        hdfs_bytes = 0
        for f in tmpfiles:
            hdfs_bytes += f.size
        hdfs_bytes /= (1024*1024)
    
        logger_metrics.info("group=export, exportname=\"%s\", sid=%s, child_sid=\"%s\", search_time=%.3f, moved_files=%d, move_time=%.3f, total_size_mb=%d" 
                            % (name, sid, ','.join(child_sid), sj_et-sj_st, moved_files, move_time, hdfs_bytes))
    
        suh.updateDoneStatus(et, lt)
   
    # If in auto optimize mode try to adjust the following variables such that avg file size is close to configured file size
    # and that we're catching up as fast as we can 
    # a. execution period
    # b. minspan/maxspan
    #optimizeExport(ej, lt, now, minspan, maxspan, currspan, hdfs_bytes, moved_files, roll_size)

if __name__ == '__main__':
    results, dummyresults, settings = isp.getOrganizedResults()
    infoPath = settings.get('infoPath', '')
    info = SearchResultsInfo()
    info.readFrom(infoPath)
    
    suh = StatusUpdateHandler()
    try:
        runExport = RunExport()
        logger.error("[---] Splunk Debug is calling main")
        runExport.main(results, settings, info, suh)
    except InfoSearchException as e1:
        logger.info('Canceled run_export script: '+str(e1))
        info.addInfoMessage(str(e1))
    except Exception as e:
        logger.exception('Failed to run run_export script')
        msg = toUserFriendlyErrMsg(e)
        #msg = "HCERR1508: Error in 'runexport' command: " + msg
        msg = "Error in 'runexport' command: " + msg
        info.addErrorMessage(msg)
        info.finalizeSearch()
        if suh.content['status'] != 'done':
            suh.updateFailedStatus(msg)
    finally:
        try:
            info.writeOut()
        except Exception as e:
            logger.exception("Failed to update search result info")    
    
            
