from hdfs_base import HDFSAppObjModel
import time
import logging
from splunk.models.saved_search import SavedSearch
from splunk.models.field import Field, FloatField
from splunk.models.field import IntField, EpochField
import splunk.rest as rest

logger = logging.getLogger('splunk')

'''
Provides object mapping for HDFS export objects
'''

class HDFSExport(SavedSearch):

    resource = 'hdfs_export'
    search = Field()
    uri = Field()
    base_path = Field()
    starttime = IntField()
    next_scheduled_time = Field()
    cron_schedule = Field()
    parallel_searches = Field()
    partition_fields = Field()
    status = Field()
    compress_level = Field()
    firstevent = EpochField('status.earliest')
    lastevent = EpochField('status.latest')
    jobexporterrors = Field('status.jobs.errors')
    jobprogess = FloatField('status.jobs.progress')
    jobruntime = FloatField('status.jobs.runtime')
    jobstart = EpochField('status.jobs.starttime')
    jobend = EpochField('status.jobs.endtime')
    jobearliest = EpochField('status.jobs.earliest')
    load = Field('status.load')
    export_sids = Field('status.jobs.sids') # comma delimited list
    scheduled_sid = Field('status.jobs.psid')
    maxspan = Field()
    minspan = Field()
    roll_size = Field()
    format = Field()
    fields = Field()

    def get_export_factor(self):
        if self.load == None or len(self.load) == 0:
           return 0
        loads = self.load.split(',')
        return float(loads[0])

    def get_percent_complete(self):
        '''
            returns the percentage of index times attempted to be exported
            compared to today.
        '''
        try:
            indexTimeSpan = time.mktime(self.lastevent.timetuple()) - time.mktime(self.firstevent.timetuple())
            exportedIndexTimeSpan = time.time() - time.mktime(self.firstevent.timetuple())
            return indexTimeSpan / exportedIndexTimeSpan
        except:
            return 0

    def getErrors(self):
        if self.jobexporterrors == None or len(self.jobexporterrors) == 0:
           return []

        err = self.jobexporterrors.split(',')
        return err

    def isPaused(self):
        return not self.schedule.is_scheduled

    def execute_action(self, action_name):
        if not self.action_links:
            return False
        url = None
        url_base = None
        for item in self.action_links:
            if action_name == item[0]:
                url = item[1]
                break
            elif item[0] == 'list':
                url_base = item[1]

        # ghetto way to build the action url when not provided by endpoint
        if url == None and url_base != None:
           url = url_base + '/' + action_name

        if url == None:
           return False

        response, content = rest.simpleRequest(url, method='POST')
        return response.status == 200

    def pause(self):
        return self.execute_action('pause')

    def resume(self):
        return self.execute_action('resume')

    def force(self):
        #TODO: return the actual search id of the spawned search job
        return self.execute_action('force')

    def hasPartitionField(self, field):
        fields = self.partition_fields.split(',')
        return field in fields

    @classmethod
    def parse_except_messages(cls, e):
        return HDFSAppObjModel.parse_except_messages(e)
