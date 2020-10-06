#
# Splunk UI module python renderer
# This module is imported by the module loader (lib.module.ModuleMapper) into
# the splunk.appserver.mrsparkle.controllers.module.* namespace.
#


# required imports
import cherrypy
import controllers.module as module

# common imports
import splunk, splunk.search, splunk.util, splunk.entity
import lib.util as util
import lib.i18n as i18n

# logging setup
import logging
logger = logging.getLogger('splunk.appserver.controllers.module.HDFSSimpleFileTable')

import math
import cgi
import re

# define standard time field name
TIME_FIELD = '_time'

RAW_FIELD = '_raw'

# define wrapper for rendering multi-value fields
MULTI_VALUE_WRAPPER = '<div class="mv">%s</div>'

# define hard limit for displaying multi-value fields
MAX_MULTI_VALUE_COUNT = 50
MAX_SPARKLINE_MV_COUNT = 101

class HDFSSimpleFileTable(module.ModuleHandler):
    '''
    Provides module content for the SimpleResultsTable.  The arguments supported
    are any params supported by the /splunk/search/jobs/<sid>/results endpoint, i.e.,

    count
    offset
    field_list
    search
    '''

    def generateResults(self, host_app, client_app, sid, count=1000,
            earliest_time=None, latest_time=None, field_list=None,
            offset=0, max_lines=None, reverse_order=0, entity_name='results',
            postprocess=None, display_row_numbers='True', show_preview='0', mark_interactive=None,
            sortField=None, sortDir=None):

        # check inputs
        count = max(int(count), 0)
        offset = max(int(offset), 0)
        display_row_numbers = splunk.util.normalizeBoolean(display_row_numbers)
        if not sid:
            raise Exception('SimpleResultsTable.generateResults - sid not passed!')

        job = splunk.search.JobLite(sid)

        # pass in any field list
        if (field_list) :
            job.setFetchOption(fieldList=field_list, show_empty_fields=False)

        if postprocess:
            job.setFetchOption(search=postprocess)

        if splunk.util.normalizeBoolean(show_preview) and entity_name == 'results':
            entity_name = 'results_preview'

        # set formatting
        job.setFetchOption(
            time_format=cherrypy.config.get('DISPATCH_TIME_FORMAT'),
            earliestTime=earliest_time,
            latestTime=latest_time,
            output_time_format=i18n.ISO8609_MICROTIME
        )

        offset_start = offset

        # these two lines are a noop, since offset=max(0,int(offset)!
        #if offset < 0 and count < abs(offset):
        #    offset_start = -count

        rs = job.getResults(entity_name, offset, count)

        if rs == None:
            return _('<p class="resultStatusMessage">The job appears to have expired or has been canceled. Splunk could not retrieve data for this search.</p>')

        #dataset = getattr(job, entity_name)[offset_start: offset+count]
        dataset = rs.results()

        #if we don't have anything....lets get out fast!
        if len(dataset) == 0:
            if rs.isPreview():
                return self.generateStatusMessage(entity_name, 'waiting', sid)
            else:
                return self.generateStatusMessage(entity_name, 'nodata', sid)

        # displayable fields; explicitly pull the _time field into the first column and _raw into the last
        fieldNames = [x for x in rs.fieldOrder() if (not x.startswith('_') or x in (TIME_FIELD, RAW_FIELD) )]
        #fieldNames = [x for x in getattr(job, entity_name).fieldOrder if (not x.startswith('_') or x == TIME_FIELD)]
        try:
            timePos = fieldNames.index(TIME_FIELD)
            fieldNames.pop(timePos)
            fieldNames.insert(0, TIME_FIELD)
        except ValueError:
            pass

        try:
            rawPos = fieldNames.index(RAW_FIELD)
            fieldNames.pop(rawPos)
            fieldNames.append(RAW_FIELD)
        except ValueError:
            pass
        try:
            locationPos = fieldNames.index('Location')
            fieldNames.pop(locationPos)
        except ValueError:
            pass

        # the client will request reverse_order=1, when it has determined
        # that we're in the special case of the 'mostly-backwards' sort order of real time search.
        # (we reverse it manually so it appears to the user 'mostly correct'.)
        # (yes, for the pedantic, correct just means "consistent with historical searches, with latest events on page 1")
        #
        # NOTE: the arithmetic of the offset and count with respect to eventAvailableCounts, will
        # already have been done on the client.  This literally just reverses the sort order of
        # the N events we're actually being asked to return.
        if (splunk.util.normalizeBoolean(reverse_order)) :
            dataset.reverse()

        # -------------------------build output ---------------------------#
        shash = hash(sid)
        output = []
        output.append('<div class="simpleResultsTableWrapper">')
        output.append('<table class="simpleResultsTable splTable')
        if (mark_interactive) :
            output.append(' enableMouseover')
        output.append('">')

        # generate headers
        output.append('<tr>')
        if display_row_numbers:
            output.append('<th class="pos"></th>')

        for field in fieldNames:
            output.append('<th><a><span class="sortLabel">%s</span> <span class="splSort%s"></span></a></th>' \
                % (cgi.escape(field), field != sortField and "None" or sortDir))
        output.append('<th>Actions</th>')


        # generate data
        for i, result in enumerate(dataset):

            isDir = str(result.get('Type')) == 'dir'
            location = str(result.get('Location'))
            output.append('<tr data-dir="%s" ' % (isDir))
            output.append('data-Location="%s">' % location.replace('"','&quot;'))

            for field in fieldNames:
                output.append('<td')

                if (mark_interactive and (field!="NULL" and field!="OTHER")) :
                    output.append(' class="d"')


                fieldValues = result.get(field, None)

                # render field values; multi-value as a list
                # cap display count to prevent blowout
                if fieldValues:
                    output.append(' field="%s">' % cgi.escape(field))

                    renderedValues = [cgi.escape(x.value) for x in fieldValues[:MAX_MULTI_VALUE_COUNT]]

                    if len(fieldValues) > MAX_MULTI_VALUE_COUNT:
                        clipCount = len(fieldValues) - MAX_MULTI_VALUE_COUNT
                        renderedValues.append(_('[and %d more values]') % clipCount)

                    # when we have multiValued fields we wrap them each in its own div elements
                    if (len(renderedValues) > 1):
                        multiValueStr = [MULTI_VALUE_WRAPPER % x for x in renderedValues]
                        output.append("".join(multiValueStr))
                    # however for single values the extra div is unwanted.
                    else:
                        if field=="Type":
                            output.append("<img src=\"" + util.make_url("/static/app/HadoopConnect/images/%s_icon.png" % (fieldValues[0])) + "\"/>")
                        else:
                            output.append("".join(renderedValues))

                    output.append('</span></td>')

                else:
                    output.append('></td>')

            input_type = 'hdfs' if location.startswith('hdfs://') else 'monitor'
            path = re.sub('^\w+://', '', location) 
            get_args = {'ns': client_app, 'action': 'edit', 'def.name': path} 
            if input_type == 'monitor':
               get_args['preflight'] = 'preview' #skip preview

            indexHDFSManager = util.make_url(['manager',client_app,'data','inputs', input_type ,'_new'], get_args)
            previewURI = util.make_url(['app','search','flashtimeline'], {'q': '| hdfs read %s' % ''.join(('"',location.replace('"',r'\"'),'"'))})
            output.append("<td>")
            actions = []
            if (int(cherrypy.config.get("version_label")[0])>=5):
                indexLink = []
                indexLink.append("<a target=\"_new\" href=\"")
                indexLink.append(indexHDFSManager)
                indexLink.append("\">Add as data input</a>")
                actions.append(''.join(indexLink))
            if ( str(result.get('Type')) == 'file'):
                previewLink = []
                previewLink.append("<a target=\"_new\" href=\"")
                previewLink.append(previewURI)
                previewLink.append("\">Search</a>")
                actions.append(''.join(previewLink))
            output.append(' | '.join(actions))
            output.append("</td>")

            output.append('</tr>')

        output.append('</table></div>')

        #---------------------
        #Pass the data on out
        #---------------------

        output = ''.join(output)

        return output
