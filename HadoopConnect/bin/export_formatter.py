from constants import *
from errors import *

class ResultFormatter:
    def __init__(self, fields, format_type):
        """
        Create a formatter that will be asked to format the required result fields
        fields - a list of fields, optionally wildcarded, that need to be exported 
        """
        self.orig_fields  = fields
        self.bytes   = 0
        self.header  = []
        # a list of field names, each name is a full name, no wildcard
        self.fields  = []
        # a list of indexes, each index indicates where each field of the self.fields is located in self.header
        self.indexes = []
        self.format_type = format_type
         
    def _index(self, item):
        try:
            return self.header.index(item)
        except:
            return INVALID_INDEX

    def _isWildcardField(self, f):
        return f.find('*') >=0

    def setHeader(self, header):
        """
        Called once for each set of results that need to be formated
        """
        self.header  = header
        self.fields  = []
        self.indexes = []
        for field in self.orig_fields:
            if field != None:
                if self._isWildcardField(field):
                    import fnmatch
                    matchedFields = fnmatch.filter(header, field)
                    for matchedField in matchedFields:
                        self.fields.append(matchedField)
                        self.indexes.append(self._index(matchedField))
                elif field in header:
                    self.fields.append(field)
                    self.indexes.append(self._index(field))
            
    def format(self, row):
        """
        Called one for each result that needs to be formated, must return a string 
        that represents the formated result
        """
        try:
            result = self.formatRow(row)
        except Exception as e:
            logger.exception('Failed to format row. format=%s, row=%s', self.format_type, row)
            raise HcException(HCERR1519, {'format':self.format_type, 'error':str(e)})
        self.bytes += len(result)
        return result
    
    def formatRow(self, row):
        pass
        
    @classmethod 
    def get(cls, format_type, fields):
        if format_type == "raw":
            return RawResultFormatter(fields)
        elif format_type == "json":
            return JsonResultFormatter(fields)
        elif format_type == "xml":
            return XmlResultFormatter(fields)
        elif format_type == "csv":
            return DsvResultFormatter(fields, ',')
        elif format_type == "tsv":
            return DsvResultFormatter(fields, '\t')
        raise HcException(HCERR0503, {'name':'format', 'value':format_type, 'accepted_values':'raw, json, xml, csv, tsv'})  


class RawResultFormatter(ResultFormatter):
    def __init__(self, fields):
        if '_raw' not in fields:
            raise HcException(HCERR0502, {'name':'fields', 'value':str(fields), 'error':'_raw is not specified for raw output format'})
        if len(fields) != 1:
            raise HcException(HCERR0502, {'name':'fields', 'value':str(fields), 'error':'Only field _raw is allowed to be exported for raw output format'})
        ResultFormatter.__init__(self, fields, 'raw')
        
    def formatRow(self, row):
        return '' if self.indexes[0] < 0 else row[self.indexes[0]]


class JsonResultFormatter(ResultFormatter):
    def __init__(self, fields):
        ResultFormatter.__init__(self, fields, 'json')
        import json
        global json

    def formatRow(self, row):
        fieldsDict = {}
        for i in xrange(len(self.fields)):
            value = row[self.indexes[i]]
            if value != '':
                fieldsDict[self.fields[i]] = value
                
        return json.dumps(fieldsDict)


class XmlResultFormatter(ResultFormatter):

    def __init__(self, fields):
        ResultFormatter.__init__(self, fields, 'xml')
        from xml.etree import ElementTree
        from xml.etree.ElementTree import Element, SubElement
        global Element, SubElement, ElementTree
 
    def formatRow(self, row):
        root = Element(APP_NAME)
        for i in xrange(len(self.fields)):
            value = row[self.indexes[i]]
            if value != '':
                SubElement(root, self.fields[i]).text = value
        
        return ElementTree.tostring(root, 'utf-8')


class DsvResultFormatter(ResultFormatter):
    
    def __init__(self, fields, delimiter):
        if delimiter not in [',', '\t']:
            raise HcException(HCERR0503, {'name':'delimiter', 'value':delimiter, 'accepted_values':', \\t'})
        self.delimiter = delimiter
        ResultFormatter.__init__(self, fields, 'csv' if delimiter == ',' else 'tsv')
        for f in fields:
            if self._isWildcardField(f): 
                 raise HcException(HCERR0502, {'name':'fields', 'value':fields, 'error':'DsvResultFormatter does not support wildcard fields.'})

        
    def setHeader(self, header):    
        """
        Called once for each set of results that need to be formated. If a field is missing, INVALID_INDEX is assigned to its index. 
        """
        self.header  = header
        self.indexes = []
        for field in self.orig_fields:
            self.indexes.append(self._index(field))
    
    def formatRow(self, row):
        result = []
        for i in xrange(len(self.orig_fields)):
            # if a field is invalid or missing in a chunk, INVALID_INDEX will be assigned in setHeader call
            # note, for csv/tsv, we cannot ignore invalid, missing or empty value fields. We need to fill up a blank value to make sure each row maps to the same header
            if self.indexes[i] == INVALID_INDEX:
                value = ''
            else:
                value = row[self.indexes[i]]
            result.append(value)
                
        return self.delimiter.join(result)
