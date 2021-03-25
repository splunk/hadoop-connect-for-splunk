from splunk.models.base import SplunkAppObjModel
import sys,os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..",".."))
import errors

'''
Change models to use error codes for their error messages
'''


class HDFSAppObjModel(SplunkAppObjModel):
    @classmethod
    def parse_except_messages(cls, e):
        """
        replace the default parse_except_messages since we don't want each of:
            messages, msg, and extendedMessages
        We raise three variant exception-like objects. 
        Does it's best to extract a list of message strings.
        """
        messages = []
        if hasattr(e, 'extendedMessages') and e.extendedMessages:
            if isinstance(e.extendedMessages, basestring):
                messages.append(errors.getMessageFromRestError(e.extendedMessages))
            else:
                for item in e.extendedMessages:
                    messages.append(errors.getMessageFromRestError(item.get('text')))
        elif hasattr(e, 'msg') and e.msg:
            if isinstance(e.msg, basestring):
                messages.append(errors.getMessageFromRestError(e.msg))
            elif isinstance(e.msg, list):
                for item in e.msg:
                    if isinstance(item, dict):
                        messages.append(errors.getMessageFromRestError(item.get('text')))
                    else:
                        messages.append(errors.getMessageFromRestError(e.msg[item]))
        elif hasattr(e, 'message') and e.message:
            if isinstance(e.message, basestring):
                messages.append(errors.getMessageFromRestError(e.message))
            elif isinstance(e.message, list):
                for item in e.message:
                    if isinstance(item, dict):
                        messages.append(errors.getMessageFromRestError(item.get('text')))
                    else:
                        messages.append(errors.getMessageFromRestError(e.message[item]))
        return list(set(messages)) 

