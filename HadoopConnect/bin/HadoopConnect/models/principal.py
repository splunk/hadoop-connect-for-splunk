import sys,os
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from hdfs_base import HDFSAppObjModel
from splunk.models.field import Field


class Principal(HDFSAppObjModel):
    '''
    Provides object mapping for the principals of kerberos
    '''

    resource = 'krb5principals'
    keytab_path = Field()
