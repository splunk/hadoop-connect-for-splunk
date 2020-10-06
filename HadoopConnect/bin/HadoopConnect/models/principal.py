from hdfs_base import HDFSAppObjModel
from splunk.models.field import Field


class Principal(HDFSAppObjModel):
    '''
    Provides object mapping for the principals of kerberos
    '''

    resource = 'krb5principals'
    keytab_path = Field()
