import sys,os
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from hdfs_base import HDFSAppObjModel
from splunk.models.field import Field, BoolField, IntField

'''
Provides object mapping for the hdfs clusters
'''


class Cluster(HDFSAppObjModel):

    resource = 'clusters'
    hadoop_home = Field()
    java_home = Field()
    uri = Field()
    namenode_http_port = IntField()
    authentication_mode = Field()
    authorization_mode = BoolField()
    kerberos_principal = Field()
    kerberos_service_principal = Field()
    auth_to_local = Field()
    ha = BoolField()
    hdfs_site = Field()


    def isSecure(self):
        return self.kerberos_principal != None and self.kerberos_principal != ''

    def isHaEnabled(self):
        return self.ha == 1

    def getURI(self):
        if self.uri == None or self.uri.strip() == '':
           return 'hdfs://' + self.name
        return self.uri

    def isLocallyMounted(self):
        return self.getURI().startswith('file://')

