import os, os.path
import splunk.admin as admin
from kerberos import Krb5Principal
from delegating_handler import DelegatingRestHandler
from errors import *

required_args = ['keytab_path']
optional_args = ['keytab_path']
class Krb5PrincipalsRestHandler(DelegatingRestHandler):

  conf_principals = 'configs/conf-principals'
 
  def setup(self):
      if self.requestedAction == admin.ACTION_CREATE:
          for arg in required_args:
              self.supportedArgs.addReqArg(arg)
      elif self.requestedAction == admin.ACTION_EDIT:
          for arg in optional_args:
              self.supportedArgs.addOptArg(arg)
                  
  def handleList(self, confInfo):
      self.delegate(Krb5PrincipalsRestHandler.conf_principals, confInfo, method='GET')

      # add some runtime information to the items
      for name, obj in confInfo.items():
          krb5p = Krb5Principal(name)

          p      = krb5p.getPrincipalDir()
          tgt    = krb5p.getTgtCacheFile()
          keytab = krb5p.getKeytabFile()
          
          try:
              krb5p.validate()
              obj['valid'] = 1
          except:
              obj['valid'] = '0'
          obj['principal_dir']  = p
          obj['tgt_cache_file'] = tgt
          obj['principal_dir_exists']  = '0'
          obj['keytab_file'] = keytab
          obj['keytab_file_exists']    = '0'
          obj['tgt_cache_file_exists'] = '0'

          if os.path.isdir(p):
             obj['principal_dir_exists'] = '1'

	     if os.path.exists(keytab):
                obj['keytab_file_exists'] = '1'

             if os.path.exists(tgt):
                statinfo = os.stat(tgt)
                obj['tgt_cache_file_exists'] = '1'
                obj['tgt_cache_mtime'] = str(statinfo.st_mtime) 

  def handleRemove(self, confInfo):
      # delegate remove to /servicesNS/<user>/<app>/admin/conf-principals
      self.delegate(Krb5PrincipalsRestHandler.conf_principals, confInfo, method='DELETE')
      krb5p = Krb5Principal(self.callerArgs.id)
      krb5p.remove()

  def handleCustom(self, confInfo):
      method = 'GET' if self.requestedAction == admin.ACTION_LIST else 'POST'
      if self.customAction == "renew":
           krb5p = Krb5Principal(self.callerArgs.id)
           krb5p.validate()
           krb5p.setupEnv(tgtRenewTtl=0) 
      else:
           self.delegate(Krb5PrincipalsRestHandler.conf_principals, confInfo, method=method, customAction=self.customAction)

  def handleCreate(self, confInfo):
      self.handleCreateOrEdit(confInfo)

  def handleEdit(self, confInfo):
      self.handleCreateOrEdit(confInfo)
  
  def handleCreateOrEdit(self, confInfo):
      keytab_path = self.callerArgs['keytab_path'][0].strip() if 'keytab_path' in self.callerArgs else None 
      if not keytab_path:
          return
      if not os.path.exists(keytab_path):
          raise HcException(HCERR0016, {'path':keytab_path})
    
      # we don't want this saved in the conf file  
      del self.callerArgs['keytab_path']    

      krb5p = Krb5Principal(self.callerArgs.id)
      keytab_tmp = krb5p.getKeytabFile()+'.bak'
      import shutil
      try:    
          if self.requestedAction == admin.ACTION_CREATE:
              krb5p.create()
          else:
              shutil.move(krb5p.getKeytabFile(), keytab_tmp)   
          
          shutil.copyfile(keytab_path, krb5p.getKeytabFile())
          krb5p.setupEnv(tgtRenewTtl=0)  

          self.delegate(Krb5PrincipalsRestHandler.conf_principals, confInfo)
          if os.path.exists(keytab_tmp): 
              os.remove(keytab_tmp)
      except Exception as e:
          # rollback and rethrow
          if self.requestedAction == admin.ACTION_CREATE:
              krb5p.remove()
          elif os.path.exists(keytab_tmp):
              shutil.move(keytab_tmp, krb5p.getKeytabFile())
          raise e
          
# initialize the handler
admin.init(Krb5PrincipalsRestHandler, admin.CONTEXT_APP_ONLY)

