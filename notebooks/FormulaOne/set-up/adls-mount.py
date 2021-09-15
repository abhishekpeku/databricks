# Databricks notebook source
# MAGIC %md
# MAGIC ##### Mount formulastg with its containers

# COMMAND ----------

#dbutils.secrets.help()

# COMMAND ----------

#dbutils.secrets.list(scope = "formula-scope")
#SecretMetadata(key='formula-client-id'),
 #SecretMetadata(key='formula-client-secret'),
 #SecretMetadata(key='formula-tenant-id')
storage_account_name = "formulastg"

# COMMAND ----------

client_id=dbutils.secrets.get(scope= "formula-scope", key ="formula-client-id")
tenant_id=dbutils.secrets.get(scope= "formula-scope", key ="formula-tenant-id")
cliient_secrets=dbutils.secrets.get(scope= "formula-scope", key ="formula-client-secret")

# COMMAND ----------


#databricks secrets list-scopes

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{client_id}",
          "fs.azure.account.oauth2.client.secret": f"{cliient_secrets}",
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}



# COMMAND ----------

# Optionally, you can add <directory-name> to the source URI of your mount point.
def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

mount_adls("raw")

# COMMAND ----------

mount_adls("processed")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

