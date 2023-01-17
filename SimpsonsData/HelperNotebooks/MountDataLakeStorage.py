# Databricks notebook source
ContainerName = 'root'
account_key = 'Vd19PIUrX8pIhS0gElQfLFJSky9c62Nq41kGuNm52R5qmRjyv+JQlxULPhpwDL7y9fhmz9SMjaku+AStQTdeKA=='
storage_account = 'bedwardsdlstorage'
mount_point_name = '/mnt/datalake/'

if all(mount.mountPoint != mount_point_name for mount in dbutils.fs.mounts()):
     dbutils.fs.mount(source = f"wasbs://{ContainerName}@{storage_account}.blob.core.windows.net",mount_point = mount_point_name ,extra_configs ={f"fs.azure.account.key.{storage_account}.blob.core.windows.net":account_key})
