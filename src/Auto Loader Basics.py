# Databricks notebook source
from pyspark.sql.functions import lit, col

# Desmontar o ponto de montagem existente, se necessário
try:
    dbutils.fs.unmount("/mnt/paris2024")
except:
    print("O ponto de montagem não está montado.")

# Configurações da conta de armazenamento
account_name = "*****************"
container_name = "paris2024"  # Nome do contêiner
account_key = "*****************************************************" #chave de acesso

# Configuração da URL do Data Lake
wasbs_url = f"wasbs://{container_name}@{account_name}.blob.core.windows.net/"

# Configurações para a autenticação
configs = {
  f"fs.azure.account.key.{account_name}.blob.core.windows.net": account_key
}

# Montar o contêiner "paris2024" no Databricks
dbutils.fs.mount(
  source = wasbs_url,
  mount_point = "/mnt/paris2024",
  extra_configs = configs
)

# Configura o caminho de origem para o Autoloader
bronze_dir = "/mnt/paris2024/bronze/"

# Usar o Autoloader para ler os arquivos novos continuamente
df_auto = (spark.readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "csv")
           .option("cloudFiles.schemaLocation", "/mnt/paris2024/schema/")
           .option("delimiter", ";")
           .option("header", "true")
           .option("cloudFiles.inferColumnTypes", "true")
           .load(bronze_dir))


# Remover colunas indesejadas
columns_to_remove = ["_c10", "_c11", "_c12"]
df_auto = df_auto.drop(*columns_to_remove)

# Adicionar colunas faltantes com valores nulos
for column in ["Total", "Medalha"]:
    if column not in df_auto.columns:
        df_auto = df_auto.withColumn(column, lit(None))

# Reordenar as colunas para garantir a consistência
common_columns = sorted(df_auto.columns)
df_auto = df_auto.select(common_columns)

# Remover colunas 'Total' e 'Medalha'
df_auto = df_auto.drop("Total", "Medalha")

# Exibir o DataFrame combinado
df_auto.writeStream.format("console").start()



# COMMAND ----------

# Defina o caminho do diretório que o Auto Loader está monitorando
bronze_dir = "/mnt/paris2024/bronze/"

# Liste o conteúdo do diretório
files = dbutils.fs.ls(bronze_dir)

# Exiba a lista de arquivos e subdiretórios
for file in files:
    print(file.path)

