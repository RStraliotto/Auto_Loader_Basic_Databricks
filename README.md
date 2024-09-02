# Auto Loader Basic Databricks
Este repositório contém um exemplo básico de como configurar e usar o Auto-Loader no Databricks. O Auto-Loader é uma funcionalidade do Databricks que facilita a ingestão de dados em tempo real a partir de arquivos em um armazenamento distribuído, como o Azure Data Lake Storage (ADLS) ou o Amazon S3.

# Exemplo de Auto-Loader com Montagem de Armazenamento no Databricks

Este projeto demonstra como configurar e usar o Auto-Loader no Databricks para ler arquivos CSV de um contêiner do Azure Data Lake Storage Gen2, processar esses arquivos e exibir o DataFrame resultante.

## Descrição

O código inclui os seguintes passos:

1. **Desmontar o ponto de montagem existente:** Se houver um ponto de montagem anterior no Databricks, ele será desmontado para evitar conflitos.

2. **Configuração da conta de armazenamento:** Define as configurações necessárias para acessar o contêiner do Azure Data Lake Storage Gen2, incluindo o nome da conta, o nome do contêiner e a chave de acesso.

3. **Montagem do contêiner:** Monta o contêiner `paris2024` no Databricks para permitir o acesso aos arquivos armazenados.

4. **Configuração do Auto-Loader:** Configura o Auto-Loader para ler arquivos CSV continuamente a partir do diretório `bronze` dentro do contêiner montado.

5. **Processamento de Dados:**
   - **Remoção de Colunas Indesejadas:** Remove colunas específicas que não são necessárias para a análise.
   - **Adição de Colunas Faltantes:** Adiciona colunas faltantes com valores nulos para garantir a consistência do esquema.
   - **Reordenação e Exclusão de Colunas:** Reordena as colunas e remove colunas específicas para ajustar o DataFrame conforme necessário.

6. **Exibição do DataFrame:** Exibe o DataFrame resultante no console.
![image](https://github.com/user-attachments/assets/c22def97-0ff8-4f11-a844-bd9d67e8707a)

## Código

```python
from pyspark.sql.functions import lit, col

# Desmontar o ponto de montagem existente, se necessário
try:
    dbutils.fs.unmount("/mnt/paris2024")
except:
    print("O ponto de montagem não está montado.")

# Configurações da conta de armazenamento
account_name = "*********"
container_name = "**********"  # Nome do contêiner
account_key = "************************************************************" #chave de acesso

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
```


Requisitos
Databricks Runtime com suporte para Auto-Loader e Spark.
Conta no Azure com acesso ao Azure Data Lake Storage Gen2.
Configurações adequadas para autenticação no Azure Blob Storage.
Instruções
Prepare o Ambiente: Certifique-se de que o Databricks e o Azure Data Lake Storage Gen2 estão configurados e acessíveis.

Execute o Código: Cole o código fornecido em um notebook no Databricks e execute-o.

Monitore o Resultado: Observe o console para visualizar o DataFrame resultante do processamento.

Recursos Adicionais
Documentação do Databricks Auto-Loader
Documentação do Azure Data Lake Storage Gen2
Licença
Este projeto está licenciado sob a MIT License.
