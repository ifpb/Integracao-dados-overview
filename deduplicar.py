import os

import pyspark.sql.functions as func
import findspark

from config import CONFIG

findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName(CONFIG['APP_NAME']).getOrCreate()

"""
Recupera a lista de usuários construída no arquivo (converter.py) e realiza a remoção das instâncias duplicadas
"""
def deduplicate():
    ## Carrega lista de usuários persistida no formato Parquet
    users = spark.read.parquet(CONFIG['OUTPUT_PATH'])

    ## Cria um grupo contendo um id único e a data de última atualização das instâncias vinculadas ao id corrente
    cluster = users.groupBy('id').agg(func.max("update_date").alias('update_date'))

    ## Faz o join do dataframe completo com o grupo, removendo as duplicatas
    users_deduplicated = users.join(cluster, ['id', 'update_date'])\
        .sort(users.id.asc())

    ## Exibe o resultado
    users_deduplicated.show()

    ## Persiste o novo dataframe em um novo Parquet
    if not os.path.isdir(CONFIG['OUTPUT_PATH_DEDUPLICATED']):
        users_deduplicated.write.parquet(CONFIG['OUTPUT_PATH_DEDUPLICATED'])
    return users_deduplicated

deduplicate()
