import pyspark.sql.functions as func
import findspark

from config import CONFIG

findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName(CONFIG['APP_NAME']).getOrCreate()

def operations():
    users = spark.read.parquet(CONFIG['OUTPUT_PATH_DEDUPLICATED'])
    users = users.sort(users.id.asc())
    users.show()
    print("Total de usuários = ", users.count())
    users_pd = users.toPandas()
    print("Média de idade = ", users_pd['age'].mean())
    print("Usuário mais velho = ", users.select('name', 'email', 'age').sort(users.age.desc()).first())
    print("Usuário mais novo = ", users.select('name', 'email', 'age').sort(users.age.asc()).first())

operations()