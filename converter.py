import os

import findspark
from pyspark.sql.types import StructType

from config import CONFIG

findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName(CONFIG['APP_NAME']).getOrCreate()

"""
Carrega o arquivo de entrada (load.csv), realiza mapeamento do esquema conforme o arquivo JSON (ex., types_mapping.json)

@see config.py para ter acesso às configurações dos arquivos que são carregados 
"""
def convert_data_types_and_formats():

    ## Obtém a lista de campos na ordem original presente no CSV de entrada
    fields1 = spark.read.csv(CONFIG['INPUT_FILE_1'], header=False).first()
    fields2 = spark.read.csv(CONFIG['INPUT_FILE_2'], header=False).first()
    fields = [f for f in fields1]

    ## Carrega o JSON contendo uma lista indicando o tipo de dado das colunas mapeadas
    type_mapping = spark.read.json(CONFIG['TYPES_MAPPING'], multiLine=True)
    schema_mappping = spark.read.json(CONFIG['SCHEMA_MAPPING'], multiLine=True)

    ## Constrói um dicionário fazendo a mescla com os dados que foram definidos no JSON
    ## Isso é necessário para permitir que elementos do esquema definido no JSON sejam descritos em qualquer ordem
    schema_dict = next(map(lambda row: row.asDict(), type_mapping.collect()))
    schema_dict = _transform_dict(_create_dict(fields, schema_dict))
    schema = StructType.fromJson(schema_dict)

    ## De posse do esquema pronto a ser utilizado, é feito o carregamento dos dados do CSV
    df1 = spark.read.csv(CONFIG['INPUT_FILE_1'], header=True, mode="DROPMALFORMED", schema=schema)
    df2 = spark.read.csv(CONFIG['INPUT_FILE_2'], header=True, inferSchema=True)
    df2 = df2.drop("internal_id")

    df1.show()
    df2.show()

    ## Colunas correspondentes são mapeadas e os dois conjuntos são mesclados
    for source, target in zip(schema_mappping.columns, schema_mappping.collect().pop()):
        df2 = df2.withColumnRenamed(source, target)
    df3 = df1.unionByName(df2)

    ## Exibe o resultado (note que o esquema segue o que foi definido no arquivo JSON)
    df3.show()
    print(df3.printSchema)

    ## Salva os dados carregados como Parquet no diretório indicado
    if not os.path.isdir(CONFIG['OUTPUT_PATH']):
        df3.write.parquet(CONFIG['OUTPUT_PATH'])

def _transform_dict(d):
    """
    Recebe um dicionário e retorna a versão mapeada para ser utilizada como StructField

    :param d: o dicionário a ser transformado, contendo como chave o nome da coluna e como valor o tipo de dado
    :return: versão de dicionário compatível com os campos da StructType
    """
    newdict = {}
    fields = []
    for k,v in d.items():
        item = {}
        item['name'] = k
        item['type'] = v
        item['nullable'] = True
        item['metadata'] = {}
        fields.append(item)
    newdict['fields'] = fields
    newdict['type'] = 'struct'
    return newdict

def _create_dict(fields, schema_dict):
    """"
    Cria um dicionário a partir da lista completa de campos lidos do CSV e dos itens mapeados no JSON
    Caso um elemento presente no CSV não seja mapeado no JSON, o seu tipo de dado é atribuído como string
    """
    newdict = {}
    for f in fields:
        if f in schema_dict:
            newdict[f] = schema_dict[f]
        else:
            newdict[f] = 'string'
    return newdict


convert_data_types_and_formats()