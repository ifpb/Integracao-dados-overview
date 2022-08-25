
### Orientações para Execução

#### Pré-requisitos para execução:
* Apache Spark 3.3.0
* Python 3.x

#### Utilizando o Virtual Environment

    $ python3.7 -m venv venv
    $ source venv/bin/activate

Para instalar as bibliotecas que são dependências do projeto, execute o comando abaixo:

    $ pip install -r requirements.txt
    
Em seguida, execute o arquivo responsável por cada operação:

* Mapeamento de esquemas e conversão de tipos de dados e formatos (requisitos 1 e 3):
   
    $ python converter.py
    
* Deduplicação (requisito 2):

    $ python deduplicar.py

* Exemplos de operações com os dados prontos:
    
    $ python operacoes.py

### Funcionalidades demonstradas

1. Mapeamento dos esquemas dos arquivos CSVs presentes no diretório data/input/users através da correlação das colunas correspondentes (seguindo o arquivo schema_mapping.json presente na pasta config);

2. Conversão do formato dos arquivo integrado para um formato colunar de alta performance de leitura (Parquet).

3. Deduplicação dos dados convertidos: No conjunto de dados convertidos há múltiplas entradas para um mesmo registro, variando apenas os valores de alguns dos campos entre elas. Foi necessário realizar um processo de deduplicação destes dados, a fim de apenas manter a última entrada de cada registro, usando como referência o id para identificação dos registros duplicados e a data de atualização (update_date) para definição do registro mais recente;

4. Conversão do tipo dos dados deduplicados: No diretório config há um arquivo JSON de configuração (types_mapping.json), contendo os nomes dos campos e os respectivos tipos desejados de output. Utilizando esse arquivo como input, foi realizado um processo de conversão dos tipos dos campos descritos, no conjunto de dados deduplicados.

### Notas gerais
- Todas as operações foram realizadas utilizando Spark.

- Cada operação utilizou como base o dataframe resultante do passo anterior, sendo persistido em arquivos Parquet.

- Houve a transformação de tipos de dados em alguns campos (id, age, create_date, update_date)

### Referências

[1] PLASE, D.; NIEDRITE, L.; TARANOVS, R. A comparison of HDFS compact data formats: Avro versus Parquet / HDFS glaustųjų duomenų formatų palyginimas: Avro prieš Parquet. Mokslas – Lietuvos ateitis / Science – Future of Lithuania, v. 9, n. 3, p. 267-276, 4 jul. 2017.

