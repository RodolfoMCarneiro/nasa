from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import regexp_extract, col, isnan, when, trim
sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)


import re
import logging
import sys

#iniciando o arquivo de log
logging.basicConfig(filename='Resposta_desafio.log',level = "INFO", format = '%(message)s')
logger = logging.getLogger('respostaDesafio')

try:
    raw_df = spark.read.text('/user/raj_ops/desafio_semantix/NASA*')

except Exception as e:
    logger.info('Não foi possível importar os arquivos devido ao erro:')
    logger.info(e)
    sys.exit(1)
#iniciando a estruturacao dos dados para um dataframe
try:
    initial_df = raw_df.select(regexp_extract('value', r'(^\S+\.[\S+\.]+\S+)\s', 1).alias('Host'),
                  regexp_extract('value', r'\[(\d{2}/\w{3}/\d{4}):(\d{2}:\d{2}:\d{2}) (-\d{4})]', 1).alias('Timestamp_data'),
                  regexp_extract('value', r'\[(\d{2}/\w{3}/\d{4}):(\d{2}:\d{2}:\d{2}) (-\d{4})]', 2).alias('Timestamp_hora'),
                  regexp_extract('value', r'\[(\d{2}/\w{3}/\d{4}):(\d{2}:\d{2}:\d{2}) (-\d{4})]', 3).alias('Timestamp_fuso'),
                  regexp_extract('value', r'\"(\S+)\s(\S+)\s*(\S*)\"', 2).alias('URL'),
                  regexp_extract('value', r'\s(\d{3})\s', 1).alias('CodigoRetorno'),
                  regexp_extract('value', r'\s(\d+)$', 1).alias('bytes'))
except Exception as e:
    logger.info('Nao foi possivel extrair as informacoes devido ao erro:')
    logger.info(e)
    sys.exit(1)

#Transformando os dados vazios em nulos no dataframe
try:
    for columnName in initial_df.columns:
        initial_df = initial_df.withColumn(columnName, trim(col(columnName)))

    for columnName in initial_df.columns:
        initial_df = initial_df.withColumn(columnName, when(col(columnName) != "", col(columnName)).otherwise(None))
except Exception as e:
    logger.info('As celulas nao puderam ser transformadas em nulo')

#Contagem de nulos no dataframe

null_df = initial_df.filter(initial_df['Host'].isNull() | 
                            initial_df['Timestamp_data'].isNull() |
                            initial_df['Timestamp_hora'].isNull() |
                            initial_df['Timestamp_fuso'].isNull() |
                            initial_df['URL'].isNull() |
                            initial_df['CodigoRetorno'].isNull() |
                            initial_df['bytes'].isNull())
countNull = null_df.count()
countInitial = initial_df.count()
lossPercent = int((countNull/countInitial)*100)
logger.info('Existem %i linhas com valores nulos no dataframe inicial' %(countNull))
logger.info('Sera removida uma porcentagem de %i do dataframe inicial' %(lossPercent))

filtered_df = initial_df.filter(initial_df['Host'].isNotNull() & 
                            initial_df['Timestamp_data'].isNotNull() &
                            initial_df['Timestamp_hora'].isNotNull() &
                            initial_df['Timestamp_fuso'].isNotNull() &
                            initial_df['URL'].isNotNull() &
                            initial_df['CodigoRetorno'].isNotNull() &
                            initial_df['bytes'].isNotNull())

countFiltered = filtered_df.count()
logger.info('A analise prosseguira com %i linhas no dataframe' %(countFiltered))

logger.info('================= Respostas dos desafios =================')
logger.info('As respostas apresentadas a seguir possuem, separadamente, informacoes de registros que possuem todas as informacoes e registros que estao sem alguma informacao')
logger.info('Ao final das resposta, serao exibidos alguns registros que possuem falta de informacao em cada coluna')

#Resposta do item 1
logger.info('Resposta para o item 1:')
try:
    hostCount_nullDf        = null_df.select("Host").distinct().count()
    hostCount_filteredDf    = filtered_df.select("Host").distinct().count()
    hostCount_total          = hostCount_nullDf + hostCount_filteredDf

    logger.info('Quantidade de hosts em registros com todas as informacoes: %i' %(hostCount_filteredDf))
    logger.info('Quantidade de hosts em registros sem todas as informacoes: %i' %(hostCount_nullDf))
    logger.info('Quantidade de hosts total: %i' %(hostCount_total))

except Exception as e:
    logger.info('Sem resposta para o item 1 devido ao seguinte erro:')
    logger.info(e)

#Resposta do item 2:
logger.info('Resposta para o item 2:')
try:
    return404_nullDf        = null_df.groupby('CodigoRetorno').count().filter(null_df.CodigoRetorno == 404).collect()[0]['count']
    return404_filteredDf    = filtered_df.groupby('CodigoRetorno').count().filter(filtered_df.CodigoRetorno == 404).collect()[0]['count']
    return404_total         = return404_nullDf + return404_filteredDf
    
    logger.info('Quantidade de erros 404 em registros com todas as informacoes: %i' %(return404_filteredDf))
    logger.info('Quantidade de erros 404 em registros sem todas as informacoes: %i' %(return404_nullDf))
    logger.info('Quantidade de erros 404 total: %i' %(return404_total))

except Exception as e:
    logger.info('Sem resposta para o item 2 devido ao seguinte erro:')
    logger.info(e)

#Resposta do item 3:
logger.info('Resposta do item 3:')

try:
    host_returnFull_df      = filtered_df.filter(filtered_df.CodigoRetorno == 404).groupby('Host').count().sort('count', ascending = False).limit(5)
    host_returnNull_df      = null_df.filter(null_df.CodigoRetorno == 404).groupby('Host').count().sort('count', ascending = False).limit(5)
    host_returnOriginal_df  = initial_df.filter(initial_df.CodigoRetorno == 404).groupby('Host').count().sort('count', ascending = False).limit(5)
    
    logger.info('Contagem baseada nos registros com todas as informacoes:')
    logger.info(host_returnFull_df._jdf.showString(host_returnFull_df.count(), 100, False))
    logger.info('Contagem baseada nos registros sem todas as informacoes:')
    logger.info(host_returnNull_df._jdf.showString(host_returnNull_df.count(), 100, False))
    logger.info('Contagem baseada nos registros originais:')
    logger.info(host_returnOriginal_df._jdf.showString(host_returnOriginal_df.count(), 100, False))

except Exception as e: 
    logger.info('Sem resposta para o item 3 devido ao seguinte erro:')
    logger.info(e)

#Resposta do item 4:
logger.info('Resposta do item 4:')

try:
    date_returnFull_df      = filtered_df.filter(filtered_df.CodigoRetorno == 404).groupby('Timestamp').count().sort('count', ascending = False)
    date_returnNull_df      = null_df.filter(null_df.CodigoRetorno == 404).groupby('Timestamp').count().sort('count', ascending = False)
    date_returnOriginal_df  = initial_df.filter(initial_df.CodigoRetorno == 404).groupby('Timestamp').count().sort('count', ascending = False)
    
    logger.info('Contagem baseada nos registros com todas as informacoes:')
    logger.info(date_returnFull_df._jdf.showString(date_returnFull_df.count(), 20, False))
    logger.info('Contagem baseada nos registros com todas as informacoes:')
    logger.info(date_returnNull_df._jdf.showString(date_returnNull_df.count(), 20, False))
    logger.info('Contagem baseada nos registros originais:')
    logger.info(date_returnOriginal_df._jdf.showString(date_returnOriginal_df.count(), 20, False))

except Exception as e:
    logger.info('Sem resposta para o item 4 devido ao seguinte erro:')                                                                                                                                           
    logger.info(e) 

#Resposta do item 5:
logger.info('Resposta do item 5:') 

try:
    bytes_returnFull_df = filtered_df.withColumn('bytes', filtered_df.bytes.cast('float'))
    bytes_returnNull_df = null_df.withColumn('bytes', null_df.bytes.cast('float'))
    bytes_returnOriginal_df = initial_df.withColumn('bytes', initial_df.bytes.cast('float'))
    logger.info('Contagem baseada nos registros com todas as informacoes:')
    logger.info(bytes_returnFull_df.groupBy().sum('bytes').collect()[0][0])
    logger.info('Contagem baseada nos registros sem todas as informacoes:')
    logger.info(bytes_returnNull_df.groupBy().sum('bytes').collect()[0][0])
    logger.info('Contagem baseada nos registros originais:')
    logger.info(bytes_returnOriginal_df.groupBy().sum('bytes').collect()[0][0])

except Exception as e:  
    logger.info('Sem resposta para o item 5 devido ao seguinte erro:')                                                                                                                                         
    logger.info(e) 


logger.info('================= Detalhamento das informacoes que faltam em cada uma das colunas =================') 
logger.info('Coluna Host:')

example_df = initial_df.filter(initial_df['Host'].isNull())
logger.info(example_df._jdf.showString(10, 20, False))

logger.info('Coluna Timestamp:')

example_df = initial_df.filter(initial_df['Timestamp'].isNull())
logger.info(example_df._jdf.showString(10, 20, False))

logger.info('Coluna URL:')

example_df = initial_df.filter(initial_df['URL'].isNull())
logger.info(example_df._jdf.showString(10, 20, False))

logger.info('Coluna CodigoRetorno:')

example_df = initial_df.filter(initial_df['CodigoRetorno'].isNull())
logger.info(example_df._jdf.showString(10, 20, False))

logger.info('Coluna bytes:')

example_df = initial_df.filter(initial_df['bytes'].isNull())
logger.info(example_df._jdf.showString(10, 20, False))