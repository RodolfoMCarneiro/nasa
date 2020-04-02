from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import regexp_extract
sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)


import re
import logging

#iniciando o arquivo de log
logging.basicConfig(filename='Resposta_desafio.log',level = "INFO", format = '%(message)s')
logger = logging.getLogger('respostaDesafio')

try:
    raw_df = spark.read.text('/user/raj_ops/desafio_semantix/NASA*')

except Exception as e:
    logger.info(e)

#iniciando a estruturacao dos dados para um dataframe
try:
    initial_df = raw_df.select(regexp_extract('value', r'(^\S+\.[\S+\.]+\S+)\s', 1).alias('Host'),
                  regexp_extract('value', r'\[(\d{2}/\w{3}/\d{4}):(\d{2}:\d{2}:\d{2}) (-\d{4})]', 1).alias('Timestamp'),
                  regexp_extract('value', r'\"(\S+)\s(\S+)\s*(\S*)\"', 2).alias('URL'),
                  regexp_extract('value', r'\s(\d{3})\s', 1).alias('CodigoRetorno'),
                  regexp_extract('value', r'(\d+)$', 1).alias('bytes'))
except Exception as e:
    logger.info(e)

#Resposta do item 1
try:
    logger.info('Resposta para o item 1:')
    logger.info(initial_df.select("Host").distinct().count())
except Exception as e:
    logger.info(e)

#Resposta do item 2:
try:
    logger.info('Resposta para o item 2:')
    logger.info(initial_df.groupby('CodigoRetorno').count().filter(initial_df.CodigoRetorno == 404).collect()[0]['count'])
except Exception as e:
    logger.info(e)

#Resposta do item 3:
try:
    logger.info('Resposta do item 3:')
    host_retorno_df = initial_df.filter(initial_df.CodigoRetorno == 404).groupby('Host').count().sort('count', ascending = False).limit(5)
    logger.info(host_retorno_df._jdf.showString(host_retorno_df.count(), 100, False))
except Exception as e:                                                                                                                                           
    logger.info(e)

#Resposta do item 4:
try:
    logger.info('Resposta do item 4:')
    data_retorno_df = initial_df.filter(initial_df.CodigoRetorno == 404).groupby('Timestamp').count().sort('count', ascending = False)
    logger.info(data_retorno_df._jdf.showString(data_retorno_df.count(), 20, False))
except Exception as e:                                                                                                                                           
    logger.info(e) 

#Resposta do item 5:
try:
    logger.info('Resposta do item 5:')  
    initial_df = initial_df.withColumn('bytes', initial_df.bytes.cast('float'))
    logger.info(initial_df.groupBy().sum('bytes').collect()[0][0])
except Exception as e:                                                                                                                                           
    logger.info(e) 
