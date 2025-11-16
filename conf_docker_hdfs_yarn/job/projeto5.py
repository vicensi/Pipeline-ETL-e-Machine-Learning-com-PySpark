# Otimização de Pipeline ETL e Machine Learning com PySpark
# Módulo Principal do Pipeline 

# Imports
import os
import traceback
import pyspark 
from pyspark.sql import SparkSession
from p5_log import grava_log
from p5_processamento import limpa_transforma_dados
from p5_ml import cria_modelos_ml

# Cria a sessão e grava o log no caso de erro
try:
	spark = SparkSession.builder.appName("Projeto5").getOrCreate()
	spark.sparkContext.setLogLevel("ERROR")
except:
	grava_log("Log   - Ocorreu uma falha na Inicialização do Spark.")
	grava_log(traceback.format_exc())
	raise Exception(traceback.format_exc())

# Grava o log
 grava_log("\nLog   - Iniciando o Projeto 5.")
 grava_log("Log   - Spark Inicializado.")

# Bloco de limpeza e transformação
try:
	DadosHTFfeaturized, DadosTFIDFfeaturized, DadosW2Vfeaturized =  limpa_transforma_dados(spark)
except:
	 grava_log("Log   - Ocorreu uma falha na limpeza e transformação dos dados.")
	 grava_log(traceback.format_exc())
	spark.stop()
	raise Exception(traceback.format_exc())

# Bloco de criação dos modelos de Machine Learning
try:
	 cria_modelos_ml(spark, DadosHTFfeaturized, DadosTFIDFfeaturized, DadosW2Vfeaturized)
except:
	 grava_log("Log   - Ocorreu Alguma Falha ao Criar os Modelos de Machine Learning.")
	 grava_log(traceback.format_exc())
	spark.stop()
	raise Exception(traceback.format_exc())

# Grava o log
 grava_log("Log   - Modelos Criados e Salvos com Sucesso.")

# Grava o log
 grava_log("Log   - Processamento Finalizado com Sucesso.\n")

# Finaliza o Spark (encerra o cluster)
spark.stop()



