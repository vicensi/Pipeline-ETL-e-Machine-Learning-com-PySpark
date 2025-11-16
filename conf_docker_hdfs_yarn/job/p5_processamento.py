# Otimização de Pipeline ETL e Machine Learning com PySpark
# Módulo de Processamento do Pipeline (ETL)

# Imports
import os
import os.path
import numpy
from pyspark.ml.feature import * 
from pyspark.sql import functions
from pyspark.sql.functions import * 
from pyspark.sql.types import StringType,IntegerType
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from p5_log import grava_log

# Define uma função para calcular a quantidade e a porcentagem de valores nulos em cada coluna de um DataFrame
def calcula_valores_nulos(df):
    
    # Inicializa uma lista vazia para armazenar o resultado de contagem de nulos
    null_columns_counts = []
    
    # Conta o número total de linhas no DataFrame
    numRows = df.count()

    # Itera sobre cada coluna no DataFrame
    for k in df.columns:
        
        # Conta o número de linhas nulas na coluna atual
        nullRows = df.where(col(k).isNull()).count()
        
        # Verifica se o número de linhas nulas é maior que zero
        if(nullRows > 0):
            
            # Cria uma tupla com o nome da coluna, número de nulos e a porcentagem de nulos
            temp = k, nullRows, (nullRows / numRows) * 100
            
            # Adiciona a tupla à lista de resultados
            null_columns_counts.append(temp)

    # Retorna a lista de colunas com a contagem e porcentagem de valores nulos
    return null_columns_counts

# Função para limpeza e transformação
def limpa_transforma_dados(spark, path=None):
	
	# Pasta raiz no HDFS
	if path is none:
		path = "/opt/spark/data/"

		# Grava no log
		grava_log("Log  - Importando os dados...")

		# Carrega o arquivo CSV
		reviews = spark.read.csv(path + 'dataset.csv', header=True, escape="\"")

		# Grava no log
		grava_log("Log - Dados Importados com Sucesso.")
		grava_log("Log - Total de Registros: " + str(reviews.count()))
		grava_log("Log - Verificando se Existem Dados Nulos.")
	else:
		path = path
		# Grava no log
		grava_log("Log - Importando os novos dados...")
		# Carrega o arquivo CSV
		reviews = spark.read.csv(path + 'novo_dataset.csv', header=True, escape="\"")

		# Grava no log
		grava_log("Log  - Novos Dados Importados com Sucesso.")
		grava_log("Log  - Total de Registros: " + str(reviews.count()))
		

	# Calcula os valores ausentes
	null_columns_calc_list = dsa_calcula_valores_nulos(reviews)

	# Ação com base nos valores ausentes
	if (len(null_columns_calc_list) > 0):
		for column in null_columns_calc_list:
			dsa_grava_log("Coluna " + str(column[0]) + " possui " + str(column[2]) + " de dados nulos")
		reviews = reviews.dropna()
		grava_log("Dados nulos excluídos")
		grava_log("Log  - Total de Registros Depois da Limpeza: " + str(reviews.count()))
	else:
		grava_log("Log  - Valores Ausentes Nao Foram Detectados.")

	# Grava no log
	grava_log("Log  - Verificando o Balanceamento de Classes.")
	
	# Conta os registros de avaliações positivas e negativas
	count_positive_sentiment = reviews.where(reviews['sentiment'] == "positive").count()
	count_negative_sentiment = reviews.where(reviews['sentiment'] == "negative").count()

	# Grava no log
	grava_log("Log  - Existem " + str(count_positive_sentiment) + " reviews positivos e " + str(count_negative_sentiment) + " reviews negativos.")

	# Cria o dataframe
	df = reviews

	# Grava no log
	grava_log("Log DSA - Transformando os Dados.")
	
	# Cria o indexador
	indexer = StringIndexer(inputCol="sentiment", outputCol="label")
	
	# Treina o indexador
	df = indexer.fit(df).transform(df)

	# Grava no log
	grava_log("Log  - Limpeza dos Dados.")
	
	# Remove caracteres especiais dos dados de texto
	df = df.withColumn("review", regexp_replace(df["review"], '<.*/>', ''))
	df = df.withColumn("review", regexp_replace(df["review"], '[^A-Za-z ]+', ''))
	df = df.withColumn("review", regexp_replace(df["review"], ' +', ' '))
	df = df.withColumn("review", lower(df["review"]))

	# Grava no log
	grava_log("Log  - Os Dados de Texto Foram Limpos.")
	grava_log("Log  - Tokenizando os Dados de Texto.")

	# Cria o tokenizador 
	regex_tokenizer = RegexTokenizer(inputCol="review", outputCol="words", pattern="\\W")

	# Aplica o tokenizador
	df = regex_tokenizer.transform(df)

	# Grava no log
	grava_log("Log  - Removendo Stop Words.")

	# Cria o objeto para remover stop words
	remover = StopWordsRemover(inputCol="words", outputCol="filtered")

	# Aplica o objeto e remove stop words
	feature_data = remover.transform(df)

	# Grava no log
	grava_log("Log  - Aplicando HashingTF.")

	# Cria e aplica o processador de texto 1
	hashingTF = HashingTF(inputCol="filtered", outputCol="rawfeatures", numFeatures=250)
	HTFfeaturizedData = hashingTF.transform(feature_data)

	# Grava no log
	grava_log("Log  - Aplicando IDF.")

	# Cria e aplica o processador de texto 2
	idf = IDF(inputCol="rawfeatures", outputCol="features")
	idfModel = idf.fit(HTFfeaturizedData)
	TFIDFfeaturizedData = idfModel.transform(HTFfeaturizedData)
	
	# Ajusta o nome dos objetos
	TFIDFfeaturizedData.name = 'TFIDFfeaturizedData'
	HTFfeaturizedData = HTFfeaturizedData.withColumnRenamed("rawfeatures","features")
	HTFfeaturizedData.name = 'HTFfeaturizedData' 

	# Grava no log
	grava_log("Log  - Aplicando Word2Vec.")

	# Cria e aplica o processador de texto 3
	word2Vec = Word2Vec(vectorSize=250, minCount=5, inputCol="filtered", outputCol="features")
	model = word2Vec.fit(feature_data)
	W2VfeaturizedData = model.transform(feature_data)

	# Grava no log
	grava_log("Log  - Padronizando os Dados com MinMaxScaler.")

	# Cria e aplica o padronizador
	scaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")
	scalerModel = scaler.fit(W2VfeaturizedData)
	scaled_data = scalerModel.transform(W2VfeaturizedData)
	
	# Ajusta o nome dos objetos
	W2VfeaturizedData = scaled_data.select('sentiment','review','label','scaledFeatures')
	W2VfeaturizedData = W2VfeaturizedData.withColumnRenamed('scaledFeatures','features')
	W2VfeaturizedData.name = 'W2VfeaturizedData'

	# Grava no log
	grava_log("Log  - Salvando os Dados Limpos e Transformados.")

	# Define o caminho para salvar o resultado (no HDFS)
	path = '/opt/spark/data/dados_processados/'

	HTFfeaturizedData.write.mode("Overwrite").partitionBy("label").parquet(path)
	TFIDFfeaturizedData.write.mode("Overwrite").partitionBy("label").parquet(path)
	W2VfeaturizedData.write.mode("Overwrite").partitionBy("label").parquet(path)

	grava_log("Log  - Dados Salvos com Sucesso.")

	return HTFfeaturizedData, TFIDFfeaturizedData, W2VfeaturizedData

	