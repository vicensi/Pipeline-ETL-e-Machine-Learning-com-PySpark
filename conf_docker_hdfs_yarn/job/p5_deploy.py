# Otimização de Pipeline ETL e Machine Learning com PySpark
# Módulo de Deploy



#i Imports
from pyspark.ml import PipelineModel
from p5_processamento import limpa_transforma_dados
from p5_log import grava_log


# Aplica transformacao 
_, _, DadosW2Vfeaturized = limpa_transforma_dados(spark,path="/opt/spark/novosDados/")


# Carrega modelo
def carregar_modelo(spark,path="/opt/spark/data/modelo/"):
    if not os.path.exists(path):
        raise Exception(f"Log - Caminho {path} não encontrado.")

    grava_log("Log - Carregando modelo treinado.")
    modelo = PipelineModel.load(path)
    return modelo

# Aplicando o modelo aos novos dados para fazer previsões
# Isso inclui as etapas de StringIndexer e VectorAssembler (pipeline)
previsoes = modelo.transform(DadosW2Vfeaturized)

# Salvando no HDFS em modo overwrite
previsoes_para_salvar.write.csv("hdfs:///opt/spark/data/previsoesnovosdados", mode="overwrite")