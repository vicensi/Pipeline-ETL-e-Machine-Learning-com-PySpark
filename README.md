# Pipeline-ETL-e-Machine-Learning-com-PySpark

# Instalação do Apache Spark e Preparação do Ambiente de Trabalho
# Instalação e Configuração do Cluster Spark

# Inicializar o cluster
docker-compose -f docker-compose.yml up -d --scale spark-worker-yarn=3

# Visualizar os logs
docker-compose logs

# Testar o cluster
docker exec dsa-spark-master-yarn spark-submit --master yarn --deploy-mode cluster ./examples/src/main/python/pi.py

# Derrubar o cluster
docker-compose down --volumes --remove-orphans

# Spark Master
http://localhost:9091

# History Server
http://localhost:18081

# Instruções para executar o Projeto 5


# 1 - Coloque o arquivo dataset.csv na pasta de dados do cluster Spark

# 2 - Coloque os scripts PySpark no pasta de jobs do cluster Spark

# 3 - Recrie o cluster a partir do zero. 

# 4 - Execute o comando abaixo no terminal ou prompt de comando (Vamos usar o deploy mode como client para reduzir o consumo de memória RAM):

docker exec spark-master-yarn spark-submit --deploy-mode client ./apps/projeto5.py


OBS: no docker uma forma de enviar arquivos para o HDFS podemos fazer da seguinte forma
No terminal do docker dentro do pasta o comando e hdfs dfs -put nome_arquivo.ext caminho da pasta. Exemplo /opt/spark/nome_pasta

Para listar o conteúdo salvo no hdfs podemos utilizar hdfs dfs -ls diretório	
Utilizamos -get diretorio/nome_arquivo.ext para enviar do hdfs para a maquina local verificar o caminho onde estamos executando pois sera salvo nessa pasta atual
Para visualizar: cat nome_arquivo

O arquivo de novos dados também precisao estar no hdfs pois o arquivo deploy faz a leitura de la 
