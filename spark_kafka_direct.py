from pyspark.sql import SparkSession
from pyspark import SparkConf

# Configuration manuelle
conf = SparkConf()
conf.set("spark.jars", "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/spark/jars/kafka-clients-3.4.0.jar")
conf.set("spark.driver.extraClassPath", "/opt/spark/jars/*")
conf.set("spark.executor.extraClassPath", "/opt/spark/jars/*")

# Initialiser Spark
spark = SparkSession.builder \
    .appName("KafkaDirect") \
    .master("spark://spark-master:7077") \
    .config(conf=conf) \
    .getOrCreate()

print("=== DÉMARRAGE AVEC CONFIGURATION MANUELLE ===")

try:
    # Lire depuis Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "airports") \
        .option("startingOffsets", "earliest") \
        .load()
    
    print("✅ Connexion Kafka réussie!")
    
    # Afficher le schema
    df.printSchema()
    
    # Compter les messages
    query = df \
        .selectExpr("COUNT(*)") \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()
    
    # Attendre 15 secondes
    query.awaitTermination(15000)
    query.stop()
    
except Exception as e:
    print(f"❌ Erreur: {e}")

spark.stop()
