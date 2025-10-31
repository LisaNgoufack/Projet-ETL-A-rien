from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

print("=== SPARK KAFKA AIRPORTS PROCESSOR ===")

# Configuration Spark avec les jars Kafka
spark = SparkSession.builder \
    .appName("AirportsKafkaProcessor") \
    .config("spark.jars", 
            "/opt/spark/additional-jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,"
            "/opt/spark/additional-jars/kafka-clients-3.4.0.jar,"
            "/opt/spark/additional-jars/postgresql-42.5.0.jar") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Augmenter le niveau de log pour le débogage
spark.sparkContext.setLogLevel("INFO")

try:
    print("1. 📖 Lecture des données depuis Kafka...")
    
    # Lire le stream depuis Kafka
    df_kafka = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "airports") \
        .option("startingOffsets", "earliest") \
        .load()
    
    count = df_kafka.count()
    print(f"   ✅ {count} messages trouvés dans le topic 'airports'")
    
    if count > 0:
        print("2. 📊 Affichage d'un échantillon des données brutes...")
        df_kafka.selectExpr("CAST(value AS STRING) as json_data").show(5, truncate=False)
        
        print("3. 🔧 Parsing des données JSON...")
        # Définir le schéma des données
        schema = StructType([
            StructField("icao", StringType(), True),
            StructField("name", StringType(), True),
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True)
        ])
        
        # Parser le JSON
        df_parsed = df_kafka.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        print("   ✅ Données parsées avec succès")
        df_parsed.show(10)
        
        print("4. 📈 Calcul des statistiques...")
        stats_df = df_parsed.select(
            count("*").alias("total_records"),
            countDistinct("icao").alias("unique_airports"),
            avg("lat").alias("avg_latitude"),
            avg("lon").alias("avg_longitude"),
            min("lat").alias("min_latitude"),
            max("lat").alias("max_latitude")
        )
        
        stats = stats_df.collect()[0]
        print(f"   📊 Total enregistrements: {stats['total_records']}")
        print(f"   📊 Aéroports uniques: {stats['unique_airports']}")
        print(f"   📊 Latitude moyenne: {stats['avg_latitude']:.4f}")
        print(f"   📊 Longitude moyenne: {stats['avg_longitude']:.4f}")
        print(f"   📊 Latitude min/max: {stats['min_latitude']:.4f} / {stats['max_latitude']:.4f}")
        
        print("5. 💾 Écriture dans PostgreSQL...")
        
        # S'assurer que la table existe
        df_parsed.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/mydb") \
            .option("dbtable", "airports") \
            .option("user", "admin") \
            .option("password", "admin") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        
        print("   ✅ Données écrites dans la table 'airports'")
        
        print("6. ✅ Vérification dans PostgreSQL...")
        df_pg = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/mydb") \
            .option("dbtable", "airports") \
            .option("user", "admin") \
            .option("password", "admin") \
            .load()
        
        pg_count = df_pg.count()
        print(f"   ✅ {pg_count} enregistrements vérifiés dans PostgreSQL")
        df_pg.show(5)
        
    else:
        print("❌ Aucune donnée trouvée dans le topic Kafka")
        print("💡 Vérifiez que NiFi envoie bien des données vers Kafka")
        
except Exception as e:
    print(f"❌ Erreur lors du traitement: {e}")
    import traceback
    traceback.print_exc()

finally:
    spark.stop()
    print("🎉 TRAITEMENT TERMINÉ")

print("=" * 50)
