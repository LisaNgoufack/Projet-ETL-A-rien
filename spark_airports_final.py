

# Créer le nouveau script avec les bonnes permissions
docker exec -it spark-worker bash -c 'cat > /tmp/spark_airports_final.py << "EOF"
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configuration Spark avec les jars Kafka
spark = SparkSession.builder \
    .appName("AirportsKafkaProcessor") \
    .config("spark.jars", 
            "/opt/spark/additional-jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,"
            "/opt/spark/additional-jars/kafka-clients-3.4.0.jar,"
            "/opt/spark/additional-jars/postgresql-42.5.0.jar") \
    .getOrCreate()

try:
    print("1. 📖 Lecture des données depuis Kafka...")
    
    # Lire depuis Kafka
    df_kafka = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "airports") \
        .option("startingOffsets", "earliest") \
        .load()
    
    count = df_kafka.count()
    print(f"   ✅ {count} messages trouvés dans Kafka")
    
    if count > 0:
        print("2. 🔍 Échantillon des données brutes...")
        df_kafka.selectExpr("CAST(value AS STRING) as json_data").show(3, truncate=False)
        
        print("3. 🛠️  Parsing des données JSON...")
        
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("icao", StringType(), True),
            StructField("lon", StringType(), True),
            StructField("id", StringType(), True),
            StructField("lat", StringType(), True)
        ])
        
        # Parser le JSON
        df_parsed = df_kafka.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        print("   ✅ Données parsées (avant nettoyage):")
        df_parsed.show(5)
        
        print("4. 🧹 Nettoyage des données...")
        # Convertir et filtrer les données valides
        df_clean = df_parsed \
            .withColumn("latitude", 
                       when((col("lat") != "") & (col("lat").isNotNull()), 
                            col("lat").cast(DoubleType())).otherwise(lit(None))) \
            .withColumn("longitude", 
                       when((col("lon") != "") & (col("lon").isNotNull()), 
                            col("lon").cast(DoubleType())).otherwise(lit(None))) \
            .filter(col("latitude").isNotNull() & col("longitude").isNotNull())
        
        clean_count = df_clean.count()
        print(f"   ✅ {clean_count} enregistrements valides après nettoyage")
        df_clean.select("id", "name", "icao", "latitude", "longitude").show(5)
        
        print("5. 📊 Statistiques...")
        df_clean.select(
            count("*").alias("total_records"),
            countDistinct("id").alias("unique_airports"),
            avg("latitude").alias("avg_latitude"),
            avg("longitude").alias("avg_longitude"),
            min("latitude").alias("min_latitude"),
            max("latitude").alias("max_latitude")
        ).show()
        
        print("6. 💾 Écriture dans PostgreSQL...")
        df_clean.select("id", "name", "icao", "latitude", "longitude") \
            .write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/mydb") \
            .option("dbtable", "airports") \
            .option("user", "admin") \
            .option("password", "admin") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        
        print("   ✅ Données écrites dans PostgreSQL")
        
        print("7. ✅ Vérification finale...")
        df_pg = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/mydb") \
            .option("dbtable", "airports") \
            .option("user", "admin") \
            .option("password", "admin") \
            .load()
        
        pg_count = df_pg.count()
        print(f"   ✅ {pg_count} enregistrements dans PostgreSQL")
        df_pg.show(3)
        
    else:
        print("❌ Aucune donnée dans Kafka")
        
except Exception as e:
    print(f"❌ Erreur: {e}")
    import traceback
    traceback.print_exc()

finally:
    spark.stop()
    print("🎉 TRAITEMENT TERMINÉ AVEC SUCCÈS")
