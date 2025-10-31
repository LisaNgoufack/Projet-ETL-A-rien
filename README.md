# Projet ETL A rien

Pipeline ETL temps‑réel pour collecter, transformer et visualiser des données aéronautiques en s’appuyant sur Kafka, NiFi, Spark, PostgreSQL et Grafana.

## Objectif

Ce projet vise à mettre en place un pipeline de données temps‑réel capable de :

* Collecter des informations sur les aéroports depuis l’API **OpenAIP** et publier ces messages dans un topic Kafka via un flux NiFi (voir `nifi/NiFi_Flow.json`).
* Traiter ces données avec Apache Spark (Batch ou Structured Streaming) : parse du JSON, nettoyage des coordonnées, calcul de statistiques et écriture dans une base PostgreSQL.
* Mettre à disposition les enregistrements nettoyés et agrégés pour la visualisation dans Grafana.
* Orchestrer l’ensemble des services grâce à un fichier Docker Compose.

## Architecture

* **Flux NiFi** (`nifi/NiFi_Flow.json`) : chaîne de processeurs qui interroge l’API OpenAIP via `InvokeHTTP`, découpe la réponse JSON avec `SplitJson`, extrait les champs id, name, icao, lon et lat avec `EvaluateJsonPath`, convertit ces attributs en JSON via `AttributesToJSON` et publie les messages dans Kafka grâce à `PublishKafka_2_0` (clé de message : `${id}`, topic : `airports`). Astuce : si vous utilisez `PublishKafkaRecord_2_0`, renseignez `Message Key Field=/id` et assurez‑vous que le `RecordReader` lit un objet (et non un tableau) ; sinon, préférez `PublishKafka_2_0` avec `Kafka Key=${id}`.
* **Apache NiFi** : outil d’orchestration des flux qui exécute le flux décrit ci‑dessus.
* **Kafka** : message broker qui reçoit les données des aéroports et les met à disposition pour Spark.
* **Spark** : deux scripts sont fournis :
  * `spark_airports_final.py` : lit le topic `airports`, parse le JSON via un schéma défini, nettoie les valeurs de latitude et longitude, calcule quelques statistiques et écrit le résultat dans PostgreSQL. Il vérifie ensuite que les écritures ont bien été effectuées et affiche un échantillon des données.
  * `spark_kafka_direct.py` : exemple minimal de Structured Streaming qui se connecte à Kafka, affiche le schéma et compte le nombre de messages reçu pendant une fenêtre de 15 secondes.
* **PostgreSQL** : base relationnelle qui stocke la table `airports` (id, name, icao, latitude, longitude).
* **Grafana** : tableau de bord permettant de visualiser les données de la base PostgreSQL.
* **Docker Compose** : définit tous les services (Zookeeper, Kafka, NiFi, Spark master/worker, PostgreSQL, Grafana) et leurs dépendances. Les ports et volumes sont configurés pour un usage local (port Kafka 9092, Spark 7077/8081, PostgreSQL 5432, Grafana 3000) et les variables d’environnement permettent de fixer les utilisateurs et mots de passe.

## Pré‑requis

* Docker et Docker Compose installés sur votre machine.
* Accès réseau pour récupérer l’API OpenAIP (clé API nécessaire).
* Python 3 n’est plus nécessaire pour publier les messages si vous utilisez le flux NiFi fourni, mais reste requis pour exécuter les scripts Spark.

## Mise en route

1. **Cloner le dépôt**

   ```bash
   git clone https://github.com/LisaNgoufack/Projet-ETL-A-rien.git
   cd Projet-ETL-A-rien
   ```

2. **Lancer l’environnement via Docker Compose**

   Exécutez :

   ```bash
   docker compose -f docker-compose-M2DATA.yml up
   ```

   Cela démarrera les services Zookeeper, Kafka, NiFi, Spark master/worker, PostgreSQL et Grafana. Patientez quelques minutes le temps que tous les services soient disponibles.

3. **Configurer le flux NiFi**

   * Ouvrez l’interface NiFi (port 8080 dans le docker‑compose) et importez le flux fourni `nifi/NiFi_Flow.json`.
   * Le flux interrogera automatiquement l’API OpenAIP et publiera les messages dans le topic Kafka `airports`.

4. **Traiter les données avec Spark**

   * **Mode batch** : exécutez `spark_airports_final.py` depuis le container Spark :

     ```bash
     docker exec -it spark-worker bash
     # une fois dans le conteneur
     spark-submit /workspace/spark_airports_final.py
     ```

     Ce script lit les messages du topic `airports`, parse les données, nettoie les coordonnées, affiche des statistiques et écrit le résultat dans PostgreSQL. Il vérifie ensuite le nombre d’enregistrements dans la table et termine proprement.

   * **Mode streaming minimal** : pour tester la connexion à Kafka en streaming, lancez `spark_kafka_direct.py` :

     ```bash
     docker exec -it spark-worker bash
     spark-submit /workspace/spark_kafka_direct.py
     ```

     Le script se connecte à Kafka, affiche le schéma du flux et compte les messages pendant 15 secondes.

5. **Visualiser les données dans Grafana**

   * Rendez‑vous sur `http://localhost:3000` et connectez‑vous avec les identifiants admin/admin (modifiable dans le docker‑compose).
   * Ajoutez PostgreSQL comme source de données (host : `postgres:5432`, base : `mydb`, user : `admin`, password : `admin`).
   * Créez un tableau de bord pour explorer la table `airports` et représenter la distribution géographique des aéroports.

## Organisation des fichiers

| Fichier / Dossier           | Rôle                                                                                                           |
| --------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `nifi_conf/`                | Configuration éventuelle pour des flux NiFi.                                                                   |
| `nifi/NiFi_Flow.json`       | Export du flux NiFi pour interroger OpenAIP et publier les messages dans Kafka.                               |
| `spark-jars/`               | Jars supplémentaires pour Spark (Kafka, PostgreSQL).                                                           |
| `docker-compose-M2DATA.yml` | Définit les services Docker (Kafka, Spark, PostgreSQL, Grafana, etc.).                                         |
| `spark_airports_final.py`   | Traitement Spark batch : lecture du topic Kafka, parsing, nettoyage, statistiques et écriture dans PostgreSQL. |
| `spark_kafka_direct.py`     | Exemple de Structured Streaming minimal avec Kafka.                                                            |

## Notes

* Le projet utilise une clé API OpenAIP qui doit être fournie à NiFi pour interroger l’API. Pour un usage en production, il est conseillé de la fournir via une variable d’environnement ou un fichier de configuration.
* Les identifiants des services (NiFi, PostgreSQL, Grafana) sont définis dans le fichier docker‑compose et peuvent être adaptés selon vos besoins.
* Assurez‑vous que les ports utilisés (9092 pour Kafka, 7077/8081 pour Spark, 5432 pour PostgreSQL, 3000 pour Grafana) sont disponibles sur votre machine.
