from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

import json
import os
import sys

print("__name__: ", __name__)

if __name__ == "__main__":
    print("sys.argv: ", sys.argv)
    config_file = ""

    if len(sys.argv) < 2:
        print("Usage: spark-submit my_spark_app.py <input_path> <output_path>")
        sys.exit(1)
    else:
        config_file = sys.argv[1]

    print("config_file: ", config_file)

    f = open(config_file,)
    config = json.load(f)
    f.close()

    source = config["source"]
    destination = config["destination"]
    processNoOfRows = config["processNoOfRows"]
    sparkMaster = config["sparkMaster"]
    sparkJars = config["sparkJars"]
    checkpointLocation = config["checkpointLocation"]
    kafkaBroker = config["kafkaBroker"]
    kafkaTopics = config["kafkaTopics"]
    sparkExecutorMemory = config["sparkExecutorMemory"]
    sparkDriverMemory = config["sparkDriverMemory"]
    sparkExecutorCores = config["sparkExecutorCores"]
    sparkCoresMax = config["sparkCoresMax"]
    preloadTables = config["preloadTables"]

    print("source: ", source)
    print("destination: ", destination)
    print("processNoOfRows: ", processNoOfRows)
    print("sparkMaster: ", sparkMaster)
    print("sparkJars: ", sparkJars)
    print("checkpointLocation: ", checkpointLocation)
    print("kafkaBroker: ", kafkaBroker)
    print("kafkaTopics: ", kafkaTopics)
    print("sparkExecutorMemory: ", sparkExecutorMemory)
    print("sparkDriverMemory: ", sparkDriverMemory)
    print("sparkExecutorCores: ", sparkExecutorCores)
    print("sparkCoresMax: ", sparkCoresMax)
    print("preloadTables: ", preloadTables)

    active_spark_session = SparkSession.getActiveSession()
        
    if active_spark_session:
        active_spark_session.stop()
    
    # Initialize Spark Session
    spark = (
        SparkSession
            .builder
            .config(
                    "spark.driver.host", 
                    "localhost"
                )
            .master(sparkMaster)
            .config("spark.jars", sparkJars)
            .config("spark.executor.memory", sparkExecutorMemory)
            .config("spark.driver.memory", sparkDriverMemory)
            .config("spark.executor.cores", sparkExecutorCores)
            .config("spark.cores.max", sparkCoresMax)
            .appName("InsertTestDBMessages")
            .getOrCreate()
    )

    spark.sparkContext.setLogLevel('WARN')

    # Establish destinaton connecttion
    df = (
        spark.read
            .format("jdbc")
            .option("url", destination["url"])
            .option("dbtable", "(SELECT TempCol = 1) AS Temp")
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .load()
    )

    def get_ending_offset(table):
        connection = spark._jvm.java.sql.DriverManager.getConnection(destination["url"])
        
        statement = connection.createStatement()
        
        sqlGetEndingOffset = f"""
            DECLARE @ending_offset INT = NULL

            SELECT
                @ending_offset = [ending_offset]
            FROM
            (
                SELECT TOP 1
                    [ending_offset]
                FROM 
                    [dbo].[load_history]
                WHERE
                    [table_name] = '{table["name"]}' AND
                    [is_preloaded] = 1
            ) offset

            SELECT [ending_offset] = ISNULL(@ending_offset, -1)

        """

        print("sqlGetEndingOffset: ", sqlGetEndingOffset)

        result = statement.executeQuery(sqlGetEndingOffset)
        
        result.next()

        endingOffset = result.getInt("ending_offset")

        print("endingOffset: ", endingOffset)

        return endingOffset

    startingOffsets = {}
    
    for table in preloadTables:
        if bool(table["isArchive"]) == False:
            ending_offset = get_ending_offset(table)

            if ending_offset < 0:
                raise Exception(f"Table {table} not yet pre-loaded.")
            
            startingOffsets[table["kafkaTopic"]] = {"0":ending_offset}

    print("startingOffsets: ", startingOffsets)

    # Read data from Kafka
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBroker)
        .option("subscribe", ",".join(kafkaTopics))
        .option("startingOffsets", json.dumps(startingOffsets))
        .load()
    )

    def foreach_batch_function(df, epoch_id):
        config["startingTimeStamp"] = str(int(datetime.now().timestamp() * 1000))
        messages = df.selectExpr("CAST(value AS STRING) as json_value").collect()

        json_messages = []
        for message in messages:
            if message.json_value is not None:
                json_message = json.loads(message.json_value)
                json_messages.append({
                    "payload": json_message["payload"] 
                })

        print("json_messages: ", json_messages)

        connection = spark._jvm.java.sql.DriverManager.getConnection(destination["url"])
        
        # insert messages
        insertMessagesSql = f"EXEC [dbo].[InsertMessages] ?, 0"
        insertStatement = connection.prepareStatement(insertMessagesSql)
        insertStatement.setString(1, json.dumps(json_messages))
        insertStatement.executeUpdate()

    (
        raw_df.writeStream
            .foreachBatch(foreach_batch_function)
            .option("checkpointLocation", checkpointLocation) \
            .start()
            .awaitTermination()
    )
