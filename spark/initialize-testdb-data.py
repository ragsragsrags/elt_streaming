from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

import json
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
    offsetPreloadTimestamp = config["offsetPreloadTimestamp"]

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
    print("offsetPreloadTimestamp: ", offsetPreloadTimestamp)
    
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
        isArchive = bool(table["isArchive"])
        tableName = table["name"]
        archiveTableName = table["archive"]
        dateKey = table["dateKey"]
        orderByKey = table["orderByKey"]

        print("isArchive: ", isArchive)
        print("tableName: ", tableName)
        print("dateKey: ", dateKey)
        print("orderByKey: ", orderByKey)

        connection = spark._jvm.java.sql.DriverManager.getConnection(source["url"])
        
        statement = connection.createStatement()

        sqlGetEndingOffset = ""
        
        if isArchive == False:
            sqlGetEndingOffset = f"""
                SELECT
                    [ending_offset] = 
                        (
                            SELECT
                                COUNT(*)
                            FROM 
                                [dbo].[{tableName}]
                            WHERE
                                {dateKey} <= CAST('{offsetPreloadTimestamp}' AS DATETIME2(7))
                        ) + 
                        (
                            SELECT
                                COUNT(*)
                            FROM 
                                [dbo].[{archiveTableName}] A
                            WHERE
                                CAST('{offsetPreloadTimestamp}' AS DATETIME2(7)) BETWEEN [valid_from] AND [valid_to] AND
                                NOT EXISTS
                                (
                                    SELECT
                                        1
                                    FROM
                                        [dbo].[{tableName}] M
                                    WHERE
                                        M.{orderByKey} = A.{orderByKey} AND
                                        M.valid_from = A.valid_to AND
			                            M.valid_from <= CAST('{offsetPreloadTimestamp}' AS DATETIME2(7))
                                )
                        )

            """
        else:
            sqlGetEndingOffset = f"""
                SELECT
                    [ending_offset] = COUNT(*)
                FROM 
                    [dbo].[{tableName}]
                WHERE
                    {dateKey} <= CAST('{offsetPreloadTimestamp}' AS DATETIME2(7))
            """

        print("sqlGetEndingOffset: ", sqlGetEndingOffset)

        result = statement.executeQuery(sqlGetEndingOffset)
        
        result.next()

        endingOffset = result.getInt("ending_offset")

        print("endingOffset: ", endingOffset)

        return endingOffset
    
    def is_table_initialized(table):
        loadHistoryInitialized = """
            (
                SELECT 
                    [ending_offset]
                FROM 
                    load_history 
                WHERE 
                    table_name LIKE '@table' AND
                    status IN ('Successful', 'Initial') AND
                    is_preloaded = 1
            ) AS loadHistoryPreloaded
        """.replace(
            "@table", 
            table["name"]
        )

        df = (
            spark.read
                .format("jdbc")
                .option("url", destination["url"])
                .option("dbtable", loadHistoryInitialized)
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .load()
        )

        is_initialized = False
        ending_offset = 0
        if df.count() > 0:
            is_initialized = True
            ending_offset = df.collect()[0]["ending_offset"]

        print("is_initialized: ", is_initialized)
        print("ending_offset: ", ending_offset)

        return is_initialized, ending_offset

    startingOffsets = {}
    startingOffsetHistories = []

    for table in preloadTables:
        initialized, ending_offset = is_table_initialized(table)
        
        if initialized == False:
            ending_offset = get_ending_offset(table)
            startingOffsetHistories.append({
                "table": table["name"],
                "startingOffset": ending_offset
            })
    
        startingOffsets[table["kafkaTopic"]] = {"0":ending_offset}

    print("startingOffsets: ", startingOffsets)

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
        insertMessagesSql = f"EXEC [dbo].[InsertMessages] ?, 1"
        insertStatement = connection.prepareStatement(insertMessagesSql)
        insertStatement.setString(1, json.dumps(json_messages))
        insertStatement.executeUpdate()

        insertOffsetLoadHistoriesSql = f"EXEC [dbo].[InsertOffsetLoadHistories] ?"
        insertStatement = connection.prepareStatement(insertOffsetLoadHistoriesSql)
        insertStatement.setString(1, json.dumps(startingOffsetHistories))
        insertStatement.executeUpdate()

        raw_df.stop()
        spark.stop()
    
    # Read data from Kafka
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBroker)
        .option("subscribe", ",".join(kafkaTopics))
        .option("startingOffsets", json.dumps(startingOffsets))
        .load()
        .writeStream
        .foreachBatch(foreach_batch_function)
        .option("checkpointLocation", checkpointLocation)
        .start()

    )

    raw_df.awaitTermination()