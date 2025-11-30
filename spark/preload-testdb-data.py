from pyspark.sql import SparkSession

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
    sparkMaster = config["sparkMaster"]
    sparkJars = config["sparkJars"]
    preloadTables = config["preloadTables"]
    retriesMax = config["retriesMax"]
    sparkExecutorMemory = config["sparkExecutorMemory"]
    sparkDriverMemory = config["sparkDriverMemory"]
    sparkExecutorCores = config["sparkExecutorCores"]
    sparkCoresMax = config["sparkCoresMax"]
    offsetPreloadTimestamp = config["offsetPreloadTimestamp"] 

    print("source: ", source)
    print("destination: ", destination)
    print("sparkMaster: ", sparkMaster)
    print("sparkJars: ", sparkJars)
    print("preloadTables: ", preloadTables)
    print("retriesMax: ", retriesMax)    
    print("sparkExecutorMemory: ", sparkExecutorMemory)
    print("sparkDriverMemory: ", sparkDriverMemory)
    print("sparkExecutorCores: ", sparkExecutorCores)
    print("sparkCoresMax: ", sparkCoresMax)
    print("offsetPreloadTimestamp: ", offsetPreloadTimestamp)

    def get_spark_session():
        return (
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
    
    def stop_spark_session():
        active_spark_session = SparkSession.getActiveSession()
        
        if active_spark_session:
            active_spark_session.stop()

    stop_spark_session()

    def get_preload_max_date():
        spark = get_spark_session()

        connection = spark._jvm.java.sql.DriverManager.getConnection(destination["url"])
        
        statement = connection.createStatement()
        
        sqlGetMaxDateFromTable = f"""
            SELECT 
                [MaxDate] = ISNULL(
                    [dbo].[GetDateTime2FromUnixEpoch](
                        CAST(MAX(JSON_VALUE(M.[after], 
                            CASE
                                WHEN [table] LIKE '%_archive' then '$.valid_to'
                                ELSE '$.valid_from'
                            END
                        )) AS BIGINT), 
                        'NANOSECOND'
                    ),
                    CAST('{offsetPreloadTimestamp}' AS DATETIME2(7))
                )
            FROM
                [dbo].[messages] M
            WHERE
                M.operation = 'r'
        """

        print("sqlGetMaxDateFromTable: ", sqlGetMaxDateFromTable)

        result = statement.executeQuery(sqlGetMaxDateFromTable)
        
        result.next()

        maxDate = result.getString("MaxDate")

        print("maxDate: ", maxDate)

        return maxDate

    def get_source_db_table(table):
        tableName = table["name"]
        archiveTableName = table["archive"]
        archiveSelect = table["archiveSelect"]
        dateKey = table["dateKey"]
        orderByKey = table["orderByKey"]
        isArchive = table["isArchive"]
        sourceDbTable = ""
        max_date = get_preload_max_date()
        
        print("tableName: ", tableName)
        print("archiveTableName: ", archiveTableName)
        print("archiveSelect: ", archiveSelect)
        print("dateKey: ", dateKey)
        print("orderByKey: ", orderByKey)
        print("isArchive: ", isArchive)
        print("max_date: ", max_date)
        
        if isArchive == False:
            sourceDbTable = f"""
                (
                    SELECT
                        *
                    FROM 
                        [dbo].[{tableName}]
                    WHERE
                        {dateKey} <= CAST('{max_date}' AS DATETIME2(7))

                    UNION ALL 

                    SELECT
                        {",".join(archiveSelect)}
                    FROM
                        (
                            SELECT 
                                [{orderByKey}] = A.[{orderByKey}],
                                [valid_from] = MAX(A.[valid_from]),
                                [valid_to] = MAX(A.[valid_to])
                            FROM 
                                [dbo].[{archiveTableName}] A
                            WHERE
                                CAST('{max_date}' AS DATETIME2(7)) BETWEEN [valid_from] AND [valid_to] AND
                                NOT EXISTS
                                (
                                    SELECT
                                        1
                                    FROM
                                        [dbo].[{tableName}] M
                                    WHERE
                                        M.{orderByKey} = A.{orderByKey} AND
                                        M.valid_from = A.valid_to AND
                                        M.valid_from <= CAST('{max_date}' AS DATETIME2(7))
                                )
                            GROUP BY
                                A.{orderByKey}
                        ) A1 JOIN
                        [dbo].[{archiveTableName}] A ON
                            A.{orderByKey} = A1.{orderByKey} 
                    WHERE
                        A1.valid_from = A.valid_from AND
                        A1.valid_to = A.valid_to
                ) AS {tableName}
            """
        else:
            sourceDbTable = f"""
                (
                    SELECT
                        *
                    FROM 
                        [dbo].[{tableName}]
                    WHERE
                        {dateKey} <= '{max_date}'
                ) AS {tableName}
            """

        return sourceDbTable
    
    def update_preloaded_status(table, ending_offset):

        retries = 0
        isSuccessful = False

        while retries < retriesMax:
            retries = retries + 1

            try:
                
                spark = get_spark_session()

                # establish connection
                df = (
                    spark.read
                        .format("jdbc")
                        .option("url", destination["url"])
                        .option("dbtable", "(SELECT Temp = 1) AS Temp")
                        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                        .load()
                )

                connection = spark._jvm.java.sql.DriverManager.getConnection(destination["url"])
                
                statement = connection.createStatement()
                
                statement.executeUpdate(f"""
                    UPDATE
                        [dbo].[load_history]
                    SET
                        [status] = 'Successful'
                    WHERE
                        [table_name] = '{table["name"]}' AND
                        [is_preloaded] = 1
                """)

                retries = retriesMax

                isSuccessful = True

            except Exception as ex:
                stop_spark_session()

                if retries >= retriesMax:
                    raise ex

        if isSuccessful == False:
            raise Exception("Unable to update load_history")

    def preload_testdb_to_wh(table):
        sourceDbTable = get_source_db_table(table)
        destDbTable = f"[dbo].{table["name"]}"

        print('sourceDbTable: ', sourceDbTable)
        print('destDbTable: ', destDbTable)
        
        ending_offset = 0
        retries = 0
        isSuccessful = False

        while retries < retriesMax:
            retries = retries + 1

            try:
                
                spark = get_spark_session()
                
                # read 
                df = (
                    spark.read
                        .format("jdbc")
                        .option("url", source["url"])
                        .option("dbtable", sourceDbTable)
                        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                        .load()
                )

                ending_offset = df.count()
                print("ending_offset: ", ending_offset)

                # write
                (
                    df.write
                        .format("jdbc")
                        .mode("append")
                        .option("url", destination["url"])
                        .option("dbtable", destDbTable)
                        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                        .save()
                )

                retries = retriesMax

                isSuccessful = True

            except Exception as ex:
                stop_spark_session()

                if retries >= retriesMax:
                    raise ex

        if isSuccessful == False:
            raise Exception("Unable to load table to warehouse")
        else:
            # save success load history
            update_preloaded_status(table, ending_offset)

    def is_table_initilized_preloaded(table):
        loadHistoryPreloaded = """
            (
                SELECT 
                    status
                FROM 
                    load_history 
                WHERE 
                    table_name LIKE '@table' AND
                    is_preloaded = 1
            ) AS loadHistoryPreloaded
        """.replace(
            "@table", 
            table["name"]
        )

        spark = get_spark_session()

        df = (
            spark.read
                .format("jdbc")
                .option("url", destination["url"])
                .option("dbtable", loadHistoryPreloaded)
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .load()
        )

        is_preloaded = False
        is_initialized = False
        
        if df.count() > 0:
            is_initialized = True
            if str(df.collect()[0]["status"]) == "Successful":
                is_preloaded = True

        print("is_initialized: ", is_initialized)
        print("is_preloaded: ", is_preloaded)

        return is_initialized, is_preloaded

    for table in preloadTables:
        print("preload table: ", table)

        initialized, preloaded = is_table_initilized_preloaded(table)
        
        if initialized == False:
            print(f"Table {table["name"]} is not yet initialized.")
            raise("Table is not yet initialized!")
        else:
            if preloaded == False:
                preload_testdb_to_wh(table)
            else:
                print("Table already loaded!")
    
    stop_spark_session()
