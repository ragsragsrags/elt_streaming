from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.testing import assertDataFrameEqual

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
        maxDate = get_preload_max_date()
        
        print("tableName: ", tableName)
        print("archiveTableName: ", archiveTableName)
        print("archiveSelect: ", archiveSelect)
        print("dateKey: ", dateKey)
        print("orderByKey: ", orderByKey)
        print("isArchive: ", isArchive)
        print("maxDate: ", maxDate)
        
        if isArchive == False:
            sourceDbTable = f"""
                (
                    SELECT
                        *
                    FROM 
                        [dbo].[{tableName}]
                    WHERE
                        {dateKey} <= '{maxDate}' 

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
                                CAST('{maxDate}' AS DATETIME2(7)) BETWEEN [valid_from] AND [valid_to] AND
                                NOT EXISTS
                                (
                                    SELECT
                                        1
                                    FROM
                                        [dbo].[{tableName}] M
                                    WHERE
                                        M.{orderByKey} = A.{orderByKey} AND
                                        M.valid_from = A.valid_to AND
                                        M.valid_from <= CAST('{maxDate}' AS DATETIME2(7))
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
                        {dateKey} <= '{maxDate}'
                ) AS {tableName}
            """

        return sourceDbTable
    
    def update_history_validated_status(table):

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
                        [is_validated] = 1,
                        [validated_date] = 
                            (
                                SELECT 
                                    [MaxDate] = ISNULL(
                                        [dbo].[GetDateTime2FromUnixEpoch](
                                            CAST(MAX(JSON_VALUE(
                                                M.[after],
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
                            )
                    WHERE
                        [table_name] = '{table["name"]}' AND
                        [is_preloaded] = 1 AND
                        [status] = 'Successful'
                """)

                retries = retriesMax

                isSuccessful = True

            except Exception as ex:
                stop_spark_session()

                if retries >= retriesMax:
                    raise ex

        if isSuccessful == False:
            raise Exception("Unable to set update history validated status")

    def assert_data(table):
        retries = 0
        isSuccessful = False

        while retries < retriesMax:
            retries = retries + 1

            try:
                
                spark = get_spark_session()
                
                # get source data 
                df_source = (
                    spark.read
                        .format("jdbc")
                        .option("url", source["url"])
                        .option("dbtable", get_source_db_table(table))
                        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                        .load()
                )

                # get destination data
                df_destination = (
                    spark.read
                        .format("jdbc")
                        .option("url", destination["url"])
                        .option("dbtable", f"(SELECT * FROM [dbo].{table["name"]}) AS destination")
                        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                        .load()
                ) 

                if bool(table["hasDeletedColumn"]) == True:
                    df_destination = df_destination.drop("deleted")

                retries = retriesMax

                isSuccessful = True

                 # Rows in df1 but not in df2
                diff_source = df_source.exceptAll(df_destination) 
                # Rows in df2 but not in df1
                diff_destination = df_destination.exceptAll(df_source)

                if diff_source.isEmpty() and diff_destination.isEmpty():
                    print("DataFrames are identical in content.")
                else:
                    print(f"Differences found in table {table["name"]}:")
                    diff_source.show()
                    diff_destination.show()
                    raise(f"Differences found!")

            except Exception as ex:
                stop_spark_session()

                if retries >= retriesMax:
                    raise ex

        if isSuccessful == False:
            raise Exception("Unable to assert data")
        else:
            update_history_validated_status(table)

    def is_table_preloaded_validated(table):
        loadHistoryPreloaded = """
            (
                SELECT 
                    *
                FROM 
                    load_history 
                WHERE 
                    table_name LIKE '@table' AND
                    status = 'Successful' AND
                    is_preloaded = 1  
            ) AS loadHistoryPreloaded
        """.replace(
            "@table", 
            table["name"]
        )

        loadHistoryValidated = """
            (
                SELECT 
                    *
                FROM 
                    load_history 
                WHERE 
                    table_name LIKE '@table' AND
                    status = 'Successful' AND
                    is_preloaded = 1 AND
                    ISNULL(is_validated, 0) = 1  
            ) AS loadHistoryPreloaded
        """.replace(
            "@table", 
            table["name"]
        )

        spark = get_spark_session()

        # pre_loaded
        df = (
            spark.read
                .format("jdbc")
                .option("url", destination["url"])
                .option("dbtable", loadHistoryPreloaded)
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .load()
        )
        
        is_preloaded = False
        
        if df.count() > 0:
            is_preloaded = True

        print("is_preloaded: ", is_preloaded)

        # validated
        is_validated = False
        
        if is_preloaded == True:

            df_validated = (
                spark.read
                    .format("jdbc")
                    .option("url", destination["url"])
                    .option("dbtable", loadHistoryValidated)
                    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                    .load()
            )    

            if df_validated.count() > 0:
                is_validated = True            

        print("is_validated: ", is_validated)

        return is_preloaded, is_validated

    for table in preloadTables:
        print("preload table: ", table)

        preloaded, validated = is_table_preloaded_validated(table)
        
        if preloaded == True:
            if validated == False:
                assert_data(table)
            else:
                print("Table already validated!")
        else:
            print(f"Table {table["name"]} not loaded!")
            raise("Table not yet loaded!")
    
    stop_spark_session()
