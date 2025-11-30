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
        print("Usage: spark-submit replicate-testdb-data.py <config file> <preload>")
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
    tables = config["tables"]
    sparkExecutorMemory = config["sparkExecutorMemory"]
    sparkDriverMemory = config["sparkDriverMemory"]
    sparkExecutorCores = config["sparkExecutorCores"]
    sparkCoresMax = config["sparkCoresMax"]
    offsetPreloadTimestamp = config["offsetPreloadTimestamp"]

    print("source: ", source)
    print("destination: ", destination)
    print("processNoOfRows: ", processNoOfRows)
    print("sparkMaster: ", sparkMaster)
    print("sparkJars: ", sparkJars)
    print("tables: ", tables)
    print("sparkExecutorMemory: ", sparkExecutorMemory)
    print("sparkDriverMemory: ", sparkDriverMemory)
    print("sparkExecutorCores: ", sparkExecutorCores)
    print("sparkCoresMax: ", sparkCoresMax)
    print("offsetPreloadTimestamp: ", offsetPreloadTimestamp)
    
    # Initialize Spark Session
    def get_spark_session():
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

        return spark
    
    def stop_spark_session():
        active_spark_session = SparkSession.getActiveSession()
        
        if active_spark_session:
            active_spark_session.stop()

    def set_spark_url(spark, url):
        # establish connection
        df = (
            spark.read
                .format("jdbc")
                .option("url", url)
                .option("dbtable", "(SELECT Temp = 1) AS Temp")
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .load()
        )

    def get_db_table(table, last_validated_date, next_validated_date, is_archive):
        tableName = table["name"]
        archiveTableName = table["archive"]
        dateKeyFrom = table["dateKeyFrom"]
        dateKeyTo = table["dateKeyTo"]
        archiveSelect = table["archiveSelect"]
        mainSelect = table["mainSelect"]
        idKey = table["idKey"]
        sourceDbTable = ""
        
        print("tableName: ", tableName)
        print("archiveTableName: ", archiveTableName)
        print("dateKeyFrom: ", dateKeyFrom)
        print("dateKeyTo: ", dateKeyTo)
        print("is_archive: ", is_archive)
        print("archiveSelect: ", archiveSelect)
        print("mainSelect: ", mainSelect)
        print("idKey: ", idKey)
        
        if is_archive == True:
            sourceDbTable = f"""
                (
                    SELECT
                        {",".join(mainSelect)}
                    FROM 
                        [dbo].[{archiveTableName}] A LEFT JOIN
                        [dbo].[{tableName}] A2 ON
                            A2.[{idKey}] = A.[{idKey}]
                    WHERE
                        CASE
                            WHEN A2.[{idKey}] IS NULL THEN CAST(A.valid_to AS DATETIME2(0))
                            ELSE CAST(A.valid_to AS DATETIME2(7))
                        END > CAST('{last_validated_date}' AS DATETIME2(7)) AND
                        (
                            CASE
                                WHEN A2.[{idKey}] IS NULL THEN CAST(A.valid_to AS DATETIME2(0))
                                ELSE CAST(A.valid_to AS DATETIME2(7))
                            END <=
                            CASE
                                WHEN A2.[{idKey}] IS NULL THEN CAST('{next_validated_date}' AS DATETIME2(0))
                                ELSE CAST('{next_validated_date}' AS DATETIME2(7))
                            END
                        )
                        
                ) AS {archiveTableName}
            """
        else:
            sourceDbTable = f"""
                (
                    SELECT
                        *
                    FROM 
                        [dbo].[{tableName}]
                    WHERE
                        {dateKeyFrom} > CAST('{last_validated_date}' AS DATETIME2(7)) AND
                        {dateKeyFrom} <= CAST('{next_validated_date}' AS DATETIME2(7))

                    UNION ALL

                    SELECT
                        {",".join(archiveSelect)}
                    FROM
                        (
                            SELECT TOP 1000000
                                [{idKey}] = A.[{idKey}],
                                [valid_from] = MAX(A.[valid_from]),
                                [valid_to] = MAX(A.[valid_to]),
                                [deleted] = 
                                    CASE
                                        WHEN MAX(A2.[{idKey}]) IS NULL THEN CAST(1 AS BIT)
                                        ELSE CAST(0 AS BIT)
                                    END
                            FROM 
                                [dbo].[{archiveTableName}] A LEFT JOIN
                                [dbo].[{tableName}] A2 ON
                                    A2.{idKey} = A.{idKey}
                            WHERE
                                A.[valid_from] > CAST('{last_validated_date}' AS DATETIME2(7)) AND
                                CASE
                                    WHEN A2.[{idKey}] IS NULL THEN CAST('{next_validated_date}' AS DATETIME2(0))
                                    ELSE CAST('{next_validated_date}' AS DATETIME2(7))
                                END BETWEEN
                                        CASE
                                            WHEN A2.[{idKey}] IS NULL THEN CAST(A.valid_from AS DATETIME2(0))
                                            ELSE CAST(A.valid_from AS DATETIME2(7))
                                        END AND 
                                        CASE
                                            WHEN A2.[{idKey}] IS NULL THEN CAST(A.valid_to AS DATETIME2(0))
                                            ELSE CAST(A.valid_to AS DATETIME2(7))
                                        END AND
                                NOT EXISTS
                                (
                                    SELECT
                                        1
                                    FROM
                                        [dbo].[{tableName}] M
                                    WHERE
                                        M.{idKey} = A.{idKey} AND
                                        CASE
                                            WHEN A2.[{idKey}] IS NULL THEN CAST(M.valid_from AS DATETIME2(0))
                                            ELSE CAST(M.valid_from AS DATETIME2(7))
                                        END = CASE
                                            WHEN A2.[{idKey}] IS NULL THEN CAST(A.valid_to AS DATETIME2(0))
                                            ELSE CAST(A.valid_to AS DATETIME2(7))
                                        END AND
                                        CASE
                                            WHEN A2.[{idKey}] IS NULL THEN CAST(M.valid_from AS DATETIME2(0))
                                            ELSE CAST(M.valid_from AS DATETIME2(7))
                                        END <= CASE
                                            WHEN A2.[{idKey}] IS NULL THEN CAST('{next_validated_date}' AS DATETIME2(0))
                                            ELSE CAST('{next_validated_date}' AS DATETIME2(7))
                                        END
                                        
                                )
                            GROUP BY
                                A.{idKey}
                        ) A1 JOIN
                        [dbo].[{archiveTableName}] A ON
                            A.{idKey} = A1.{idKey} 
                    WHERE
                        A1.valid_from = A.valid_from AND
                        A1.valid_to = A.valid_to 
                ) AS {tableName}
            """

        print("sourceDbTable: ", sourceDbTable)

        return sourceDbTable
    
    def update_history_validated_status(last_validated_date, next_validated_date):
        retriesMax = 1
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
                        [is_validated] = 1
                    WHERE
                        [table_name] = 'all' AND
                        [is_preloaded] = 0 AND
                        [status] = 'Successful' AND
                        [validated_date] > '{last_validated_date}' AND
                        [validated_date] <= '{next_validated_date}'
                """)

                retries = retriesMax

                isSuccessful = True

            except Exception as ex:
                stop_spark_session()

                if retries >= retriesMax:
                    raise ex

        if isSuccessful == False:
            raise Exception("Unable to set update history validated status")
    
    def assert_data(table, last_validated_date, next_validated_date, is_archive):
        retriesMax = 1
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
                        .option("dbtable", get_db_table(table, last_validated_date, next_validated_date, is_archive))
                        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                        .load()
                )

                # get destination data
                df_destination = (
                    spark.read
                        .format("jdbc")
                        .option("url", destination["url"])
                        .option("dbtable", get_db_table(table, last_validated_date, next_validated_date, is_archive))
                        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                        .load()
                ) 

                if is_archive == True:
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

    def get_validated_dates():
        loadValidatedDate = f"""
            DECLARE @LastValidatedDate DATETIME2(7) = 
                (
                    SELECT
                        MAX([validated_date])
                    FROM
                        [dbo].[load_history]
                    WHERE
                        [status] = 'Successful' AND
                        [is_validated] = 1
                )
            DECLARE @NextValidatedDate DATETIME2(7) = 
                (
                    SELECT 
                        [next_validated_date] = MAX([validated_date])
                    FROM
                        [dbo].[load_history]
                    WHERE
                        [status] = 'Successful' AND
                        [validated_date] > @LastValidatedDate AND
                        [is_validated] = 0
                )

            SELECT 
                [LastValidatedDate] = @LastValidatedDate, 
                [NextValidatedDate] = @NextValidatedDate
        """

        print("loadValidatedDate: ", loadValidatedDate)

        spark = get_spark_session()

        set_spark_url(spark, destination["url"])

        connection = spark._jvm.java.sql.DriverManager.getConnection(destination["url"])
        
        statement = connection.createStatement()

        result = statement.executeQuery(loadValidatedDate)
        
        result.next()

        last_validated_date = result.getString("LastValidatedDate")
        next_validated_date = result.getString("NextValidatedDate")

        print("last_validated_date: ", last_validated_date)
        print("next_validated_date: ", next_validated_date)
        
        return last_validated_date, next_validated_date

    last_validated_date, next_validated_date = get_validated_dates()

    if (next_validated_date is None) == False:
        for table in tables:
            assert_data(table, last_validated_date, next_validated_date, False)
            assert_data(table, last_validated_date, next_validated_date, True)   
        
        update_history_validated_status(last_validated_date, next_validated_date)