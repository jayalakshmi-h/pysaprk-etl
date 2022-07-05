"""
etl_job.py
~~~~~~~~~~
This Python module contains Apache Spark ETL job definition. It can be
submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions. For
example, this example script can be executed as follows,
    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --py-files packages.zip \
    --files configs/etl_config.json \
    jobs/etl_job.py
where packages.zip contains Python modules required by ETL job (in
this example it contains a class to provide access to Spark's logger),
which need to be made available to each executor process on every node
in the cluster; etl_config.json is a text file sent to the cluster,
containing a JSON object with all of the configuration parameters
required by the ETL job; and, etl_job.py contains the Spark application
to be executed by a driver process on the Spark master node.
The approach for structuring jobs is to separate the individual
'units' of ETL - the Extract, Transform and Load parts - into dedicated
functions, such that the key Transform steps can be covered by tests
and jobs or called from within another environment (e.g. a Jupyter or
Zeppelin notebook).
"""
import sys

import pyspark.sql.functions as F

from dependencies.spark import start_spark


def main():
    """Main ETL script definition.
    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='etl_job',
        files=['etl_config.json'])


    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # execute ETL pipeline
    data = extract_data(spark, log)
    data_validate = dq_check(data, log)
    data_transformed = transform_data(data_validate, config, log)
    load_data(data_transformed, log)

    # log the success and terminate Spark application
    log.warn('etl_job is finished')
    spark.stop()
    return None


def extract_data(spark, log):
    """Load data from Json file format.
     Do a data quality and pass only the valid records
    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    try:
        df = (
            spark
                .read
                .json('input')
                .select("ingredients", "cookTime", "prepTime"))
        return df
    except Exception as e:
        log.error("Error while reading the input file" + str(e))
        sys.exit(400)


def transform_data(df, config, log):
    """Transform original dataset.
    :param df: Input DataFrame.
    :param config: Configuration for ingredient and difficulty level classification
    :return: Transformed DataFrame.
    """
    try:
        df_transformed = (
            df
            .where(F.lower("ingredients").like("%" + config['ingredient'] + "%"))
            .withColumn("total_cook_time",
                        (convert_iso_to_mins(F.col('cookTime')) + convert_iso_to_mins(F.col('prepTime'))))
            .withColumn("difficulty", F.expr(get_condn(config)))
            .groupby("difficulty")
            .agg(F.avg(F.col('total_cook_time')).alias("avg_total_cooking_time"))
            .withColumn("avg_total_cooking_time", convert_mins_to_iso(F.col('avg_total_cooking_time')))
            .drop("ingredients", "cookTime", "prepTime", "total_cook_time")
        )
        return df_transformed
    except Exception as e:
        log.error("Error while transforming the data" + str(e))
        sys.exit(400)


def dq_check(df, log):
    """
    Validates whether cookTime and prepTime are in valid ISO8601 duration format
    :param df: Input Dataframe
    :return: Filtered Dataframe
    """
    try:
        df_filtered = mandatory_cols_check(df)
        df_validate = isoformat_valid(df_filtered)
        return df_validate
    except Exception as e:
        log.error("Error while performing data quality check" + str(e))
        sys.exit(400)


def mandatory_cols_check(df):
    """
    Filters the records which has cookTime and prepTime values
    :param df:
    :return: Filtered Dataframe
    """
    df_filtered = df.where(((F.col('cookTime').isNotNull() & (F.col('cookTime') != "")) | (F.col('prepTime').isNotNull() & (F.col('prepTime') != ""))))
    return df_filtered


def isoformat_valid(df_filtered):
    """
        Validates whether cookTime and prepTime are in valid ISO8601 duration format
        :param df: Input Dataframe
        :return: Filtered Dataframe
    """
    regex = "^P(T(?=\d)((\d+H)|(\d+\.\d+H$))?((\d+M)|(\d+\.\d+M$))?(\d+(\.\d+)?S)?)??$"
    return df_filtered.filter(((F.col('cookTime').rlike(regex))| (F.col('cookTime') == "")) & ((F.col('prepTime').rlike(regex)) | (F.col('prepTime') == "")))


def get_condn(config):
    """
    Creates the sql condition for difficulty classification
    :param config:
    :return: sql condition
    """
    condn = ""
    for diff in (config['difficulty']):
        if len(condn) == 0:
            condn = "CASE"
        if len(diff['upper_bound']) > 0:
            lb_condn = " WHEN total_cook_time " + (">= " if diff['endpoints_included'] else "> ")
            ub_condn = " AND total_cook_time " + ("<= " if diff['endpoints_included'] else "< ") + diff['upper_bound']
            condn = condn + lb_condn + diff['lower_bound'] + ub_condn + " THEN '" + diff['level'] + "'"
        else:
            condn = condn + " ELSE '" + diff['level'] + "' END"
    return condn


def convert_iso_to_mins(iso_time):
    """
    Function to convert a column which has ISO 8601 duration to minutes
    :param iso_time:
    :return: time in minutes
    """
    return (F.coalesce(F.regexp_extract(iso_time, r'(\d+)H', 1).cast('int'), F.lit(0)) * 60 +
            F.coalesce(F.regexp_extract(iso_time, r'(\d+)M', 1).cast('int'), F.lit(0)) +
            F.coalesce(F.regexp_extract((iso_time), r'(\d+)S', 1).cast('int'), F.lit(0)) / 60)


def convert_mins_to_iso(mins):
    """
        Function to convert minutes to ISO 8601 duration
    :param mins:
    :return: time in ISO 8601 duration
    """
    days = F.floor(mins / 1440)
    mins = mins - days * 1440
    hours = F.floor(mins / 60)
    mins = mins - hours * 60
    seconds = (mins - (F.floor(mins))) * 60
    dur = "PT"
    return F.concat(F.lit(dur),
                    F.coalesce((F.when(hours > 0, F.concat(hours, F.lit("H")))), F.lit("")),
                    F.coalesce((F.when(mins > 0, F.concat(F.floor(mins), F.lit("M")))), F.lit("")),
                    F.coalesce((F.when(seconds > 0, F.concat(seconds, F.lit("S")))), F.lit("")))


def load_data(df, log):
    """Collect data and write to CSV.
    :param df: DataFrame to print.
    :return: None
    """
    try:
        (df
        .write
        .csv('output', mode='overwrite', header=True))
        return None
    except Exception as e:
        log.error("Error while loading the data" + str(e))
        sys.exit(400)



# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
