import os, re
import configparser
from datetime import timedelta, datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T


# AWS Key and Password are configured in ppl.cfg:
config = configparser.ConfigParser()
config.read('ppl.cfg')

# Read AWS access info, save in a environment variable:
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def spark_session_create(aws_spark_emr):
    """
    Funciton for creating spark session, and return the spark session object.
    
    Parameters
    ==========
    aws_spark_emr: string, this is the endpoint of EMR.
    """
    spark = SparkSession.builder.config(aws_spark_emr).getOrCreate()
    return spark


def spark_read_data(spark, columns, input_file_path, input_file_format="csv", limit=None, **params):
    """
    Function for reading data using pyspark, and return the result as a spark dataframe.
    
    Parameters
    ==========
    spark: SparkSession object, created by spark_session_create method.
    input_file_path: string, path of the input file.
    input_file_format: string, format of the input file. Default "csv".
    columns: list, list of columns.
    limit: integer, whether to have a limit number of records to return.
    params: all other available parameters.
    """
    if limit is None:
        data = spark.read.load(input_file_path, format=input_file_format, **params).select(columns)
    else:
        data = spark.read.load(input_file_path, format=input_file_format, **params).select(columns).limit(limit)
    return data


def save_parquet(data, columns, output_file_path, output_file_format="parquet", mode="overwrite", partitionBy=None, **params):
    """
    Function for saving files in parquet format.
    
    Parameters
    ==========
    data: dataframe, dataframe that is wanted to be processed.
    output_file_path: string, the path for output file to be saved in Hadoop file system.
    output_file_format: string, format for output file. Default "parquet".
    columns: list, list of columns to be processed.
    mode: string, specify saving mode if data already exists. Default "overwrite".
    partitionBy: list, list of columns to be partitioned by.
    params: all other available parameters.
    """
    data.select(columns).write.save(output_file_path, mode=mode, format=output_file_format, partitionBy=partitionBy, **params)

    
def etl_immigration_data(spark, input_path="immigration_data_sample.csv", output_path="../immigration.parquet",
                         date_output_path="../date.parquet",
                         input_format = "csv",
                         columns=['cicid','i94yr','i94mon','i94res','i94port','arrdate','depdate',\
                                  'airline','i94addr','i94visa','i94mode','visatype',\
                                  'gender','i94bir','i94cit'], 
                         limit=None, partitionBy=["i94yr", "i94mon"], columns_to_save='*', header=True, **params):
    """
    Function for reading in immigration dataset, performing ETL process, and saveing it to the output path
    
    Parameters
    ==========
    spark: Spark session. 
    input_path: string, path of the input file.
    output_path: string, path to save the file.
    date_output_path: string, path to save the date file.
    input_format: string, format of the input file. Default to "csv".
    columns: list, list of the columns.
    limit: integer, number of rows to read in.
    partitionBy: list, list of columns to be partitioned by, and files will be saved based on all those partitioning columns.
    columns_to_save: string, columns to be saved in the end. Default ""*"" - meaning all columns are saved.
    header: boolean, whether to show the column names. Default True as to show column names.
    params: all other available parameters.
    """
    
    ### Loading immigration dataframe using spark:
    # Only a subset of columns are used to load the data here, and those columns are listed in the function parameter 'columns'.
    immigration_df = spark_read_data(spark, input_file_path=input_path, input_file_format=input_format, columns=columns, limit=limit, header=header, **params)
    
    ### Transforming columns of immigration dataset:
    # 1st - Convert columns with double type to integer, via spark dataframe column type conversion - CAST:
    int_cols = ['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'arrdate', 'depdate', 'i94mode', 'i94bir', 'i94visa']
    for col_key, type_value in dict(zip(int_cols, [T.IntegerType()]*len(int_cols))).items():
        immigration_df = immigration_df.withColumn(col_key, immigration_df[col_key].cast(type_value))
        
    # 2nd - Convert SAS date to YYYY-MM-DD date format:
    date_cols = ['arrdate', 'depdate']
    sas_date_udf = F.udf(lambda x: x if x is None else (timedelta(days=x) + datetime(1960, 1, 1)).strftime("%Y-%m-%d"))
    for col in date_cols:
        immigration_df = immigration_df[col].withColumn(col, sas_date_udf(immigration_df[col]))
    
    ### Creating a new dataframe called DATE, and save it as parquet formatted file to the output_path:
    # Select all the unique dates from arrdate column and depdate column:
    arrdate = immigration_df.select('arrdate').distinct()
    depdate = immigration_df.select('depdate').distinct()
    
    # Merge unique arrival dates and departure dates into a dataframe called dates:
    dates = arrdate.union(depdate)
    
    # Make changes to dates dataframe by adding new columns and dropping useless columns in the end:
    dates = dates.withColumn("date", F.to_date(dates.arrdate, "%Y-%m-%d"))
    dates = dates.withColumn("year", F.year(dates.date))
    dates = dates.withColumn("month", F.month(dates.date))
    dates = dates.withColumn("day", F.dayofmonth(dates.date))
    dates = dates.withColumn("weekofyear", F.weekofyear(dates.date))
    dates = dates.withColumn("dayofweek", F.dayofweek(dates.date))
    dates = dates.drop("date").withColumnRenamed('arrdate', 'date')
    
    # Save dates dataframe to parquet file:
    save_parquet(data=dates.select("date", "year", "month", "day", "weekofyear", "dayofweek"), output_file_path=date_output_path)
    
    ### Save the processed immigration dataset to the output_path
    save_parquet(data=immigration_df.select("*"), output_file_path=output_path, partitionBy=partitionBy)

    return immigration_df

def etl_temperature_data(spark, input_path="../GlobalLandTemperaturesByCity.csv", output_path="../temperature.parquet", 
                         input_format="csv", columns="*", limit=None, partitionBy=["Country", "City"], header=True, **params):
    """
    Function for reading in temperature dataset, performing ETL process, and saveing it to the output path
    
    Parameters
    ==========
    spark: Spark session. 
    input_path: string, path of the input file.
    output_path: string, path to save the file.
    input_format: string, format of the input file. Default to "csv".
    columns: list, list of the columns.
    limit: integer, number of rows to read in. Default "*" - meaning load all records.
    partitionBy: list, list of columns to be partitioned by, and files will be saved based on all those partitioning columns.
    header: boolean, whether to show the column names. Default True as to show column names.
    params: all other available parameters.
    """
    # Load temperature dataframe using spark:
    temperature_df = spark_read_data(spark, input_file_path=input_path, input_file_format=input_format, 
                                     columns=columns, limit=limit, header=header, **options)
    # Save temperature dataset as spark parquet file:
    save_parquet(data=temperature_df, output_file_path=output_path, partitionBy=partitionBy)
    
    return temperature_df


def etl_airport_data(spark, input_path="../airport-codes_csv.csv", output_path="../airport.parquet", 
                     input_format="csv", columns="*", limit=None, partitionBy=["iso_country"], header=True, **params):
    """
    Function for reading in airport dataset, performing ETL process, and saveing it to the output path
    
    Parameters
    ==========
    spark: Spark session. 
    input_path: string, path of the input file.
    output_path: string, path to save the file.
    input_format: string, format of the input file. Default to "csv".
    columns: list, list of the columns. Default "*" - meaning load all records.
    limit: integer, number of rows to read in.
    partitionBy: list, list of columns to be partitioned by, and files will be saved based on all those partitioning columns.
    header: boolean, whether to show the column names. Default True as to show column names.
    params: all other available parameters.
    """
    # Load airport dataframe using spark:
    airport_df = spark_read_data(spark, input_file_path=input_path, input_file_format=input_format,
                                 columns=columns, limit=limit, header=header, **params)    
    # Save airport dataset as spark parquet file:
    save_parquet(data=airport_df, output_file_path=output_path, partitionBy=partitionBy)

    return airport_df


def etl_demographics_data(spark, input_path="../us-cities-demographics.csv", output_path="../demographics.parquet", 
                          input_format="csv", columns="*", limit=None, partitionBy=["State Code"], header=True, sep=";", **params):
    """
    Function for reading in demographics dataset, performing ETL process, and saveing it to the output path
    
    Parameters
    ==========
    spark: Spark session. 
    input_path: string, path of the input file.
    output_path: string, path to save the file.
    input_format: string, format of the input file. Default to "csv".
    columns: list, list of the columns. Default "*" - meaning load all records.
    limit: integer, number of rows to read in.
    partitionBy: list, list of columns to be partitioned by, and files will be saved based on all those partitioning columns.
    header: boolean, whether to show the column names. Default True as to show column names.
    params: all other available parameters.
    """
    # Load demographics dataframe using spark:
    demographics = spark_read_data(spark, input_file_path=input_path, input_file_format=input_format, 
                                   columns=columns, limit=limit, header=header, sep=sep, **params)
    
    ### Transforming columns of demographics dataset:
    # 1st - Convert numeric columns to Integer types:
    int_cols = ['Count', 'Male Population', 'Female Population', 'Total Population', 'Number of Veterans', 'Foreign-born']
    for col_key, type_value in dict(zip(int_cols, [T.IntegerType()]*len(int_cols))).items():
        demographics_df = demographics_df.withColumn(col_key, demographics_df[col_key].cast(type_value))

    # 2nd - Convert numeric columns to Double types:
    float_cols = ['Median Age', 'Average Household Size']
    for col_key, type_value in dict(zip(float_cols, [T.IntegerType()]*len(float_cols))).items():
        demographics_df = demographics_df.withColumn(col_key, demographics_df[col_key].cast(type_value))
    
    ### Groupby operation, and return the first record in each category: 
    agg_df = demographics_df.groupby(["City", "State", "State Code"]).agg({
        "Median Age": "first",
        "Male Population": "first",
        "Female Population": "first", 
        "Total Population": "first", 
        "Number of Veterans": "first", 
        "Foreign-born": "first",
        "Average Household Size": "first"
    })
    
    # Pivot Table to transform values of the column Race to different columns
    piv_df = demographics_df.groupBy(["City", "State", "State Code"]).pivot("Race").sum("Count")
    
    # Rename column names removing the spaces to avoid problems when saving to disk (we got errors when trying to save column names with spaces)
    demographics_df = agg_df.join(other=piv_df, on=["City", "State", "State Code"], how="inner") \
    .withColumnRenamed('first(Total Population)', 'TotalPopulation') \
    .withColumnRenamed('first(Female Population)', 'FemalePopulation') \
    .withColumnRenamed('first(Male Population)', 'MalePopulation') \
    .withColumnRenamed('first(Median Age)', 'MedianAge') \
    .withColumnRenamed('first(Number of Veterans)', 'NumberVeterans') \
    .withColumnRenamed('first(Foreign-born)', 'ForeignBorn') \
    .withColumnRenamed('first(Average Household Size)', 'AverageHouseholdSize') \
    .withColumnRenamed('Hispanic or Latino', 'HispanicOrLatino') \
    .withColumnRenamed('Black or African-American', 'BlackOrAfricanAmerican') \
    .withColumnRenamed('American Indian and Alaska Native', 'AmericanIndianAndAlaskaNative')

    ### Fill NaNs with 0:
    numeric_cols = ['TotalPopulation', 'FemalePopulation', 'MedianAge', 'NumberVeterans', 'ForeignBorn', 'MalePopulation', 'AverageHouseholdSize',
                    'AmericanIndianAndAlaskaNative', 'Asian', 'BlackOrAfricanAmerican', 'HispanicOrLatino', 'White']
    demographics_df = demographics_df.fillna(0, numeric_cols)
    
    ### Save demographics dataset as spark parquet file:
    save_parquet(data=demographics_df, output_file_path=output_path, partitionBy=partitionBy)
    
    return demographics_df


def etl_state_data(spark, output_path="../state.parquet"):
    """
    Function for creating a new dataframe called state, and save it to the output path as parquet spark file
    
    Parameters
    ==========
    spark: Spark session. 
    output_path: string, path to save the output file.
    """
    # Load demographics dataframe using spark:
    demographics = etl_demographics_data(spark, output_path=None)

    # Aggregate by state:
    columns = ['TotalPopulation', 'FemalePopulation', 'MalePopulation', 'NumberVeterans', 'ForeignBorn', 
               'AmericanIndianAndAlaskaNative', 'Asian', 'BlackOrAfricanAmerican', 'HispanicOrLatino', 'White']
    state = demographics.groupby(["State Code", "State"]).agg(dict(zip(columns, ["sum"]*len(columns))))
    
    # Get state information about state names and state codes:
    state_info = demographics[["State", "State Code"]].dropDuplicates()
    state_info = state_info.withColumnRenamed('State', 'State Original')
    
    # Join the two datasets:
    capitalize_udf = F.udf(lambda x: x if x is None else x.title())
    state_info = state_info.join(state, state["State Code"] == addr.Code, "left")
    state_info = state_info.withColumn("State", when(isnull(state_info["State"]), capitalize_udf(state_info['State Original'])).otherwise(state_info["State"]))
    state_info = state_info.drop('State Original', 'State Code')
    
    cols = ['sum(BlackOrAfricanAmerican)', 'sum(White)', 'sum(AmericanIndianAndAlaskaNative)',
            'sum(HispanicOrLatino)', 'sum(Asian)', 'sum(NumberVeterans)', 'sum(ForeignBorn)', 'sum(FemalePopulation)', 
            'sum(MalePopulation)', 'sum(TotalPopulation)']
    
    # Rename the columns to get rid of names like sum(MalePopulation) returned by spark aggregation operation:
    # e.g., column 'sum(MalePopulation)' -> 'MalePopulation'
    mapping = dict(zip(cols, [re.search(r'\((.*?)\)', c).group(1) for c in cols]))
    state_info = state_info.select([col(c).alias(mapping.get(c, c)) for c in state_info.columns])
    
    # Save the resulting dataset to the output_path
    save_parquet(data=state_info, output_file_path=output_path)
    
    return state_info


if __name__ == "__main__" :
    # Start a spark session:
    spark = create_spark_session()
    
    # ETL process for immigration dataset, create immigration table & date table, save both as parquet spark files to S3:
    immigration = etl_immigration_data(spark, input_path='../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat',
                                       output_path="s3a://udacity-dend-capstone/immigration.parquet",
                                       date_output_path="s3a://udacity-dend-capstone/date.parquet",
                                       input_format = "com.github.saurfang.sas.spark", 
                                       limit=1000, partitionBy=None, 
                                       columns_to_save="*")
    
    # ETL process for state dataset, create state table, save it as parquet spark files to S3:
    state = etl_state_data(spark, output_path="s3a://udacity-dend-capstone/state.parquet")
