### Read in the data from https://www.kaggle.com/new-york-city/nyc-parking-tickets
### These spark jobs run on a local (Mac Pro 16 gb RAM, 1 TB Flash Storage) spark installation using spark-3.0.3-bin-hadoop2.7
### spark session is available as spark and spark context is available as sc


#### add the config file within the spark context
`sc.addFile('/Users/sumangangopadhyay/complex-spark-transformations/config.py')`

#### import the relevant libraries
`import config as cf` 

`from pyspark.sql import functions as func`

#### get the relevant configurations in variables
`data_path = cf.data_path()`

`primary_response_variables = cf.primary_response_variables().split(',')`

`secondary_response_variables = cf.secondary_response_variables().split(',')`

`primary_explanatory_variables = cf.primary_explanatory_variables().split(',')`

#### read the data
`df = spark.read.csv(data_path, header=True)`

#### getting to know the data, number of rows and columns and see a few records to understand the structure of the dataframe
`df.count()`
`len(df.columns)`
`df.show(3)`

#### Remove the spaces from the column names so that it's easier to use the columns later on
`df_with_no_spaces_in_colm_names = df.select([func.col(col).alias(col.replace(' ', '_')) for col in df.columns])`

#### get the count of distinct values of the attributes which form the response variables ( In statistical terms, response variables are the variables on the y-axis, i.e. the variables whose variations are being observed)

`unique_count_of_primary_response_variables = df_with_no_spaces_in_colm_names.select([func.countDistinct(col).alias('unique_'+ col) for col in primary_response_variables])`

`unique_count_of_primary_response_variables.show()`

`unique_count_of_secondary_response_variables = df_with_no_spaces_in_colm_names.select([func.countDistinct(col).alias('unique_'+ col) for col in secondary_response_variables])`

`unique_count_of_secondary_response_variables.show()`

#### Check the NaNs and Nulls in the explanatory variables. These variables typically go in the x-axis. Statistically, we are interested in the extent to which the variation in the response variables are associated with the variation in these variables

`nan_null_count_in_primary_explanatory_variables = df_with_no_spaces_in_colm_names.select([func.count(func.when(func.isnan(col) | func.col(col).isNull(), col)).alias('null_nan_count_'+ col) for col in primary_explanatory_variables])`

`nan_null_count_in_primary_explanatory_variables.show()`

#### Creating a new categorical explanatory variable (Categorical variables are factors with 2 or more levels, e.g. a rainbow is a factor with 7 levels)

`df_with_no_spaces_in_colm_names = df_with_no_spaces_in_colm_names.withColumn('Violation_AM_or_PM', func.when(func.isnan(df_with_no_spaces_in_colm_names.Violation_Time) | func.col('Violation_Time').isNull(), func.lit(None)).otherwise(func.substring(df_with_no_spaces_in_colm_names.Violation_Time,5,1)))`

#### Verify that the new column has been populated correctly

`df_with_no_spaces_in_colm_names.select('Violation_Time','Violation_AM_or_PM').filter(df_with_no_spaces_in_colm_names.Violation_Time.isNotNull()).show(10)`

`df_with_no_spaces_in_colm_names.select('Violation_Time','Violation_AM_or_PM').filter(df_with_no_spaces_in_colm_names.Violation_Time.isNull()).show(10)`

`df_with_no_spaces_in_colm_names.select('Violation_Time','Violation_AM_or_PM').filter(func.isnan(df_with_no_spaces_in_colm_names.Violation_Time)).show(10)`

