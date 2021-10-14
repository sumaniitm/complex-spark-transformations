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

#### Check the NaNs and Nulls in the response variables

`nan_null_count_in_primary_explanatory_variables = df_with_no_spaces_in_colm_names.select([func.count(func.when(func.isnan(col) | func.col(col).isNull(), col)).alias('null_nan_count_'+ col) for col in primary_explanatory_variables])`

`nan_null_count_in_primary_explanatory_variables.show()`