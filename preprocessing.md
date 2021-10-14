### Read in the data from https://www.kaggle.com/new-york-city/nyc-parking-tickets
### These spark jobs are run on a local (Mac Pro 16gb RAM, 1TB Flash Storage) spark installation using spark-3.0.3-bin-hadoop2.7
### spark session is available as spark and spark context is available as sc


#### add the config file within the spark context
`sc.addFile('/Users/sumangangopadhyay/complex-spark-transformations/config.py')`

#### import the relevant libraries
`import config as cf` 

`from pyspark.sql import functions as func`

#### get the relevant configurations in variables
`data_path = cf.data_path()`

#### read the data
`df = spark.read.csv(data_path, header=True)`

#### getting to know the data, number of rows and columns and see a few records to understand the structure of the dataframe
`df.count()`
`len(df.columns)`
`df.show(3)`

#### Remove the spaces from the column names so that it's easier to use the columns later on
`df_with_no_spaces_in_colm_names = df.select([func.col(col).alias(col.replace(' ', '_')) for col in df.columns])`

#### get the count of distinct values of the attributes which form the explanatory variables ( In statistical terms, explanatory variables are the variables on the x-axis)
`explanatory_variables = ['Registration_State', 'Plate_Type', 'Violation_Code', 'Vehicle_Make', 'Vehicle_Body_Type', 'Law_Section', 'Violation_Legal_Code']`

`unique_count_of_explanatory_variables = df_with_no_spaces_in_colm_names.select([func.countDistinct(col).alias('unique_'+ col) for col in explanatory_variables])`

`unique_count_of_explanatory_variables.show()`

