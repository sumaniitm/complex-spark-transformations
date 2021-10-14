# Read in the data from https://www.kaggle.com/new-york-city/nyc-parking-tickets
# These spark jobs are run on a local (Mac Pro 16gb RAM, 1TB Flash Storage) spark installation using
# spark-3.0.3-bin-hadoop2.7
# spark session is available as spark and spark context is available as sc

sc.addFile('/Users/sumangangopadhyay/complex-spark-transformations/config.py')

import config as cf
data_path = cf.data_path()

df = spark.read.csv(data_path, header=True)


