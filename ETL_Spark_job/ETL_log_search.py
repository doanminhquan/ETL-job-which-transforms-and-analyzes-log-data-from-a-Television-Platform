import findspark
findspark.init()
from pyspark.sql import SparkSession 
from pyspark.sql.functions import * 
from pyspark.sql.window import Window
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from connect_mysql import url, driver, user, password, dbtable


# retrieve log search
def retrieve_data_search(path, spark):
    schema = StructType([
        StructField("datetime", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("keyword", StringType(), True)])
    data = spark.createDataFrame([],schema = schema)
    df = spark.read.parquet(path)
    df = df.select('datetime','user_id','keyword')
    data = data.union(df)
    data = data.filter(data.user_id.isNotNull())
    data = data.filter(data.keyword.isNotNull())
    print('Read data {}'.format(path))
    return data

# process log search to find most search keyword
def process_log_search(data):
    data = data.select('user_id','keyword')
    data = data.filter(col('keyword').isNotNull() & col("user_id").isNotNull())
    data = data.groupBy('user_id','keyword').count()
    data = data.withColumnRenamed('count','TotalSearch')
    data = data.orderBy('user_id',ascending = False )
    window = Window.partitionBy('user_id').orderBy(col('TotalSearch').desc())
    data = data.withColumn('Rank',row_number().over(window))
    data = data.filter(col('Rank') == 1)
    data = data.withColumnRenamed('keyword','Most_Search')
    data = data.select('user_id','Most_Search')
    return data 

# map category
def map_most_search_category(df, spark):
    category_mapping =  spark.read.option("header",True).csv("D:\STUDY_DE\Big-data\Log_search_content_project\\map_search_category.csv")
    df = df.join(category_mapping, on = 'Most_Search', how = 'left')
    return df

# find treding type
def find_trend(df):
    df = df.withColumn('trending_type', when(df['category_t6'] == df['category_t7'], 'Unchanged').otherwise('Changed'))
    df = df.withColumn('previous', when(df['category_t6'] == df['category_t7'], 'Unchanged').otherwise(concat_ws('-', df['category_t6'], df['category_t7'])))
    return df

# import to mysql
def import_to_mysql(df):
    df.write.format('jdbc') \
    .options(url = url , driver = driver , dbtable = dbtable , user=user , password = password) \
    .save(mode = 'append')
    print('Data imported successfully')

# main
def main(path, spark):
    path_t6 = path + '\\data_t6\\*'
    path_t7 = path + '\\data_t7\\*'
    print('Retrieving data from log search t6')
    print('-------------------------------------')
    data_t6 = retrieve_data_search(path_t6,spark)
    print('Retrieving data from log search t7')
    print('-------------------------------------')
    data_t7 = retrieve_data_search(path_t7,spark)
    print('Processing data')
    print('-------------------------------------')
    most_search_t6 = process_log_search(data_t6)
    most_search_category_t6 = map_most_search_category(most_search_t6, spark)
    most_search_t7 = process_log_search(data_t7)
    most_search_category_t7 = map_most_search_category(most_search_t7, spark)
    most_search_category_t6 = most_search_category_t6.withColumnRenamed('Most_Search','most_search_t6').withColumnRenamed('Category', 'category_t6')
    most_search_category_t7 = most_search_category_t7.withColumnRenamed('Most_Search','most_search_t7').withColumnRenamed('Category', 'category_t7')
    result = most_search_category_t6.join(most_search_category_t7,'user_id','inner')
    result = result.filter(col('category_t6').isNotNull() & col('category_t7').isNotNull())
    print('Find trending category of each user')
    print('-------------------------------------')    
    result = find_trend(result)
    import_to_mysql(result)

if __name__ == 'main':
    path = 'D:\STUDY_DE\Big-data\Dataset\log_search'
    spark = SparkSession.builder \
            .config("spark.jars.packages","com.mysql:mysql-connector-j:8.3.0") \
            .getOrCreate()
    main(path,spark)