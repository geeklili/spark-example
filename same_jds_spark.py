from pyspark.sql import SparkSession, types
from base import build_sentence_vector, cosin_sim
from gensim.models import Word2Vec
from bson import ObjectId
from pymongo import MongoClient
w2v_model = Word2Vec.load('./data/w2v_model.pkl')
jd_mongo_url = 'mongodb://root:******@********:3717/'

jd_id = ObjectId("5f6b053846e7b9449879c77b")
jd_mongo_client = MongoClient(jd_mongo_url)
jd = jd_mongo_client['cvexplorer']['jd'].find_one({'_id': jd_id})
job_description = jd['skill'] + jd['jobDuty']
job_des = ' '.join(job_description)
jd_title = jd['title']
des_vector = build_sentence_vector(job_des, w2v_model)
title_vector = build_sentence_vector(jd_title, w2v_model)
jd_vector = (0.8 * des_vector + 0.2 * title_vector).tolist()[0]


def distance(jd):
    # 发布的jd来自工作流系统， 搜索的jd来自各个平台的抓取
    dis = float((2 * (1 - cosin_sim(jd_vector, jd['w2v']))) ** 0.5)
    result = {"distance": dis, "hash_value": jd['hash_value'], 'com_std_name': jd['com_std_name'],
              "company_name": jd['company_name'], "job_name": jd['job_name'], "target_jd_id": str(jd_id),
              'job_city': jd['job_city']}
    return result


if __name__ == '__main__':
    import os
    """
    提交命令
    spark-submit same_jds_spark.py
    """
    # os.environ['JAVA_HOME'] = 'C:\Program Files\Java\jdk1.8.0_151'
    # 与mongo连接读取数据时，注意数据类型对照
    # https://docs.mongodb.com/spark-connector/master/scala/datasets-and-sql/
    input_schema = types.StructType([types.StructField('hash_value', types.StringType(), True),
                                     types.StructField('job_name', types.StringType(), True),
                                     types.StructField('job_city', types.StringType(), True),
                                     types.StructField('com_std_name', types.StringType(), True),
                                     types.StructField('company_name', types.StringType(), True),
                                     types.StructField('w2v', types.ArrayType(types.DoubleType()), True)])
    result_schema = types.StructType([types.StructField('distance', types.DoubleType(), True),
                                      types.StructField('hash_value', types.StringType(), True),
                                      types.StructField('job_city', types.StringType(), True),
                                      types.StructField('target_jd_id', types.StringType(), True),
                                      types.StructField('job_name', types.StringType(), True),
                                      types.StructField('com_std_name', types.StringType(), True),
                                      types.StructField('job_city', types.StringType(), True),
                                      types.StructField('company_name', types.StringType(), True)])
    read_col = 'mongodb://root:******@*********:3717/?authSource=admin'
    # save_col = "mongodb://192.168.1.43:27017/spark_data.jd_similar"
    save_col = "mongodb://****:****@192.168.1.5:27017/spark_data.jd_similar?authSource=admin"
    # save_col = "mongodb://root:******@********:3717/spark_data.jd_similar?authSource=admin"
    """PREPROCESS"""
    spark = SparkSession \
        .builder \
        .appName("myApp") \
        .config("spark.mongodb.input.uri", read_col) \
        .config("spark.mongodb.input.database", "jd_combine") \
        .config("spark.mongodb.input.collection", "jd_select_pool") \
        .config("spark.mongodb.output.uri", save_col) \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1") \
        .getOrCreate()
    df = spark.read.schema(input_schema).format("com.mongodb.spark.sql.DefaultSource").option("spark.mongodb.input.partitionerOptions.partitionSizeMB", "32").load()
    df.printSchema()
    data = df.rdd.map(lambda row: row.asDict()).map(lambda row: distance(row)).filter(lambda row: row['distance'] < 0.6)
    # data = df.rdd.map(lambda row: row.asDict()).map(lambda row: distance(row)).filter(lambda row: row['distance'] < 0.5)
    schem_data = spark.createDataFrame(data, result_schema)
    schem_data.printSchema()
    schem_data.write.format("mongo").option("replaceDocument", "false").mode("append").save()