from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import os
spark = SparkSession.builder.appName("log_search").getOrCreate()
def read_data():
    path = "D:\\Long Class Data\\DE\\Recent class\\Dataset\\log_search\\"
    number = [6,7]
    month_dict = {}
    for i in number:
        largest_original_file = None
        for j in range(1,15):
            if j < 10:
                filename = "20220" + str(i)
                largest_file = 0
                filename = filename + "0" + str(j)
                for data_file in os.listdir(path + filename):
                    file_size = os.path.getsize(path+filename+'\\'+data_file)
                    if int(file_size) > largest_file:
                        largest_filename = path + filename + '\\' + data_file
                        largest_file = file_size
                    else:
                        continue
            else:
                largest_file = 0
                filename = "20220" + str(i)
                filename = filename + str(j)
                for data_file in os.listdir(path + filename):
                    file_size = os.path.getsize(path+filename+'\\'+data_file)
                    if int(file_size) > largest_file:
                        largest_filename = path + filename + '\\' + data_file
                        largest_file = file_size
            if largest_original_file == None:
                largest_file = largest_filename.replace("'\'","\\")
                read_newfile = spark.read.parquet(largest_file)
                largest_original_file = read_newfile
            else:
                largest_file = largest_filename.replace("'\'","\\")
                read_newfile = spark.read.parquet(largest_file)
                largest_original_file = largest_original_file.union(read_newfile)
        month_dict[i] = largest_original_file
    return month_dict
def load_data():
    ds_month6 = read_data()[6]
    ds_month7 = read_data()[7]
    category_month6 = spark.read.csv("D:\\Long Class Data\\DE\\Recent class\\Final Project Product\\keyword_category_dsmonth6.csv",header=True)
    category_month7 = spark.read.csv("D:\\Long Class Data\\DE\\Recent class\\Final Project Product\\keyword_category_dsmonth7.csv",header=True)
    return ds_month6, ds_month7, category_month6, category_month7
def process_data(ds):
    ds= ds.select('datetime','user_id','keyword')
    ds= ds.dropna(subset=['user_id'])
    ds = ds.dropna(subset=['keyword'])
    ds =ds.drop('datetime')
    ds = ds.groupBy('user_id','keyword').agg(count('keyword').alias('count_keyword'))
    window_spec = Window.partitionBy('user_id').orderBy(desc('count_keyword'))
    ds = ds.withColumn('rank_count',dense_rank().over(window_spec))
    ds = ds.filter(col('rank_count')==1)
    ds = ds.groupBy('user_id').agg(first('keyword').alias('keyword'))
    return ds
def final_data(ds,category_ds):
    ds = process_data(ds)
    ds_final = ds.join(category_ds,on='keyword',how='left')
    ds_final = ds_final.dropna()
    return ds_final
def change_columname(ds,i):
    ds = ds.withColumnsRenamed({'keyword':'most_search' +str(i),'category':'category' + str(i)})
    return ds
def find_trending(ds1, ds2):
    ds_final = ds1.join(ds2,on='user_id',how='inner')
    ds_final = ds_final.withColumn('Trending_Type',when(col('category6') == col('category7'),lit('Unchanged')).otherwise(lit('Changed')))
    ds_final = ds_final.withColumn('Previous',when(col('Trending_Type')=='Unchanged',lit("Unchanged")).otherwise(concat_ws('-',col('category6'),col('category7'))))
    return ds_final
def main():
    print("-"*80)
    print("Start of the program")
    print("-"*80)
    ds_month6, ds_month7, category_month6, category_month7 = load_data()
    print("-"*80)
    print("Data Loaded")
    print("-"*80)
    ds_month6 = final_data(ds_month6,category_month6)
    ds_month7 = final_data(ds_month7,category_month7)
    ds_month6 = change_columname(ds_month6,6)
    ds_month7 = change_columname(ds_month7,7)
    print("-"*80)
    print("Data Processed")
    print("-"*80)
    ds_final = find_trending(ds_month6,ds_month7)
    print("-"*80)
    print("Data Finished")
    ds_final.show(truncate=False)
main()