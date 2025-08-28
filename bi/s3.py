import sys
import os
from io import BytesIO
import boto3
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql import SparkSession
from config.config import bucket_name, file_key, file_key_2,file_key_3,file_key_4,file_key_5,file_key_6,file_key_7,file_key_8,file_key_9,file_key_10,file_key_11,file_key_12,file_key_13,file_key_14,file_key_15,file_key_16
from pyspark.sql.functions import count, col


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder \
    .appName("S3 Excel to Spark") \
    .getOrCreate()

s3 = boto3.client('s3')
objects = s3.list_objects_v2(Bucket=bucket_name)
if 'Contents' in objects:
    print("Files in bucket:")
    for obj in objects['Contents']:
        print(obj['Key'])
else:
    print("Bucket is empty or not found.")

obj = s3.get_object(Bucket=bucket_name, Key=file_key)
obj2 = s3.get_object(Bucket=bucket_name, Key=file_key_2)
obj3 = s3.get_object(Bucket=bucket_name, Key=file_key_3)
obj4 = s3.get_object(Bucket=bucket_name, Key=file_key_4)
obj5 = s3.get_object(Bucket=bucket_name, Key=file_key_5)
obj6 = s3.get_object(Bucket=bucket_name, Key=file_key_6)
obj7 = s3.get_object(Bucket=bucket_name, Key=file_key_7)
obj8 = s3.get_object(Bucket=bucket_name, Key=file_key_8)
obj9 = s3.get_object(Bucket=bucket_name, Key=file_key_9)
obj10 = s3.get_object(Bucket=bucket_name, Key=file_key_10)
obj11 = s3.get_object(Bucket=bucket_name, Key=file_key_11)
obj12 = s3.get_object(Bucket=bucket_name, Key=file_key_12)
obj13 = s3.get_object(Bucket=bucket_name, Key=file_key_13)
obj14 = s3.get_object(Bucket=bucket_name, Key=file_key_14)
obj15 = s3.get_object(Bucket=bucket_name, Key=file_key_15)
obj16 = s3.get_object(Bucket=bucket_name, Key=file_key_16)



df = pd.read_excel(BytesIO(obj['Body'].read()))
df2 = pd.read_excel(BytesIO(obj2['Body'].read()))
df3 = pd.read_excel(BytesIO(obj3['Body'].read()))
df4 = pd.read_excel(BytesIO(obj4['Body'].read()))
df5 = pd.read_excel(BytesIO(obj5['Body'].read()), header=5)
df6 = pd.read_excel(BytesIO(obj6['Body'].read()))
df7 = pd.read_excel(BytesIO(obj7['Body'].read()))
df8 = pd.read_excel(BytesIO(obj8['Body'].read()))
df9 = pd.read_excel(BytesIO(obj9['Body'].read()))
df10 = pd.read_excel(BytesIO(obj10['Body'].read()))
df11 = pd.read_excel(BytesIO(obj11['Body'].read()))
df12 = pd.read_excel(BytesIO(obj12['Body'].read()))
df13 = pd.read_excel(BytesIO(obj13['Body'].read()))
df14 = pd.read_excel(BytesIO(obj14['Body'].read()), header=5)
df15 = pd.read_excel(BytesIO(obj15['Body'].read()))
df16 = pd.read_excel(BytesIO(obj16['Body'].read()), header=5)



print("df12 dtypes:")
print(df12.dtypes)

expaned_tech_day_schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Job/Est Date", StringType(), True),
    StructField("Job/Est Time", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Job/Est #", DoubleType(), True),
    StructField("PO #", StringType(), True),
    StructField("Customer", StringType(), True),
    StructField("Service Location", StringType(), True),
    StructField("Job/Est Description", StringType(), True),
    StructField("Primary Contact", StringType(), True),
    StructField("Notes For Techs", StringType(), True),
    StructField("Completion Notes", StringType(), True),
    StructField("Job Created At", StringType(), True),
    StructField("Total Jobs/Estimates", DoubleType(), True)
])


revenue_report_schema = StructType([
    StructField("Job#", DoubleType(), True),
    StructField("Date", StringType(), True),
    StructField("Time", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("PO/Ref#", StringType(), True),
    StructField("Job Category", StringType(), True),
    StructField("Job Source", DoubleType(), True),
    StructField("Customer", StringType(), True),
    StructField("Parent Customer", DoubleType(), True),
    StructField("Service Location Address 1", StringType(), True),
    StructField("Service Location Address 2", StringType(), True),
    StructField("Service Location City", StringType(), True),
    StructField("Service Location State", StringType(), True),
    StructField("Service Location Zip", StringType(), True),
    StructField("Products", DoubleType(), True),
    StructField("Services", DoubleType(), True),
    StructField("Labor", DoubleType(), True),
    StructField("Expenses (B)", DoubleType(), True),
    StructField("Total", DoubleType(), True),
    StructField("Payments/Deposits", DoubleType(), True),
    StructField("Total Due", DoubleType(), True),
    StructField("Job Details", StringType(), True),
    StructField("Tech(s) Assigned", StringType(), True),
    StructField("Completion Notes", StringType(), True),
    StructField("Labor Time", StringType(), True),
    StructField("Drive Time", StringType(), True),
    StructField("Job Charges", StringType(), True),
    StructField("Qty", DoubleType(), True),
    StructField("Rate", DoubleType(), True),
    StructField("Total.1", DoubleType(), True),
    StructField("Cost", DoubleType(), True),
    StructField("Job Subtotal", DoubleType(), True),
    StructField("Total Time & Labor", DoubleType(), True),
    StructField("Total Billable Expenses", DoubleType(), True),
    StructField("Job Total", DoubleType(), True),
    StructField("Parts Cost", DoubleType(), True),
    StructField("Service Cost", DoubleType(), True),
    StructField("Drive Time Cost", DoubleType(), True),
    StructField("Labor Time Cost", DoubleType(), True),
    StructField("Job Created At", StringType(), True)
])
job_activity_schema = StructType([
    StructField("Job#", IntegerType(), True),
    StructField("Job Date", StringType(), True),
    StructField("Customer Name", StringType(), True),
    StructField("Parent Customer Name", DoubleType(), True),
    StructField("Job Description", StringType(), True),
    StructField("Activity", StringType(), True),
    StructField("Time of Activity", StringType(), True),
    StructField("User", StringType(), True),
    StructField("Techs", StringType(), True)
])
product_service_sales_report_schema = StructType([
    StructField("Service Tech", StringType(), True),
    StructField("Product or Service", StringType(), True),
    StructField("Job#", StringType(), True),  # object → StringType
    StructField("Date", StringType(), True),
    StructField("Customer", StringType(), True),
    StructField("Qty", StringType(), True),  # object → StringType
    StructField("Rate", DoubleType(), True),
    StructField("Tax Amount($)", StringType(), True),  # object → StringType
    StructField("Tax Name", StringType(), True),
    StructField("Job Created At", StringType(), True),
    StructField("Job Category", StringType(), True),
    StructField("Product or Service Category", StringType(), True)
])

report_invoice_schema = StructType([
    StructField("Assigned Tech(s)", StringType(), True),
    StructField("Bill To City", StringType(), True),
    StructField("Bill To Location Address 1", StringType(), True),
    StructField("Bill To Location Address 2", StringType(), True),
    StructField("Bill To Location Name", StringType(), True),
    StructField("Bill To State/Province", StringType(), True),
    StructField("Bill To Zip/Post Code", StringType(), True),
    StructField("Completion Notes", StringType(), True),
    StructField("Contact Email 1", StringType(), True),
    StructField("Contact First Name", StringType(), True),
    StructField("Contact Last Name", StringType(), True),
    StructField("Customer Name", StringType(), True),
    StructField("Discount Total", DoubleType(), True),
    StructField("Invoice#", IntegerType(), True),
    StructField("Invoice Date", StringType(), True),
    StructField("Invoice Status", StringType(), True),
    StructField("Invoice Total", DoubleType(), True),
    StructField("Invoice Total Due", DoubleType(), True),
    StructField("Job Amount", DoubleType(), True),
    StructField("Job Category", StringType(), True),
    StructField("Job Date", StringType(), True),
    StructField("Job Description", StringType(), True),
    StructField("Job#", IntegerType(), True),
    StructField("PO#", StringType(), True),
    StructField("Parent Account Name", DoubleType(), True),
    StructField("Product Total", DoubleType(), True),
    StructField("Service Location Address 1", StringType(), True),
    StructField("Service Location Address 2", StringType(), True),
    StructField("Service Location City", StringType(), True),
    StructField("Service Location Name", StringType(), True),
    StructField("Service Location State/Province", StringType(), True),
    StructField("Service Location Zip/Post Code", StringType(), True),
    StructField("Service Total", DoubleType(), True),
    StructField("Tax Total", DoubleType(), True),
    StructField("Tax Rate Name", StringType(), True)
])

filtered_schema_1 = StructType([f for f in expaned_tech_day_schema.fields if f.name in df.columns])
filtered_schema_2 = StructType([f for f in revenue_report_schema.fields if f.name in df2.columns])
filtered_schema_3 = StructType([f for f in job_activity_schema.fields if f.name in df3.columns])
filtered_schema_4 = StructType([f for f in product_service_sales_report_schema.fields if f.name in df4.columns])
filtered_schema_5 = StructType([f for f in report_invoice_schema.fields if f.name in df5.columns])
filtered_schema_6 = StructType([f for f in expaned_tech_day_schema.fields if f.name in df6.columns])
filtered_schema_7 = StructType([f for f in expaned_tech_day_schema.fields if f.name in df7.columns])
filtered_schema_8 = StructType([f for f in revenue_report_schema.fields if f.name in df8.columns])
filtered_schema_9 = StructType([f for f in revenue_report_schema.fields if f.name in df9.columns])
filtered_schema_10 = StructType([f for f in job_activity_schema.fields if f.name in df10.columns])
filtered_schema_11 = StructType([f for f in job_activity_schema.fields if f.name in df11.columns])
filtered_schema_12 = StructType([f for f in product_service_sales_report_schema.fields if f.name in df12.columns])
filtered_schema_13 = StructType([f for f in product_service_sales_report_schema.fields if f.name in df13.columns])
filtered_schema_14 = StructType([f for f in report_invoice_schema.fields if f.name in df14.columns])
filtered_schema_15 = StructType([f for f in job_activity_schema.fields if f.name in df15.columns])
filtered_schema_16 = StructType([f for f in report_invoice_schema.fields if f.name in df16.columns])



spark_df_1 = spark.createDataFrame(df, schema=filtered_schema_1)
spark_df_2 = spark.createDataFrame(df2, schema=filtered_schema_2)
spark_df_3 = spark.createDataFrame(df3, schema=filtered_schema_3)
spark_df_4 = spark.createDataFrame(df4,schema=filtered_schema_4)
spark_df_5 = spark.createDataFrame(df5, schema=filtered_schema_5)
spark_df_6 = spark.createDataFrame(df6, schema=filtered_schema_6)
spark_df_7 = spark.createDataFrame(df7, schema=filtered_schema_7)
spark_df_8 = spark.createDataFrame(df8, schema=filtered_schema_8)
spark_df_9 = spark.createDataFrame(df9, schema=filtered_schema_9)
spark_df_10 = spark.createDataFrame(df10, schema=filtered_schema_10)
spark_df_11 = spark.createDataFrame(df11, schema=filtered_schema_11)
spark_df_12 = spark.createDataFrame(df12, schema=filtered_schema_12)
spark_df_13 = spark.createDataFrame(df13, schema=filtered_schema_13)
spark_df_14 = spark.createDataFrame(df14, schema=filtered_schema_14)
spark_df_15 = spark.createDataFrame(df15, schema=filtered_schema_15)
spark_df_16 = spark.createDataFrame(df16, schema=filtered_schema_16)







print("Spark DataFrame 16 preview:")
spark_df_8.show(100)
counter = spark_df_1.groupBy("Job/Est #","Job/Est Date") \
    .agg(count("Status").alias("abc")) \
    .filter(col("abc") > 1)
counter.show()

distinct_count2 = spark_df_6.select("Job/Est #","Job/Est Date","Job/Est Time").distinct().count()
distinct_count3 = spark_df_7.select("Job/Est #","Job/Est Date","Job/Est Time").distinct().count()




#print(spark_df_1.count())
#print(spark_df_2.count())
#print(spark_df_3.count())
#print(spark_df_4.count())
#print(spark_df_5.count())
#print(spark_df_6.count())
#print(spark_df_7.count())
#print(spark_df_8.count())
#print(spark_df_9.count())
#print(spark_df_10.count())
#print(spark_df_11.count())
#print(spark_df_12.count())
#print(spark_df_13.count())
#print(spark_df_14.count())
#print(spark_df_15.count())
#print(spark_df_16.count())




