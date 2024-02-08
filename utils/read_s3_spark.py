from pyspark.sql import SparkSession
import subprocess
import platform
import os

spark = SparkSession.builder.appName('s3_read')\
    .config("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")\
    .getOrCreate()

python_version = platform.python_version()
print(f"python_version --> {python_version}")

java_version = subprocess.check_output(['java', '-version'], stderr=subprocess.STDOUT)
print(f"java_version --> {java_version}")

spark_version = spark.version
print(f"spark_version --> {spark_version}")

user = ""
passphrase = ""
region = ""
proxy = ""
tablePath = ""
s3_bucket = ""
s3SourcePath = f"s3a://{s3_bucket}/{tablePath}"

os.environ["HTTP_PROXY"] = ""
os.environ["HTTPS_PROXY"] = ""
os.environ["AWS_ACCESS_KEY_ID"] = user
os.environ["AWS_SECRET_ACCESS_KEY"] = passphrase
os.environ["AWS_DEFAULT_REGION"] = region

spark.sparkContext.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")

df_1 = spark.read.parquet(s3SourcePath)
df_1.show(truncate=False)
#
# try:
#     df_1 = spark.read.parquet(s3SourcePath)
#     df_1.show(truncate=False)
# except Exception as e:
#     print(f"Exception while trying to connect to S3 "
#           f"using spark.read.parquet, "
#           f"e --> {e}")
#
#
# try:
#     spark.conf.set("spark.hadoop.fs.s3a.access.key", user)
#     spark.conf.set("spark.hadoop.fs.s3a.secret.key", passphrase)
#     spark.conf.set("spark.hadoop.fs.s3a.default.region", region)
#     spark.conf.set("spark.hadoop.fs.s3a.proxy.host", proxy)
#     spark.conf.set("spark.hadoop.fs.s3a.proxy.port", "80")
#
#     df_1 = spark.read.parquet(s3SourcePath)
#
#     df_1.show(truncate=False)
# except Exception as e:
#     print(f"Exception while trying to connect to S3 "
#           f"using spark.conf.set, without proxy.endpoint, "
#           f"e --> {e}")
#
# try:
#     spark.conf.set("spark.hadoop.fs.s3a.access.key", user)
#     spark.conf.set("spark.hadoop.fs.s3a.secret.key", passphrase)
#     spark.conf.set("spark.hadoop.fs.s3a.proxy.host", proxy)
#     spark.conf.set("spark.hadoop.fs.s3a.proxy.port", "80")
#     spark.conf.set("spark.hadoop.fs.s3a.proxy.endpoint", "s3.us-east-1.amazonaws.com")
#
#     df_1 = spark.read.parquet(s3SourcePath)
#
#     df_1.show(truncate=False)
# except Exception as e:
#     print(f"Exception while trying to connect to S3 "
#           f"using spark.conf.set, with proxy.endpoint, "
#           f"e --> {e}")
#
# try:
#     spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", user)
#     spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", passphrase)
#     spark.sparkContext.hadoopConfiguration.set("fs.s3a.proxy.host", proxy)
#     spark.sparkContext.hadoopConfiguration.set("fs.s3a.proxy.port", "80")
#
#     df_1 = spark.read.parquet(s3SourcePath)
#
#     df_1.show(truncate=False)
# except Exception as e:
#     print(f"Exception while trying to connect to S3 "
#           f"using spark.sparkContext.hadoopConfiguration.set, without proxy.endpoint, "
#           f"e --> {e}")
#
# try:
#     spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", user)
#     spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", passphrase)
#     spark.sparkContext.hadoopConfiguration.set("fs.s3a.proxy.host", proxy)
#     spark.sparkContext.hadoopConfiguration.set("fs.s3a.proxy.port", "80")
#     spark.sparkContext.hadoopConfiguration.set("fs.s3a.proxy.endpoint", "s3.us-east-1.amazonaws.com")
#
#     df_1 = spark.read.parquet(s3SourcePath)
#
#     df_1.show(truncate=False)
# except Exception as e:
#     print(f"Exception while trying to connect to S3 "
#           f"using spark.sparkContext.hadoopConfiguration.set, with proxy.endpoint, "
#           f"e --> {e}")
