import os

from pyspark.sql import SparkSession

POLARIS_PRINCIPAL = os.environ.get("POLARIS_PRINCIPAL", "dev")  # "dev" or "analyst"
POLARIS_CREDENTIAL = os.environ[f"POLARIS_{POLARIS_PRINCIPAL.upper()}_CREDENTIAL"]

BUCKET = os.environ["LAB_BUCKET_NAME"]
POLARIS_URI = os.environ["POLARIS_URI"]
POLARIS_CATALOG = os.environ["POLARIS_CATALOG"]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

spark = (
    SparkSession.builder
    .appName("lab-polaris")
    .master("local[*]")
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_REGION"))
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.polaris.type", "rest")
    .config("spark.sql.catalog.polaris.uri", f"{POLARIS_URI}/api/catalog")
    .config("spark.sql.catalog.polaris.warehouse", POLARIS_CATALOG)
    .config("spark.sql.catalog.polaris.credential", POLARIS_CREDENTIAL)
    .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
    .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print("Polaris principal:", POLARIS_PRINCIPAL)
