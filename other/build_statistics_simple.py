import sys

from pyspark.sql.functions import col
from pyspark.sql import SparkSession

dt = sys.argv[1]
print(dt)

spark = SparkSession.builder.getOrCreate()

input_path = "gs://airflowbolcom_ghermann_dummybucket/mypgdata_*"
# input_path = "gs://airflow-training-data/land_registry_price_paid_uk/*/*.json"

spark.read.json(
    input_path
).withColumn(
    "transfer_date", col("transfer_date").cast("timestamp").cast("date")
).createOrReplaceTempView(
    "land_registry_price_paid_uk"
)

# >>> df.printSchema()
# root
#  |-- city: string (nullable = true)
#  |-- county: string (nullable = true)
#  |-- district: string (nullable = true)
#  |-- duration: string (nullable = true)
#  |-- locality: string (nullable = true)
#  |-- newly_built: boolean (nullable = true)
#  |-- paon: string (nullable = true)
#  |-- postcode: string (nullable = true)
#  |-- ppd_category_type: string (nullable = true)
#  |-- price: double (nullable = true)
#  |-- property_type: string (nullable = true)
#  |-- record_status: string (nullable = true)
#  |-- saon: string (nullable = true)
#  |-- street: string (nullable = true)
#  |-- transaction: string (nullable = true)
#  |-- transfer_date: double (nullable = true)

# Do some aggregations and write it back to Cloud Storage
aggregation = spark.sql(
    """
    SELECT
        CAST(CAST(transfer_date AS timestamp) AS date) transfer_date,
        county,
        district,
        city,
        price
    FROM
        land_registry_price_paid_uk
    WHERE
        transfer_date = '{}'
    GROUP BY
        transfer_date,
        county,
        district,
        city
    ORDER BY
        county,
        district,
        city
""".format(
        dt
    )
)

(
    aggregation.write.mode("overwrite")
    .partitionBy("transfer_date")
    .parquet("gs://airflow-training-data/average_prices/")
)