
# This code uses Spark to create parquets in S3 from all the tables in the database.

# %%
import os
import shutil
from pyspark.sql.functions import col
from hereutil import here, add_to_sys_path
add_to_sys_path(here())
from src.common_basis import get_spark, get_s3fs, spark_jdbc_opts  # noqa

spark = get_spark()
s3a = get_s3fs()

tables = [row[0] for row in (spark_jdbc_opts(spark.read)
                             .option("query", "SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA='disc' AND table_name LIKE 'cmw_%_a'")
                             .load().collect())]

# %%
for table in tables:
    if 'sample' in table:
        print(table)
        t = (spark_jdbc_opts(spark.read)
            .option("dbtable", table)
            .load())
        t = t.select([col(c.name).cast("long") if c.dataType.typeName()
                    == "decimal" else col(c.name) for c in t.schema])
        (t.repartition(1)
            .write
            .mode("overwrite")
            .option("compression", "zstd")
            .parquet("s3a://dhh24/disc/parquet/t_"+table+".parquet"))
        f = next(filter(lambda x: x.endswith(".parquet"), s3a.ls(
            "dhh24/disc/parquet/t_"+table+".parquet")))
        s3a.mv(f, "dhh24/disc/parquet/"+table+".parquet")
        s3a.rm("dhh24/disc/parquet/t_"+table+".parquet", recursive=True)

print("Done.")

# %%
