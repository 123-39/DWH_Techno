import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as sf

def main(argv):

    price_path = argv[1]
    products_for_stat = argv[2]
    output_path = argv[3]

    spark = SparkSession.builder.getOrCreate()
    
    
    productStatDF = spark.read.csv(products_for_stat, sep=';').select(
        sf.col("_c0").cast(IntegerType()).alias("product_id")
    )

    priceDF = spark.read.csv(price_path, sep=';').select(
        sf.col("_c0").cast(LongType()).alias("rs_city_id"),
        sf.col("_c1").cast(IntegerType()).alias("product_id"),
        sf.col("_c2").alias("price")
    )
    priceDF = priceDF.withColumn("price", sf.regexp_replace("price", ',', '.').cast("float")) 
    
    
    price_stat = (
        priceDF.alias("left")
        .join(productStatDF, priceDF.product_id == productStatDF.product_id, "inner")
        .select(sf.col("left.rs_city_id"), sf.col("left.product_id"), sf.col("left.price"))
        .groupBy("product_id")
        .agg(
            sf.min(sf.col("price")).cast(DecimalType(20,2)).alias("min_price"),
            sf.max(sf.col("price")).cast(DecimalType(20,2)).alias("max_price"),
            sf.avg(sf.col("price")).cast(DecimalType(20,2)).alias("avg_price"),
        )
        .orderBy(["product_id"], ascending=True)
    )
    
    
    (price_stat.repartition(1)
     .write
     .mode("overwrite")
     .option("header", "true")
     .option("sep", ";")
     .csv(output_path)
    )

    
if __name__ == '__main__':
    sys.exit(main(sys.argv))

