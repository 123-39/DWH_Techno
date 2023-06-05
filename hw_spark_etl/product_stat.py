import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as sf
from pyspark.sql.window import Window

def main(argv):

    city = argv[1]
    price_path = argv[2]
    product = argv[3]
    ok_dem_path = argv[4]
    output_path = argv[5]

    spark = SparkSession.builder.getOrCreate()
    
    
    cityDF = spark.read.csv(city, sep=';').select(
        sf.col("_c0").cast(StringType()).alias("city"),
        sf.col("_c1").cast(IntegerType()).alias("city_id")
    )
    
    priceDF = spark.read.csv(price_path, sep=';').select(
        sf.col("_c0").cast(LongType()).alias("rs_city_id"),
        sf.col("_c1").cast(IntegerType()).alias("product_id"),
        sf.col("_c2").alias("price")
    )
    priceDF = priceDF.withColumn("price", sf.regexp_replace("price", ',', '.').cast("float"))
    
    productDF = spark.read.csv(product, sep=';').select(
        sf.col("_c0").cast(StringType()).alias("product"),
        sf.col("_c1").cast(IntegerType()).alias("product_id")
    )
    
    ok_dem = spark.read.option("header", "true").csv(ok_dem_path, sep=';')
    stats = ok_dem.agg(
    	sf.max(sf.col("age_avg")),
    	sf.min(sf.col("age_avg")),
    	sf.max(sf.col("men_share")),
    	sf.max(sf.col("women_share")),
    ).collect()[0]
    
    product_stat_ok = (
        ok_dem
        .where(
        	(sf.col("age_avg") == stats[0]) |
        	(sf.col("age_avg") == stats[1]) |
        	(sf.col("men_share") == stats[2]) |
        	(sf.col("women_share") == stats[3])
        	)
        .select(sf.col("city")).alias("suitable_cities")
        .join(cityDF, sf.col("suitable_cities.city") == cityDF.city, "inner")
        .select(sf.col("suitable_cities.city"), sf.col("city_id"))
        .join(priceDF, sf.col("city_id") == priceDF.rs_city_id, "inner")
        .select(sf.col("city"), sf.col("product_id"), sf.col("price")).alias("stat_ok")
        .join(productDF, sf.col("stat_ok.product_id") == productDF.product_id, "inner")
        .select(sf.col("city"), sf.col("product"), sf.col("price")) 
        .withColumn('cheap', sf.min('price').over(Window.partitionBy('city')))
        .withColumn('expensive', sf.max('price').over(Window.partitionBy('city')))
        .select(
        	sf.col("city").alias("city_name"),
        	sf.col('price'),
        	sf.when(sf.col("price") == sf.col("cheap"), sf.col("product")).alias('min_product'),
        	sf.when(sf.col("price") == sf.col("expensive"), sf.col("product")).alias('max_product'),
        	(sf.col("expensive") - sf.col("cheap")).alias("price_difference")
    	)
    	.where(
    		(sf.col("min_product").isNotNull()) | 
    		(sf.col("max_product").isNotNull())
    	)
    	.groupBy("city_name")
    	.agg(
    		sf.last("max_product", True).alias('most_expensive_product_name'), 
        	sf.last("min_product", True).alias('cheapest_product_name'),
        	sf.last("price_difference", True).alias('price_difference'),
    	)
    )
    
    
    (product_stat.repartition(1)
     .write
     .mode("overwrite")
     .option("header", "true")
     .option("sep", ";")
     .csv(output_path) 
    )


if __name__ == '__main__':
    sys.exit(main(sys.argv))

