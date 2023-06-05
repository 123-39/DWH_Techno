import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as sf

def main(argv):

    current_dt = argv[1]
    price_path = argv[2]
    price_stat_path = argv[3]
    city = argv[4]
    rs_city = argv[5]
    demography_path = argv[6]
    output_path = argv[7]

    spark = SparkSession.builder.getOrCreate()
    
    
    priceDF = spark.read.csv(price_path, sep=';').select(
        sf.col("_c0").cast(LongType()).alias("rs_city_id"),
        sf.col("_c1").cast(IntegerType()).alias("product_id"),
        sf.col("_c2").alias("price")
    )
    priceDF = priceDF.withColumn("price", sf.regexp_replace("price", ',', '.').cast("float"))
    
    price_stat = spark.read.option("header", "true").csv(price_stat_path, sep=';')
    
    cityDF = spark.read.csv(city, sep=';').select(
        sf.col("_c0").cast(StringType()).alias("city"),
        sf.col("_c1").cast(IntegerType()).alias("city_id")
    )
    
    rsCityDF = spark.read.csv(rs_city, sep='\t').select(
        sf.col("_c0").cast(LongType()).alias("ok_city_id"),
        sf.col("_c1").cast(IntegerType()).alias("rs_city_id")
    )
    
    demographyDF = spark.read.csv(demography_path, sep='\t').select(
        sf.col("_c0").cast(IntegerType()).alias("user_id"),
        sf.col("_c1").cast(LongType()).alias("create_date"),
        sf.col("_c2").cast(IntegerType()).alias("birth_date"),
        sf.col("_c3").cast(ShortType()).alias("gender"),
        sf.col("_c4").cast(LongType()).alias("id_country"),
        sf.col("_c5").cast(LongType()).alias("id_city"),
        sf.col("_c6").cast(IntegerType()).alias("login_region")
    )
    
    
    ok_dem  = (
        priceDF
        .join(
            price_stat,
            [priceDF.product_id == price_stat.product_id, priceDF.price > price_stat.avg_price],
            "inner",
        )
        .select(sf.col('rs_city_id')).alias("rich_cities")
        .distinct()
        .join(cityDF, sf.col('rich_cities.rs_city_id') == cityDF.city_id, "inner")
        .select(sf.col("city"), sf.col("rs_city_id"))
        .join(rsCityDF, sf.col('rich_cities.rs_city_id') == rsCityDF.rs_city_id, "inner")
        .select(sf.col('city'), sf.col('ok_city_id'))
        .join(demographyDF, sf.col('ok_city_id') == demographyDF.id_city, 'inner')
        .select(
            sf.col('city'),
            sf.col('birth_date'),
            sf.col('gender'),
        )
        .groupBy(sf.col('city'))
        .agg(
            sf.count(sf.col('gender')).alias('user_cnt'),
            sf.count(sf.when(sf.col('gender') == 1, True)).alias('men_cnt'),
            sf.count(sf.when(sf.col('gender') == 2, True)).alias('women_cnt'),
            sf.avg(sf.datediff(sf.lit(current_dt), sf.from_unixtime(sf.col('birth_date') * 24 * 60 * 60)) / 365.25)
            .cast(IntegerType()).alias('age_avg'),
        )
        .select(
            sf.col('city'),
            sf.col('user_cnt'),
            sf.col('age_avg'),
            sf.col('men_cnt'),
            sf.col('women_cnt'),
            (sf.col('men_cnt') / sf.col('user_cnt')).cast(DecimalType(20,2)).alias('men_share'),
            (sf.col('women_cnt') / sf.col('user_cnt')).cast(DecimalType(20,2)).alias('women_share'),
        )
        .orderBy(sf.col('user_cnt'), ascending=False)
    )
    
    
    (ok_dem.repartition(1)
     .write
     .mode("overwrite")
     .option("header", "true")
     .option("sep", ";")
     .csv(output_path)
    )

if __name__ == '__main__':
    sys.exit(main(sys.argv))

