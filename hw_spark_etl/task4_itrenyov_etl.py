from luigi.contrib.spark import SparkSubmitTask
from luigi.contrib.hdfs import HdfsTarget
import luigi


USER = "itrenyov"


class PriceStat(SparkSubmitTask):
    
    app = "/user/{}/hw_spark_etl/price_stat.py".format(USER)

    price_path = luigi.Parameter()
    product_for_stat = luigi.Parameter()
    output_path = luigi.Parameter() 

    def output(self):
        return HdfsTarget(self.output_path)
    
    def app_command(self):
        return [
            self.app,
            self.price_path,
            self.product_for_stat,
            self.output_path,
        ]

    
class OkDem(SparkSubmitTask):
    
    app = "/user/{}/hw_spark_etl/ok_dem.py".format(USER)
    
    current_dt = "2023-03-01"
    price_path = luigi.Parameter()
    price_stat_path = luigi.Parameter()
    city = luigi.Parameter()
    rs_city = luigi.Parameter()
    demography_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def output(self):
        return HdfsTarget(self.output_path)
    
    def requires(self):
        return PriceStat()
    
    def app_command(self):
        return [
            self.app,
            self.current_dt,
            self.price_path,
            self.price_stat_path,
            self.city,
            self.rs_city,
            self.demography_path,
            self.output_path,
        ]
    

class ProductStat(SparkSubmitTask):
    
    app = "/user/{}/hw_spark_etl/product_stat.py".format(USER)
   
    city = luigi.Parameter()
    price_path = luigi.Parameter()
    product = luigi.Parameter()
    ok_dem_path = luigi.Parameter()
    output_path = luigi.Parameter()
    
    def output(self):
        return HdfsTarget(self.output_path)
    
    def requires(self):
        return OkDemography()
    
    def app_command(self):
        return [
            self.app,
            self.city,
            self.price_path,
            self.product,
            self.ok_dem_path,
            self.output_path,
        ]
    
if __name__ == "__main__":
    luigi.run()
