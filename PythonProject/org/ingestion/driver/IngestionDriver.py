import configparser

from org.ingestion.driver.IngestionPipeline import IngestionPipeline
from pyspark.sql import SparkSession

class IngestionDriver:
    def main(self):
        spark = SparkSession.builder \
            .appName("MyApp") \
            .master("local[*]") \
            .getOrCreate()
        config = configparser.RawConfigParser()
        config.read('config.properties')

        print(config.get('Ingestion', 'destination.location'))
        company_name = config.get('Ingestion', 'source.companyname')
        print(company_name)
        ing_obj = IngestionPipeline(company_name)
        ing_obj.execute(spark)


if __name__ == "__main__":
    objIng = IngestionDriver()
    objIng.main()