# utils
import re
import urllib.parse


class Utils:
    @staticmethod
    def get_client_name(filename):
        filename = urllib.parse.unquote(filename)

        name = re.sub(r'^(?:[0-9a-fA-F]+_)+', '', filename)

        name = re.sub(r'\.[^.]+$', '', name)

        name = re.sub(r'[_\-]+', ' ', name)
        name = re.sub(r'\s+', ' ', name).strip()

        name = re.sub(r'\blogo\b', '', name, flags=re.I)
        name = re.sub(r'\borig\b', '', name, flags=re.I)
        name = re.sub(r'\(\d+\)$', '', name).strip()
        name = (" ").join(name.split("/")[-1] \
                          .split(" ")[1:]) \
            .replace("%20", " ") \
            .replace("noun circle arrow right up 2892975 FFFFFF", "")
        name = re.sub(r'(\d)+(\d)+[a-z0-9]+(\s)+', '', name)
        return name

    @staticmethod
    def save_details(spark, description, clients, news):
        df = spark.createDataFrame([[description, clients, news]], ["description", "clients", "news"])
        df.withColumn("updt_date", current_date()) \
            .withColumn("filename", lit("EOCharging")) \
            .write \
            .format("com.crealytics.spark.excel") \
            .option("sheetName", "EO_Data") \
            .option("useHeader", "true") \
            .mode("overwrite") \
            .partitionBy("updt_date", "filename") \
            .save("output")