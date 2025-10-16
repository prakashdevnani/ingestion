import requests
from bs4 import BeautifulSoup

from org.ingestion.utils.Executor import Executor
from pyspark.sql.functions import current_date,lit
from org.ingestion.utils.Utils import Utils

class H2scan(Executor):
    spark=None
    def __init__(self):
        pass
    def execute_ingestion(self,spark):
        self.spark=spark
        description = self.fetch_description()
        print(description)
        clients = self.fetch_clients()
        print(clients)
        news = self.get_news_details()
        print(news)
    def fetch_description(self):
        url = 'https://h2scan.com/about/'
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching URL: {e}")
            return None

        soup = BeautifulSoup(response.content, 'html.parser')

        # Match any section that has 'one-col-text-block' in its class list (not exact string)
        service_sections = soup.find_all('section', class_=lambda c: c and 'one-col-text-block' in c)

        all_paragraphs = []
        for section in service_sections:
            paragraphs = section.find_all('p')
            for p in paragraphs:
                text = p.get_text(strip=True)
                if text:
                    return text

        return  None

    def fetch_clients(self):
        import re

        html = "https://h2scan.com/"  # Replace with your HTML string
        try:
            response = requests.get(html, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching URL: {e}")
            return None
        soup = BeautifulSoup(response.content, "html.parser")

        # Find all logo images
        logos = soup.select(".logo-carousel img")

        names = []
        for img in logos:
            src = img.get("src", "")
            # Extract filename
            filename = src.split("/")[-1]
            # Remove extension and numeric size parts
            clean_name = re.sub(r"[-_]\d+x\d+(-\d+)?", "", filename)  # remove "-1024x343-1"
            clean_name = re.sub(r"[-_]\d+", "", clean_name)  # remove other numeric parts
            clean_name = re.sub(r"(\.png|\.jpg|\.jpeg|\.webp)$", "", clean_name, flags=re.I)
            # Extract readable part
            clean_name = re.sub(r"[-_]+", " ", clean_name).strip().title()
            if clean_name and clean_name not in names:
                names.append(clean_name)

        return ",".join(names)


    def get_news_details(self):
        returnres = ""
        url = "https://h2scan.com/news/can-you-afford-not-to-transformer-downtime"  # example URL
        returnres += f"url: {url}\n\n"

        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")

        # --- Extract topic ---
        topic_elem = soup.find(text=lambda t: t and "Topics:" in t)
        topic = topic_elem.replace("Topics:", "").strip() if topic_elem else "N/A"
        returnres += f"topic: {topic}\n\n"

        # --- Extract published by and date ---
        published_elem = soup.find(text=lambda t: t and "Published By:" in t)
        if published_elem:
            # Example text: "Published By: Traci Hopkins on September 25, 2025"
            text = published_elem.replace("Published By:", "").strip()
            parts = text.split(" on ")
            author = parts[0].strip() if len(parts) > 0 else "N/A"
            date = parts[1].strip() if len(parts) > 1 else "N/A"
        else:
            author = "N/A"
            date = "N/A"

        returnres += f"author: {author}\n"
        returnres += f"date: {date}\n\n"

        # --- Extract title ---
        heading = soup.find(["h1", "h2"])
        title = heading.get_text(strip=True) if heading else "N/A"
        returnres += f"title: {title}\n\n"

        # --- Extract first paragraph as summary ---
        paragraph = soup.find("p")
        summary = paragraph.get_text(strip=True) if paragraph else "N/A"
        returnres += f"summary: {summary}\n\n"

        return returnres
    def save_details(self,description,clients,news):

        df=self.spark.createDataFrame([[description],[clients],[news]])
        df.withColumn("updt_date",current_date())\
        .withColumn("filename",lit("SolarKal"))\
         .write\
          .format("com.crealytics.spark.excel")\
          .option("sheetName", "MyDataSheet")\
          .option("useHeader", "true")\
          .mode("overwrite")\
          .partitionBy("updt_date","filename")\
          .save("output")