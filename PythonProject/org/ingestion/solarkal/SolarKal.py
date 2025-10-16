import requests
from bs4 import BeautifulSoup

from org.ingestion.utils.Executor import Executor
from org.ingestion.utils.Utils import Utils


class SolarKal(Executor):
    spark = None

    def __init__(self):
        pass

    def execute_ingestion(self, spark):
        description = self.fetch_description()
        print(description)
        clients = self.fetch_clients()
        print(clients)
        news = self.get_news_details()
        print(news)
    def fetch_description(self):
        url = 'https://www.solarkal.com/our-services'
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')

        service_sections = soup.find_all('section', class_='s dblue-bg')

        for section in service_sections:
            description = section.find('p')
            if description:
                return description.get_text(strip=True)

    def fetch_clients(self):
        url = "https://www.solarkal.com/featured-clients"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")

        section = soup.select_one("body > section.s.less-padding")

        client_names = []

        if section:
            images = section.find_all("img")
            for img in images:
                img_url = img.get("src")
                value =Utils.get_client_name(img_url)
                if(value.strip()!=""):
                    client_names.append(value)
        return (",").join(client_names)

    def get_news_details(self):
        returnres=""
        url = "https://www.solarkal.com/blog/solar-hedge-against-rising-energy-costs"
        returnres+=f"url: {url}\n\n"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")

        section = soup.select_one("section.half-bottom")
        heading = section.find("h1")
        returnres+=f"heading: {heading.get_text(strip=True)}\n\n"
        div = soup.find("div", class_="div-block-25")

        if div:

            ps = div.find_all("p")

            date_text = ps[2].get_text(strip=True)
            returnres+=f"date: {date_text}\n\n"
        paragraphs = [p.get_text(strip=True) for p in soup.find_all("p")]

        returnres+=f"summary : {paragraphs[7]}\n\n"
        return returnres
    def save_details(self,description,clients,news):

        df=self.spark.createDataFrame([[description],[clients],[news]])
        df.write\
          .format("com.crealytics.spark.excel")\
          .option("sheetName", "MyDataSheet")\
          .option("useHeader", "true")\
          .mode("overwrite")\
          .save("output.xlsx")