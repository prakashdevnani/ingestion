import requests
from bs4 import BeautifulSoup

from org.ingestion.utils.Executor import Executor
from org.ingestion.utils.Utils import Utils

class Hydrogenious(Executor):
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
        url = 'https://hydrogenious.net/'
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Example: fetch first paragraph in main content section
        section = soup.find('div', class_='elementor-widget-container')
        if section:
            p = section.find('p')
            if p:
                return p.get_text(strip=True)
        return ""

    def fetch_clients(self):
        # Hydrogenious site may not have a "clients" section
        # You can extract logos or partners if available
        url = "https://hydrogenious.net/"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")

        client_names = []
        images = soup.find_all("img")
        for img in images:
            img_url = img.get("src")
            value = Utils.get_client_name(img_url)
            if value.strip() != "":
                client_names.append(value)
        return ",".join(client_names)

    def get_news_details(self):
        returnres = ""
        url = "https://hydrogenious.net/news/"  # Adjust if specific news page exists
        returnres += f"url: {url}\n\n"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")

        section = soup.find("article")
        if section:
            heading = section.find("h2")
            returnres += f"heading: {heading.get_text(strip=True) if heading else ''}\n\n"
            date_tag = section.find("time")
            returnres += f"date: {date_tag.get_text(strip=True) if date_tag else ''}\n\n"
            summary = section.find("p")
            returnres += f"summary: {summary.get_text(strip=True) if summary else ''}\n\n"
        return returnres
    def save_details(self,description,clients,news):

        Utils.save_details(self.spark,description, clients, news)

