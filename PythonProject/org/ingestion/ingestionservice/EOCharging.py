import requests
from bs4 import BeautifulSoup
import re
from pyspark.sql.functions import current_date,lit

from org.ingestion.utils.Executor import Executor
from org.ingestion.utils.Utils import Utils

class EOCharging(Executor):
    spark = None

    def __init__(self):
        pass

    def execute_ingestion(self, spark):
        self.spark = spark
        description = self.fetch_description()
        print("Description:", description)
        clients = self.fetch_clients()
        print("Clients:", clients)
        news = self.get_news_details()
        print("News:", news)

    def fetch_description(self):
        """Scrape About page for company description"""
        url = 'https://www.eocharging.com/about-us/'
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching URL: {e}")
            return None

        soup = BeautifulSoup(response.content, 'html.parser')

        # Try main description block
        about_sections = soup.find_all(['p', 'div'], class_=lambda c: c and ('about' in c or 'content' in c))
        for section in about_sections:
            text = section.get_text(strip=True)
            if text and len(text.split()) > 30:
                return text
        # Fallback: first <p> on the page
        first_para = soup.find('p')
        return first_para.get_text(strip=True) if first_para else None

    def fetch_clients(self):
        """Scrape home page or partners section for logos/clients"""
        url = "https://www.eocharging.com/"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching URL: {e}")
            return None

        soup = BeautifulSoup(response.content, "html.parser")

        # Find all logos
        logos = soup.select("img[src*='logo'], img[src*='client'], img[src*='partner']")
        names = []
        for img in logos:
            src = img.get("src", "")
            filename = src.split("/")[-1]
            clean_name = re.sub(r"[-_]\d+x\d+(-\d+)?", "", filename)
            clean_name = re.sub(r"[-_]\d+", "", clean_name)
            clean_name = re.sub(r"(\.png|\.jpg|\.jpeg|\.webp)$", "", clean_name, flags=re.I)
            clean_name = re.sub(r"[-_]+", " ", clean_name).strip().title()
            if clean_name and clean_name not in names:
                names.append(clean_name)

        return ",".join(names) if names else None

    def get_news_details(self):
        """Scrape the latest news article details"""
        base_url = "https://www.eocharging.com/"
        news_page = "https://www.eocharging.com/news/"
        try:
            response = requests.get(news_page, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching news page: {e}")
            return None

        soup = BeautifulSoup(response.content, "html.parser")

        # Find first article link
        article = soup.find("a", href=True, text=re.compile(r".+", re.I))
        if not article:
            return "No news articles found."

        article_url = article["href"]
        if not article_url.startswith("http"):
            article_url = base_url.rstrip("/") + "/" + article_url.lstrip("/")

        returnres = f"url: {article_url}\n\n"

        # Fetch article details
        try:
            art_resp = requests.get(article_url, timeout=10)
            art_resp.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching article: {e}")
            return returnres

        art_soup = BeautifulSoup(art_resp.content, "html.parser")

        # Extract date, author, title, summary
        title_elem = art_soup.find(["h1", "h2"])
        title = title_elem.get_text(strip=True) if title_elem else "N/A"

        date_elem = art_soup.find(string=re.compile(r"\b\d{4}\b"))
        date = date_elem.strip() if date_elem else "N/A"

        author_elem = art_soup.find(string=re.compile(r"By ", re.I))
        author = author_elem.replace("By", "").strip() if author_elem else "N/A"

        para = art_soup.find("p")
        summary = para.get_text(strip=True) if para else "N/A"

        returnres += f"title: {title}\n"
        returnres += f"date: {date}\n"
        returnres += f"author: {author}\n"
        returnres += f"summary: {summary}\n"

        return returnres


    def save_details(self,description,clients,news):

        Utils.save_details(self.spark,description, clients, news)