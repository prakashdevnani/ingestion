import requests
from bs4 import BeautifulSoup
import re

from org.ingestion.utils.Executor import Executor
from pyspark.sql.functions import current_date,lit
from org.ingestion.utils.Utils import Utils


class Prewave(Executor):
    spark = None

    def __init__(self):
        pass

    def execute_ingestion(self, spark):
        self.spark = spark
        description = self.fetch_description()
        print(description)
        clients = self.fetch_clients()
        print(clients)
        news = self.get_news_details()
        print(news)

    def fetch_description(self):
        url = 'https://www.prewave.com/about-us/'
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching URL: {e}")
            return None

        soup = BeautifulSoup(response.content, 'html.parser')

        # Find about text blocks or first few paragraphs
        sections = soup.find_all(['section', 'div'], class_=lambda c: c and ('about' in c or 'content' in c))
        for section in sections:
            paragraphs = section.find_all('p')
            for p in paragraphs:
                text = p.get_text(strip=True)
                if text and len(text.split()) > 20:
                    return text

        # fallback
        first_para = soup.find('p')
        return first_para.get_text(strip=True) if first_para else None

    def fetch_clients(self):
        import re
        url = "https://www.prewave.com/"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching URL: {e}")
            return None
        soup = BeautifulSoup(response.content, "html.parser")

        # find all client or partner logo images
        logos = soup.select("img[src*='logo'], img[src*='client'], img[src*='partner'], img[src*='customer']")
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
        return ",".join(names)

    def get_news_details(self):
        returnres = ""
        base_url = "https://www.prewave.com/"
        news_page = "https://www.prewave.com/news/"

        try:
            response = requests.get(news_page, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching news page: {e}")
            return None

        soup = BeautifulSoup(response.content, "html.parser")

        # Find first article link (latest news)
        article = soup.find("a", href=True, text=re.compile(r".+", re.I))
        if not article:
            return "No articles found."

        article_url = article["href"]
        if not article_url.startswith("http"):
            article_url = base_url.rstrip("/") + "/" + article_url.lstrip("/")
        returnres += f"url: {article_url}\n\n"

        # Fetch article content
        try:
            art_resp = requests.get(article_url, timeout=10)
            art_resp.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching article: {e}")
            return returnres

        art_soup = BeautifulSoup(art_resp.content, "html.parser")

        # Extract title
        heading = art_soup.find(["h1", "h2"])
        title = heading.get_text(strip=True) if heading else "N/A"
        returnres += f"title: {title}\n\n"

        # Extract publication date and author if available
        author = "N/A"
        date = "N/A"
        meta_date = art_soup.find("time")
        if meta_date:
            date = meta_date.get_text(strip=True)

        author_elem = art_soup.find(string=re.compile(r"By ", re.I))
        if author_elem:
            author = author_elem.replace("By", "").strip()

        returnres += f"author: {author}\n"
        returnres += f"date: {date}\n\n"

        # Extract summary (first paragraph)
        paragraph = art_soup.find("p")
        summary = paragraph.get_text(strip=True) if paragraph else "N/A"
        returnres += f"summary: {summary}\n\n"

        return returnres

    def save_details(self,description,clients,news):

        Utils.save_details(self.spark,description, clients, news)
