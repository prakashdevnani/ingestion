import requests
from bs4 import BeautifulSoup
import re
from org.ingestion.utils.Executor import Executor
from org.ingestion.utils.Utils import Utils
class ChargePoint(Executor):
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
        """Scrape About / Why ChargePoint page for company description"""
        url = 'https://www.chargepoint.com/about/'
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching URL: {e}")
            return None

        soup = BeautifulSoup(response.content, 'html.parser')

        # Try grabbing a main intro / hero paragraph
        intro_paras = soup.select('section.hero p, section.intro p, .about-text p, .why-chargepoint p')
        for p in intro_paras:
            txt = p.get_text(strip=True)
            if txt and len(txt.split()) > 20:
                return txt

        # fallback: first <p> in body
        first_p = soup.find('p')
        return first_p.get_text(strip=True) if first_p else None

    def fetch_clients(self):
        """Scrape homepage or “partners” section for client/partner logos"""
        url = 'https://www.chargepoint.com/'
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching home page: {e}")
            return None

        soup = BeautifulSoup(response.content, 'html.parser')
        names = []
        # look for img tags possibly in partner/carousel sections
        imgs = soup.select("img[src*='logo'], img[src*='partner'], img[src*='client']")
        for img in imgs:
            src = img.get('src', '')
            filename = src.split('/')[-1]
            clean = re.sub(r"[-_]\d+x\d+(-\d+)?", "", filename)
            clean = re.sub(r"[-_]\d+", "", clean)
            clean = re.sub(r"\.(png|jpg|jpeg|webp)$", "", clean, flags=re.I)
            clean = re.sub(r"[-_]+", " ", clean).strip().title()
            if clean and clean not in names:
                names.append(clean)
        return ",".join(names) if names else None

    def get_news_details(self):
        """Scrape the latest news / press release from ChargePoint’s News page"""
        base = "https://www.chargepoint.com"
        news_url = "https://www.chargepoint.com/about/news"
        try:
            resp = requests.get(news_url, timeout=10)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching news list: {e}")
            return None

        soup = BeautifulSoup(resp.content, 'html.parser')

        # find first article link
        # e.g. find <a> inside .news-list or similar
        article_a = soup.find("a", href=True, text=re.compile(r".+"))
        if not article_a:
            return "No news found."

        href = article_a['href']
        if not href.startswith("http"):
            article_link = base.rstrip('/') + '/' + href.lstrip('/')
        else:
            article_link = href

        result = f"url: {article_link}\n\n"

        # Fetch the article page
        try:
            art = requests.get(article_link, timeout=10)
            art.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching news article: {e}")
            return result

        art_soup = BeautifulSoup(art.content, 'html.parser')

        # Title
        title = art_soup.find(["h1", "h2"])
        result += f"title: {title.get_text(strip=True) if title else 'N/A'}\n"

        # Date (often in <time> or a meta tag)
        date = "N/A"
        time_tag = art_soup.find("time")
        if time_tag:
            date = time_tag.get_text(strip=True)
        result += f"date: {date}\n"

        # Author (if available)
        author = "N/A"
        author_tag = art_soup.find(string=re.compile(r"By\s+", re.I))
        if author_tag:
            author = author_tag.strip()
        result += f"author: {author}\n"

        # Summary (first paragraph)
        p = art_soup.find("p")
        summary = p.get_text(strip=True) if p else "N/A"
        result += f"summary: {summary}\n"

        return result

    def save_details(self,description,clients,news):

        Utils.save_details(self.spark,description, clients, news)