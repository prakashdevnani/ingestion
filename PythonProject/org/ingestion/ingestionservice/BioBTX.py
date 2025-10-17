import requests
from bs4 import BeautifulSoup
import re
from pyspark.sql.functions import current_date, lit
from org.ingestion.utils.Executor import Executor
from pyspark.sql.functions import current_date,lit
from org.ingestion.utils.Utils import Utils
class BioBTX(Executor):
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
        self.save_details(description, clients, news)

    def fetch_description(self):
        """Scrape the About / Home page for a descriptive paragraph."""
        url = 'https://biobtx.com/about-us/'
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching About page: {e}")
            return None

        soup = BeautifulSoup(resp.content, 'html.parser')

        # Try to pick up main intro text
        candidates = soup.select('section.hero p, .intro p, .content p, .about p')
        for p in candidates:
            txt = p.get_text(strip=True)
            if txt and len(txt.split()) > 20:
                return txt

        # fallback: first <p>
        first_p = soup.find('p')
        return first_p.get_text(strip=True) if first_p else None

    def fetch_clients(self):
        """Scrape homepage or client/partner sections for logos & infer names."""
        url = 'https://biobtx.com/'
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching homepage: {e}")
            return None

        soup = BeautifulSoup(resp.content, 'html.parser')
        names = []

        # look for logos images in client / partner sections
        imgs = soup.select("img[src*='logo'], img[src*='Logo'], img[src*='client'], .partner img")
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
        """Scrape the latest press/news/blog article from BioBTX’s site."""
        base = "https://biobtx.com"
        possible_news_paths = ["/news", "/news/"]
        news_soup = None
        news_url = None

        for path in possible_news_paths:
            try:
                resp = requests.get(base + path, timeout=10)
                if resp.status_code == 200:
                    news_soup = BeautifulSoup(resp.content, 'html.parser')
                    news_url = base + path
                    break
            except requests.RequestException:
                continue

        # If no news page found, fallback to homepage links
        if news_soup is None:
            resp = requests.get(base, timeout=10)
            resp.raise_for_status()
            news_soup = BeautifulSoup(resp.content, 'html.parser')
            news_url = base

        # Find first article link
        article_a = news_soup.find("a", href=True, text=re.compile(r".+", re.I))
        if not article_a:
            return "No news articles found."

        href = article_a['href']
        article_link = href if href.startswith("http") else f"{base.rstrip('/')}/{href.lstrip('/')}"

        result = f"url: {article_link}\n\n"

        # Fetch article page
        try:
            art_resp = requests.get(article_link, timeout=10)
            art_resp.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching news article: {e}")
            return result

        art_soup = BeautifulSoup(art_resp.content, 'html.parser')

        # Title
        heading = art_soup.find(["h1", "h2"])
        title = heading.get_text(strip=True) if heading else "N/A"
        result += f"title: {title}\n"

        # Date
        date = "N/A"
        time_tag = art_soup.find("time")
        if time_tag:
            date = time_tag.get_text(strip=True)
        result += f"date: {date}\n"

        # Author
        author = "N/A"
        author_elem = art_soup.find(string=re.compile(r"By\s+", re.I))
        if author_elem:
            author = author_elem.strip()
        result += f"author: {author}\n"

        # Summary — first paragraph
        p = art_soup.find("p")
        summary = p.get_text(strip=True) if p else "N/A"
        result += f"summary: {summary}\n"

        return result

    def save_details(self,description,clients,news):

        Utils.save_details(self.spark,description, clients, news)
