import requests
from bs4 import BeautifulSoup
import re
from org.ingestion.utils.Executor import Executor
from pyspark.sql.functions import current_date,lit
from org.ingestion.utils.Utils import Utils
class CharmIndustrial(Executor):
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
        """Scrape the Company / About page for a descriptive paragraph."""
        url = 'https://www.charmindustrial.com/company'
        # If “company” route doesn’t exist, fall back to homepage
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                # fallback to homepage
                resp = requests.get('https://www.charmindustrial.com/', timeout=10)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching About/Company page: {e}")
            return None

        soup = BeautifulSoup(resp.content, 'html.parser')

        # Try paragraphs in sections with “company”, “about”, “our mission”, etc.
        candidates = soup.select('section p, .company p, .about p, .mission p, .intro p')
        for p in candidates:
            txt = p.get_text(strip=True)
            if txt and len(txt.split()) > 20:
                return txt

        # fallback: first meaningful <p>
        first_p = soup.find('p')
        return first_p.get_text(strip=True) if first_p else None

    def fetch_clients(self):
        """Scrape homepage for client / partner logos and infer names."""
        url = 'https://www.charmindustrial.com/'
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching homepage: {e}")
            return None

        soup = BeautifulSoup(resp.content, 'html.parser')
        names = []

        # The homepage has logos (Frontier, Stripe, Alphabet, Shopify, Meta, etc.) under “Total tonnes removed” section. :contentReference[oaicite:0]{index=0}
        imgs = soup.select("img[src*='logo'], img[src*='Logo'], img[src*='frontier'], img[src*='alphabet'], img[src*='meta'], img[src*='shopify'], img[src*='stripe']")
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
        """Scrape the latest blog / press / “Resources” article from Charm’s site."""
        base = "https://www.charmindustrial.com"
        # They have a “Blog” link in footer. :contentReference[oaicite:1]{index=1}
        news_url = base + "/blog"
        try:
            resp = requests.get(news_url, timeout=10)
            if resp.status_code != 200:
                # maybe “news” or “resources”
                resp = requests.get(base + "/resources", timeout=10)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching news listing: {e}")
            return None

        soup = BeautifulSoup(resp.content, 'html.parser')

        # Try find first article link
        article_a = soup.find("a", href=True, text=re.compile(r".+"))
        if not article_a:
            return "No articles found."

        href = article_a['href']
        if not href.startswith("http"):
            article_link = base.rstrip("/") + "/" + href.lstrip("/")
        else:
            article_link = href

        result = f"url: {article_link}\n\n"

        # Fetch article page
        try:
            art_resp = requests.get(article_link, timeout=10)
            art_resp.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching article: {e}")
            return result

        art_soup = BeautifulSoup(art_resp.content, 'html.parser')

        # Title
        heading = art_soup.find(["h1", "h2"])
        title = heading.get_text(strip=True) if heading else "N/A"
        result += f"title: {title}\n"

        # Date (often in <time> tag)
        date = "N/A"
        time_tag = art_soup.find("time")
        if time_tag:
            date = time_tag.get_text(strip=True)
        result += f"date: {date}\n"

        # Author (if “By …” exists)
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