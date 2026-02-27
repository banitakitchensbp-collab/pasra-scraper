# auto_scrape.py
# Standalone Govt Jobs Scraper for PASRA app
# Daily auto run ke liye best - no server needed
# Requirements: pip install requests beautifulsoup4 firebase-admin

import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, firestore
import time
import re
from datetime import datetime
import logging
import json
import os

# Logging setup (console + file mein bhi save hoga)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scrape_log.txt")
    ]
)

# Firebase setup - Environment se load kar raha hai (Render ke liye perfect)
def initialize_firebase():
    cred_json = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if cred_json is None:
        logging.error("GOOGLE_APPLICATION_CREDENTIALS env variable nahi mila!")
        exit(1)

    try:
        cred_dict = json.loads(cred_json)
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred)
        logging.info("Firebase initialized successfully")
        return firestore.client()
    except Exception as e:
        logging.error(f"Firebase init fail: {e}")
        exit(1)

db = initialize_firebase()

# States keywords
STATES = {
    'odisha': ['odisha', 'orissa', 'bhubaneswar', 'cuttack', 'balasore', 'rourkela', 'bbsr'],
    'bihar': ['bihar', 'patna'],
    'uttar_pradesh': ['uttar pradesh', 'up', 'lucknow', 'kanpur'],
    'maharashtra': ['maharashtra', 'mumbai', 'pune'],
    'delhi': ['delhi', 'new delhi'],
    'all': []
}

# Sites list
SITES = [
    {"url": "https://www.indgovtjobs.in/", "name": "IndGovtJobs"},
    {"url": "https://www.sarkariresult.com/", "name": "SarkariResult"},
    {"url": "https://www.freejobalert.com/", "name": "FreeJobAlert"},
    {"url": "https://linkingsky.com/", "name": "LinkingSky"},
    {"url": "https://odishagovtjob.in/", "name": "OdishaGovtJob"},
]

def get_state_from_title(title):
    title_lower = title.lower()
    for state_key, keywords in STATES.items():
        if any(kw in title_lower for kw in keywords):
            return state_key
    return 'all'

def parse_date_str(date_str):
    formats = [
        '%d.%m.%Y', '%d-%m-%Y', '%d/%m/%Y',
        '%d.%m.%y', '%d-%m-%y', '%d/%m/%y',
        '%d %b %Y', '%d %B %Y', '%d %b, %Y', '%d %B, %Y'
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            if dt.year < 2000:
                dt = dt.replace(year=dt.year + 2000)
            return dt
        except ValueError:
            continue
    return None

def extract_last_date_from_text(text):
    patterns = [
        r'(?:Last Date|Closing Date|Application Last Date|Last Date for Apply|Deadline|Last Date to Apply)[\s:.-]*(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})',
        r'(?:Last Date|Closing Date)[\s:.-]*(\d{1,2}\s+[A-Za-z]+\s+\d{4})',
        r'(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})'
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            date_str = match.group(1).strip()
            dt = parse_date_str(date_str)
            if dt:
                return dt
    return None

def get_last_date_from_detail_page(link):
    if not link or 'http' not in link:
        return None
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(link, headers=headers, timeout=12)
        if response.status_code != 200:
            return None
        soup = BeautifulSoup(response.text, 'html.parser')
        full_text = soup.get_text(separator=' ', strip=True)
        dt = extract_last_date_from_text(full_text)
        if dt:
            return dt
        return None
    except Exception as e:
        logging.error(f"Detail page error for {link}: {e}")
        return None

def scrape_from_site(url, site_name):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code != 200:
            logging.warning(f"{site_name} returned {response.status_code}")
            return []
        soup = BeautifulSoup(response.text, 'html.parser')
        jobs = []

        if site_name == "IndGovtJobs":
            heading = soup.find(lambda tag: tag.name in ['h2', 'h3'] and 'Latest Government Jobs' in tag.get_text(strip=True))
            if heading:
                ul = heading.find_next('ul')
                if ul:
                    for li in ul.find_all('li'):
                        a = li.find('a')
                        if a:
                            title = a.text.strip()
                            link = a['href']
                            if not link.startswith('http'):
                                link = "https://www.indgovtjobs.in" + link
                            if len(title) > 15:
                                jobs.append({'title': title, 'link': link, 'site': site_name})

        elif site_name == "SarkariResult":
            links = soup.find_all('a', href=True)
            for a in links:
                title = a.text.strip()
                if any(word in title.lower() for word in ['form', 'recruitment', 'notification', '2026', 'vacancy']):
                    link = a['href']
                    if not link.startswith('http'):
                        link = "https://www.sarkariresult.com" + link
                    if len(title) > 15:
                        jobs.append({'title': title, 'link': link, 'site': site_name})
            jobs = jobs[:20]

        elif site_name == "FreeJobAlert":
            links = soup.find_all('a', href=True)
            for a in links:
                title = a.text.strip()
                if any(word in title.lower() for word in ['form', 'recruitment', '2026', 'jobs', 'vacancy']):
                    link = a['href']
                    if not link.startswith('http'):
                        link = "https://www.freejobalert.com" + link
                    if len(title) > 15:
                        jobs.append({'title': title, 'link': link, 'site': site_name})
            jobs = jobs[:20]

        elif site_name == "LinkingSky":
            headings = soup.find_all('h2', class_='entry-title')
            for h in headings:
                a = h.find('a')
                if a:
                    title = a.text.strip()
                    link = a['href']
                    if len(title) > 15:
                        jobs.append({'title': title, 'link': link, 'site': site_name})

        elif site_name == "OdishaGovtJob":
            post_titles = soup.find_all(['h3', 'h2'], class_=['post-title', 'entry-title'])
            for title_tag in post_titles:
                a = title_tag.find('a')
                if a:
                    title = a.text.strip()
                    link = a['href']
                    if len(title) > 15 and any(word in title.lower() for word in ['recruitment', 'job', 'notification', '2026', 'ossc', 'odisha']):
                        jobs.append({'title': title, 'link': link, 'site': site_name})
            jobs = jobs[:20]

        return jobs
    except Exception as e:
        logging.error(f"Scrape error for {site_name}: {e}")
        return []

def auto_scrape_and_save():
    logging.info("Starting auto scrape and save")
    saved_count = 0
    duplicates = 0

    for site in SITES:
        time.sleep(3)  # polite delay to avoid rate limit
        site_jobs = scrape_from_site(site['url'], site['name'])
        logging.info(f"Found {len(site_jobs)} jobs from {site['name']}")

        for job in site_jobs:
            title = job['title']
            link = job['link']
            site_name = job['site']
            state = get_state_from_title(title)
            collection = f'govt_jobs_{state}'

            # Duplicate check
            query = db.collection(collection).where('title', '==', title).where('link', '==', link).limit(1).get()
            if query:
                duplicates += 1
                continue

            last_date_dt = extract_last_date_from_text(title)
            if not last_date_dt:
                last_date_dt = get_last_date_from_detail_page(link)

            data = {
                'title': title,
                'link': link,
                'state': state,
                'site': site_name,
                'scraped_at': firestore.SERVER_TIMESTAMP,
            }
            if last_date_dt:
                data['lastDate'] = last_date_dt

            db.collection(collection).add(data)
            saved_count += 1
            logging.info(f"Saved: {title[:50]}...")

    logging.info(f"Completed: Saved {saved_count} new jobs, Skipped {duplicates} duplicates")

if __name__ == "__main__":
    auto_scrape_and_save()
