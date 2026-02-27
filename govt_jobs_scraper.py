import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, firestore
import schedule
import time

# Firebase setup
cred = credentials.Certificate('pasra-firebase.json')
firebase_admin.initialize_app(cred)
db = firestore.client()

# States keys with underscore (no space in collection names)
STATES = {
    'odisha': ['odisha', 'orissa', 'bhubaneswar', 'cuttack', 'balasore', 'rourkela', 'bbsr', 'puri'],
    'bihar': ['bihar', 'patna'],
    'uttar_pradesh': ['uttar pradesh', 'up', 'uttarpradesh', 'lucknow', 'kanpur'],
    'maharashtra': ['maharashtra', 'mumbai', 'pune'],
    'delhi': ['delhi', 'new delhi'],
    'all': []
}

def get_state_from_title(title):
    title_lower = title.lower()
    for state_key, keywords in STATES.items():
        if any(kw in title_lower for kw in keywords):
            return state_key
    return 'all'

def scrape_from_site(url, site_name, parser_func):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        print(f"Trying {site_name} ({url})...")
        response = requests.get(url, headers=headers, timeout=15)
        print(f"Status: {response.status_code}")
        if response.status_code != 200:
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        jobs = parser_func(soup)
        print(f"Found {len(jobs)} jobs from {site_name}")
        return jobs
    except Exception as e:
        print(f"Error on {site_name}: {str(e)}")
        return []

# Parsers for different sites
def parse_indgovtjobs(soup):
    heading = soup.find(lambda tag: tag.name in ['h2', 'h3'] and 'Latest Government Jobs' in tag.get_text(strip=True))
    if heading:
        ul = heading.find_next('ul')
        if ul:
            items = ul.find_all('li')
            jobs = []
            for li in items:
                a = li.find('a')
                if a:
                    title = a.get_text(strip=True)
                    link = a['href']
                    if not link.startswith('http'):
                        link = "https://www.indgovtjobs.in" + link
                    jobs.append({'title': title, 'link': link})
            return jobs
    return []

def parse_sarkariresult(soup):
    links = soup.find_all('a', href=True)
    jobs = []
    for a in links:
        text = a.get_text(strip=True)
        if any(word in text for word in ['Form', 'Recruitment', 'Notification', '2026', 'Vacancy']):
            link = a['href']
            if not link.startswith('http'):
                link = "https://www.sarkariresult.com" + link
            jobs.append({'title': text, 'link': link})
    return jobs[:20]

def parse_freejobalert(soup):
    links = soup.find_all('a', href=True)
    jobs = []
    for a in links:
        text = a.get_text(strip=True)
        if any(word in text for word in ['Form', 'Recruitment', '2026', 'Jobs', 'Vacancy']):
            link = a['href']
            if not link.startswith('http'):
                link = "https://www.freejobalert.com" + link
            jobs.append({'title': text, 'link': link})
    return jobs[:20]

SITES = [
    {"url": "https://www.indgovtjobs.in/", "name": "indgovtjobs", "parser": parse_indgovtjobs},
    {"url": "https://www.sarkariresult.com/", "name": "sarkariresult", "parser": parse_sarkariresult},
    {"url": "https://www.freejobalert.com/", "name": "freejobalert", "parser": parse_freejobalert},
]

def scrape_govt_jobs():
    total_found = 0
    saved = 0
    duplicates = 0
    all_jobs = []

    print("=== Multi-Site Govt Jobs Scrape Started ===")
    
    for site in SITES:
        jobs = scrape_from_site(site["url"], site["name"], site["parser"])
        all_jobs.extend(jobs)
        total_found += len(jobs)
        if len(jobs) >= 10:  # Agar ek site se achhe jobs mile toh break
            break

    seen_titles = set()
    
    for job in all_jobs[:50]:  # Limit to 50
        title = job['title'].strip()
        if not title or title in seen_titles or len(title) < 15:
            continue
        seen_titles.add(title)
        
        link = job['link']
        
        state = get_state_from_title(title)
        
        job_data = {
            'title': title,
            'link': link,
            'state': state,
            'source': 'multi',
            'scraped_at': firestore.SERVER_TIMESTAMP
        }
        
        collection = f'govt_jobs_{state}'
        
        # Duplicate check
        query = db.collection(collection).where('title', '==', title).limit(1).get()
        if query:
            duplicates += 1
            print(f"Duplicate skipped: {title} in {collection}")
        else:
            db.collection(collection).add(job_data)
            saved += 1
            print(f"SAVED: {title} | State: {state} | Collection: {collection}")

    print("\n=== Final Summary ===")
    print(f"Total jobs found across sites: {total_found}")
    print(f"New jobs saved to Firebase: {saved}")
    print(f"Duplicates skipped: {duplicates}")
    print("=============================\n")

# Schedule daily at 8 AM IST
schedule.every().day.at("08:00").do(scrape_govt_jobs)

print("Automation running... Daily at 8:00 AM.")
print("Running manual test scrape now...")
scrape_govt_jobs()

while True:
    schedule.run_pending()
    time.sleep(60)