from flask import Flask, request, render_template_string
import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, firestore
import time
import re
from datetime import datetime

app = Flask(__name__)

# Firebase setup
cred = credentials.Certificate('pasra-firebase.json')
firebase_admin.initialize_app(cred)
db = firestore.client()

# States with keywords
STATES = {
    'odisha': ['odisha', 'orissa', 'bhubaneswar', 'cuttack', 'balasore', 'rourkela', 'bbsr'],
    'bihar': ['bihar', 'patna'],
    'uttar_pradesh': ['uttar pradesh', 'up', 'lucknow', 'kanpur'],
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

# ======================
# DATE EXTRACTION HELPERS
# ======================

def parse_date_str(date_str):
    """Try to parse date string in common Indian formats"""
    formats = [
        '%d.%m.%Y', '%d-%m-%Y', '%d/%m/%Y',
        '%d.%m.%y', '%d-%m-%y', '%d/%m/%y',
        '%d %b %Y', '%d %B %Y', '%d %b, %Y', '%d %B, %Y'
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            if dt.year < 2000:  # Assume 20xx for two-digit years
                dt = dt.replace(year=dt.year + 2000)
            return dt
        except ValueError:
            continue
    return None

def extract_last_date_from_text(text):
    """Regex to find last date in text (title or page content)"""
    patterns = [
        r'(?:Last Date|Closing Date|Application Last Date|Last Date for Apply|Deadline|Last Date to Apply)[\s:.-]*(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})',
        r'(?:Last Date|Closing Date)[\s:.-]*(\d{1,2}\s+[A-Za-z]+\s+\d{4})',
        r'(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})'  # fallback any date-like
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
    """Scrape detail page for last date (fallback)"""
    if not link or 'http' not in link:
        return None
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(link, headers=headers, timeout=12)
        if response.status_code != 200:
            return None
        soup = BeautifulSoup(response.text, 'html.parser')

        full_text = soup.get_text(separator=' ', strip=True)
        dt = extract_last_date_from_text(full_text)
        if dt:
            return dt

        important_section = soup.find(string=re.compile(r'(important dates|dates|important links)', re.I))
        if important_section:
            parent = important_section.find_parent(['div', 'table', 'p', 'section'])
            if parent:
                section_text = parent.get_text(separator=' ', strip=True)
                dt = extract_last_date_from_text(section_text)
                if dt:
                    return dt
        return None
    except Exception as e:
        print(f"Detail page error for {link}: {e}")
        return None

# ======================
# PARSER FUNCTIONS
# ======================

def parse_indgovtjobs(soup, site_name):
    jobs = []
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
    return jobs

def parse_sarkariresult(soup, site_name):
    jobs = []
    links = soup.find_all('a', href=True)
    for a in links:
        title = a.text.strip()
        if any(word in title.lower() for word in ['form', 'recruitment', 'notification', '2026', 'vacancy']):
            link = a['href']
            if not link.startswith('http'):
                link = "https://www.sarkariresult.com" + link
            if len(title) > 15:
                jobs.append({'title': title, 'link': link, 'site': site_name})
    return jobs[:20]

def parse_freejobalert(soup, site_name):
    jobs = []
    links = soup.find_all('a', href=True)
    for a in links:
        title = a.text.strip()
        if any(word in title.lower() for word in ['form', 'recruitment', '2026', 'jobs', 'vacancy']):
            link = a['href']
            if not link.startswith('http'):
                link = "https://www.freejobalert.com" + link
            if len(title) > 15:
                jobs.append({'title': title, 'link': link, 'site': site_name})
    return jobs[:20]

def parse_linkingsky(soup, site_name):
    jobs = []
    headings = soup.find_all('h2', class_='entry-title')
    for h in headings:
        a = h.find('a')
        if a:
            title = a.text.strip()
            link = a['href']
            if len(title) > 15:
                jobs.append({'title': title, 'link': link, 'site': site_name})
    return jobs

def parse_odishagovtjob(soup, site_name):
    jobs = []
    post_titles = soup.find_all(['h3', 'h2'], class_=['post-title', 'entry-title'])
    for title_tag in post_titles:
        a = title_tag.find('a')
        if a:
            title = a.text.strip()
            link = a['href']
            if len(title) > 15 and any(word in title.lower() for word in ['recruitment', 'job', 'notification', '2026', 'ossc', 'odisha']):
                jobs.append({'title': title, 'link': link, 'site': site_name})
    return jobs[:20]

SITES = [
    {"url": "https://www.indgovtjobs.in/", "name": "IndGovtJobs", "parser": parse_indgovtjobs},
    {"url": "https://www.sarkariresult.com/", "name": "SarkariResult", "parser": parse_sarkariresult},
    {"url": "https://www.freejobalert.com/", "name": "FreeJobAlert", "parser": parse_freejobalert},
    {"url": "https://linkingsky.com/", "name": "LinkingSky", "parser": parse_linkingsky},
    {"url": "https://odishagovtjob.in/", "name": "OdishaGovtJob", "parser": parse_odishagovtjob},
]

def scrape_from_site(url, site_name, parser_func):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        return parser_func(soup, site_name)
    except Exception as e:
        print(f"Scrape error for {site_name}: {e}")
        return []

# ======================
# Flask Route
# ======================
@app.route('/', methods=['GET', 'POST'])
def index():
    jobs = []
    message = ""
    saved_count = 0
    duplicates = 0

    if request.method == 'POST':
        action = request.form.get('action')

        if action == 'find_jobs':
            for site in SITES:
                time.sleep(3)  # Polite delay
                site_jobs = scrape_from_site(site['url'], site['name'], site['parser'])
                jobs.extend(site_jobs)
            message = f"Found {len(jobs)} jobs from multiple sites!"

        elif action == 'save_jobs':
            for site in SITES:
                time.sleep(3)
                site_jobs = scrape_from_site(site['url'], site['name'], site['parser'])
                for job in site_jobs:
                    title = job['title']
                    link = job['link']
                    site_name = job['site']
                    state = get_state_from_title(title)
                    collection = f'govt_jobs_{state}'

                    # Duplicate check (positional arguments warning ignore kar sakte ho ya FieldFilter use kar sakte ho future mein)
                    query = db.collection(collection).where('title', '==', title).where('link', '==', link).limit(1).stream()
                    if any(True for _ in query):
                        duplicates += 1
                        continue

                    # Extract last date
                    last_date_dt = extract_last_date_from_text(title)
                    if not last_date_dt:
                        print(f"Trying detail page for: {title}")
                        last_date_dt = get_last_date_from_detail_page(link)

                    data = {
                        'title': title,
                        'link': link,
                        'state': state,
                        'site': site_name,
                        'scraped_at': firestore.SERVER_TIMESTAMP,
                    }
                    if last_date_dt:
                        data['lastDate'] = last_date_dt  # Direct datetime – Firestore auto Timestamp banayega
                        print(f"Saved lastDate for '{title}': {last_date_dt.strftime('%d-%m-%Y')}")

                    db.collection(collection).add(data)
                    saved_count += 1

            message = f"Saved {saved_count} new jobs! Skipped {duplicates} duplicates."

    return render_template_string('''
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>PASRA Govt Jobs Manual Scraper</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 20px; }
    h1 { color: #333; }
    button { padding: 12px 24px; font-size: 18px; margin: 10px 0; cursor: pointer; }
    table { border-collapse: collapse; width: 100%; margin-top: 20px; }
    th, td { border: 1px solid #ddd; padding: 10px; text-align: left; }
    th { background-color: #f2f2f2; }
    .message { color: green; font-weight: bold; margin: 15px 0; }
  </style>
</head>
<body>
  <h1>PASRA Govt Jobs Scraper (Fixed - Last Date Support)</h1>
  <p>Click "Find Today Jobs" → "Save to Firebase". Now saves 'lastDate' correctly.</p>

  <form method="post">
    <button type="submit" name="action" value="find_jobs">Find Today Jobs</button>
    <button type="submit" name="action" value="save_jobs">Save Jobs to Firebase</button>
  </form>

  {% if message %}
    <p class="message">{{ message }}</p>
  {% endif %}

  {% if jobs %}
    <h2>Found Jobs ({{ jobs|length }})</h2>
    <table>
      <tr>
        <th>Title</th>
        <th>Link</th>
        <th>Source Site</th>
      </tr>
      {% for job in jobs %}
      <tr>
        <td>{{ job.title }}</td>
        <td><a href="{{ job.link }}" target="_blank">{{ job.link[:80] }}...</a></td>
        <td>{{ job.site }}</td>
      </tr>
      {% endfor %}
    </table>
  {% endif %}
</body>
</html>
    ''', jobs=jobs, message=message)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)