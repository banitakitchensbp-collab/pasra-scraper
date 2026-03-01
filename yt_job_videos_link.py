# yt_job_videos_link.py
import requests
from datetime import datetime, timedelta, timezone
import firebase_admin
from firebase_admin import credentials, firestore
import os
import logging
import hashlib  # Optional duplicate ke liye extra layer

# ================== LOGGING SETUP ==================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),          # Console/GitHub Actions mein dikhega
        logging.FileHandler("scrape_log.txt")  # File mein bhi save hoga
    ]
)
logger = logging.getLogger(__name__)

# ================== CONFIG ==================
# Environment variables se secure tareeke se load (GitHub Actions ke liye best)
API_KEY = os.getenv('YOUTUBE_API_KEY')

# Firebase ke liye GOOGLE_APPLICATION_CREDENTIALS env var se direct kaam karega
# Local test ke liye fallback path (Actions mein mat use karna)
FIREBASE_KEY_PATH = os.getenv('FIREBASE_KEY_PATH', r'C:\xampp\htdocs\pasra\py\pasra-firebase.json')

COLLECTION_NAME = 'govt_job_videos'

# Sirf high-quality govt job channels (UPSC heavy news wale remove kiye)
CHANNEL_IDS = [
    'UCThcPY3lO1htOqtcHaCwU8g',   # Govt Jobs Adda247
    'UCAyYBPzFioHUxvVZEn4rMJA',   # SSC Adda247
    'UCx-7YPrGnNC81ahyqvqu27g',   # Careerwill SSC
    'UCEHZeAjdkSE3vHzQ0ntdKRA',   # Careerwill ONE
    # Agar aur chahiye to add kar sakte ho (jaise Unacademy SSC playlist wala)
]

# Positive keywords – inme se ek bhi match hona chahiye
GOVT_KEYWORDS = [
    'ssc', 'cgl', 'chsl', 'gd', 'mts', 'railway', 'rrb', 'ntpc', 'group d', 'alp', 'bank', 'ibps', 'sbi',
    'po', 'clerk', 'vacancy', 'vacancies', 'notification', 'exam date', 'syllabus', 'pyq', 'practice set',
    'mock test', 'reasoning', 'english', 'maths', 'gs', 'current affairs for ssc', 'govt job', 'sarkari naukri',
    'recruitment', 'apply online', 'last date', 'form fill', 'eligibility', 'age limit'
]

# Negative keywords – agar yeh match kare to skip (news, politics, biography avoid)
NEGATIVE_KEYWORDS = [
    'iran', 'israel', 'war', 'khamenei', 'supreme leader', 'death', 'protests', 'hitler', 'nazi', 'gdp',
    'india vs', 'geopolitics', 'the hindu', 'indian express', 'analysis', 'places in news', 'biography',
    'untold story', 'nba', 'lebron', 'luka', 'basketball', 'sports', 'cricket'
]

# ================== FIREBASE INIT ==================
try:
    if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
        # GitHub Actions mein env var se direct initialize
        firebase_admin.initialize_app()
    else:
        # Local test ke liye file path
        cred = credentials.Certificate(FIREBASE_KEY_PATH)
        firebase_admin.initialize_app(cred)
    
    db = firestore.client()
    logger.info("Firebase connected successfully!")
except Exception as e:
    logger.error(f"Firebase initialization failed: {e}")
    raise

# ================== FUNCTIONS ==================
def get_uploads_playlist(channel_id):
    url = f"https://www.googleapis.com/youtube/v3/channels?part=contentDetails&id={channel_id}&key={API_KEY}"
    try:
        resp = requests.get(url).json()
        if 'items' in resp and resp['items']:
            return resp['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        logger.warning(f"No uploads playlist for channel: {channel_id}")
        return None
    except Exception as e:
        logger.error(f"Error fetching playlist for {channel_id}: {e}")
        return None

def is_strictly_job_related(title, description):
    text = (title + " " + description).lower()
    
    has_positive = any(kw in text for kw in GOVT_KEYWORDS)
    has_negative = any(kw in text for kw in NEGATIVE_KEYWORDS)
    
    return has_positive and not has_negative

def fetch_and_save_latest_videos():
    yesterday_utc = datetime.now(timezone.utc) - timedelta(days=1)
    yesterday_str = yesterday_utc.isoformat() + 'Z'

    total_saved = 0

    for ch_id in CHANNEL_IDS:
        playlist_id = get_uploads_playlist(ch_id)
        if not playlist_id:
            continue

        url = (
            f"https://www.googleapis.com/youtube/v3/playlistItems"
            f"?part=snippet"
            f"&playlistId={playlist_id}"
            f"&maxResults=15"
            f"&key={API_KEY}"
        )
        try:
            resp = requests.get(url).json()
            if 'items' not in resp:
                logger.warning(f"No items returned for channel {ch_id}")
                continue

            for item in resp['items']:
                pub_date = item['snippet']['publishedAt']
                if pub_date < yesterday_str:
                    continue

                title = item['snippet']['title']
                desc = item['snippet']['description']

                if not is_strictly_job_related(title, desc):
                    logger.info(f"Skipped (not strict govt job related): {title}")
                    continue

                video_id = item['snippet']['resourceId']['videoId']
                doc_ref = db.collection(COLLECTION_NAME).document(video_id)

                if doc_ref.get().exists:
                    logger.info(f"Skipped duplicate: {title}")
                    continue

                video_data = {
                    'title': title,
                    'link': f"https://www.youtube.com/watch?v={video_id}",
                    'channel': item['snippet']['channelTitle'],
                    'channelId': ch_id,
                    'thumbnail': item['snippet']['thumbnails'].get('medium', {}).get('url', ''),
                    'publishedAt': pub_date,
                    'scrapedAt': firestore.SERVER_TIMESTAMP,
                    'description': desc[:400],
                    'source': 'youtube',
                    'videoId': video_id
                }

                doc_ref.set(video_data)
                total_saved += 1
                logger.info(f"SAVED REAL GOVT JOB VIDEO: {title}")
                logger.info(f"   Channel: {video_data['channel']}")
                logger.info(f"   Link: {video_data['link']}")
                logger.info("---")

        except Exception as e:
            logger.error(f"Error processing channel {ch_id}: {e}")

    logger.info(f"\n=== TOTAL STRICT GOVT JOB VIDEOS SAVED TODAY: {total_saved} ===\n")

# ================== RUN ==================
if __name__ == "__main__":
    if not API_KEY:
        logger.error("YOUTUBE_API_KEY environment variable not set!")
        exit(1)
    
    fetch_and_save_latest_videos()
