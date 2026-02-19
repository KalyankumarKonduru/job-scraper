import os
import requests
from bs4 import BeautifulSoup
import urllib.parse
import pandas as pd
import re
import time
import math
import xlsxwriter
from datetime import datetime
from openpyxl import load_workbook
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import undetected_chromedriver as uc
from dotenv import load_dotenv

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(SCRIPT_DIR, '.env'))

DATA_DIR = os.path.join(SCRIPT_DIR, 'data')
EXCEL_FILE = os.path.join(DATA_DIR, 'jobListings.xlsx')

EMAIL_RECIPIENT = "konduru.kalyan555@gmail.com"
EMAIL_SENDER = os.environ.get("GMAIL_ADDRESS", "konduru.kalyan555@gmail.com")
EMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")

SCHEDULE_INTERVAL_MIN = 30
DEFAULT_TIME_PERIOD = "h"
DEFAULT_NUM_RESULTS = "100"
DEFAULT_ROLES = "all"

keywordsDict = {
    'fullstack': [
        "software engineer", "software developer", "full stack", "fullstack", "full-stack",
        "web developer", "frontend engineer", "front end", "front-end",
        "react", "next.js", "nextjs", "node.js", "nodejs", "typescript",
        "javascript", "mern", "sde", "swe",
    ],

    'backend': [
        "software engineer", "software developer", "backend", "back end", "back-end",
        "api engineer", "platform engineer", "infrastructure engineer",
        "go ", "golang", "java ", "spring boot", "node.js", "nodejs",
        "graphql", "microservices", "distributed systems",
        "sde", "swe",
    ],

    'devops': [
        "devops", "site reliability", "sre", "cloud engineer", "platform engineer",
        "infrastructure", "kubernetes", "docker", "terraform", "ci/cd",
        "aws", "gcp",
    ],
}

ignoreDict = {
    'title': [
        "staff", "sr.", "sr ", "senior", "manager", "lead", "chief", "principal", "director",
        "sales", "head", "mechanical", "iv", "iii", "l4", "l5", "l6",
        "management", "consultant", "manufacturing", "law", "maintenance",
        "construction", "clearance", "structures", "helpdesk", "electrical", "propulsion",
        "solution architect", "customer", "data scientist", "machine learning",
        "embedded", "hardware", "firmware", "network engineer", "security engineer",
        "c#", ".net", "salesforce",
    ],
    'description': ["clearance", "itar", "10+ years", "8+ years", "7+ years", "6+ years", "5+ years",
                     "c#", ".net", "dotnet", "asp.net", "c sharp",
                     "rabbitmq", "akka", "activemq", "jms", "salesforce"]
}

def selectRoles(whichRoles):
    keywords = []
    if whichRoles == 'all':
        for role in keywordsDict:
            keywords.extend(keywordsDict[role])
    else:
        inputRoles = whichRoles.split(',')
        for role in inputRoles:
            role = role.strip()
            if role in keywordsDict:
                keywords.extend(keywordsDict[role])
            else:
                print(f"Unsupported role: {role}")
    return keywords

def parseTimePeriod(userInput):
    """Convert user input like '1-hour', '3d', 'week' to Google tbs format like 'h', 'h3', 'w'."""
    userInput = userInput.strip().lower()

    unitMap = {
        'hour': 'h', 'hours': 'h', 'h': 'h',
        'day': 'd', 'days': 'd', 'd': 'd',
        'week': 'w', 'weeks': 'w', 'w': 'w',
        'month': 'm', 'months': 'm', 'm': 'm',
        'year': 'y', 'years': 'y', 'y': 'y',
    }

    match = re.match(r'^(\d+)[- ]?(.+)$', userInput)
    if match:
        count = int(match.group(1))
        unit = unitMap.get(match.group(2).strip())
        if unit:
            return unit if count <= 1 else f"{unit}{count}"

    match = re.match(r'^([hdwmy])(\d*)$', userInput)
    if match:
        return match.group(1) + match.group(2)

    if userInput in unitMap:
        return unitMap[userInput]

    return userInput

def createDriver():
    """Create a Chrome browser instance using undetected-chromedriver."""
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    if os.environ.get('CI'):
        options.add_argument("--disable-extensions")
    return uc.Chrome(options=options)

JOB_PLATFORMS = [
    'lever.co', 'greenhouse.io',
    'myworkdayjobs.com', 'ashbyhq.com', 'icims.com',
    'smartrecruiters.com', 'careers.oracle.com',
]

def isJobPlatformLink(url):
    return any(p in url for p in JOB_PLATFORMS)

def doGoogleSearch(driver, query, numResults, timePeriod, start):
    """Search Google using undetected Chrome browser."""
    searchQuery = urllib.parse.quote_plus(query)
    url = f"https://www.google.com/search?q={searchQuery}&num={numResults}&start={start}&tbs=qdr:{timePeriod}&hl=en"

    driver.get(url)
    time.sleep(3)

    soup = BeautifulSoup(driver.page_source, "html.parser")

    searchResults = []
    seen = set()

    searchDiv = soup.find('div', id='search') or soup
    for anchor in searchDiv.find_all('a', href=True):
        link = anchor['href'].split('#')[0]
        if link.startswith('http') and isJobPlatformLink(link):
            cleaned = cleanURL(link)
            if cleaned not in seen:
                seen.add(cleaned)
                searchResults.append(cleaned)

    return searchResults

def cleanURL(job_url):
    if "lever.co" in job_url:
        pathParts = job_url.split('/')
        pathParts[4] = pathParts[4][:36]
        if len(pathParts) > 4:
            cleaned_path = '/'.join(pathParts[:5])
        else:
            cleaned_path = '/'.join(pathParts)
        return cleaned_path

    elif "greenhouse.io" in job_url:
        pathParts = job_url.split("/")
        if len(pathParts) > 5:
            numeric_sixth_item = ''.join([char for char in pathParts[5] if char.isnumeric()])
            return '/'.join(pathParts[:5] + [numeric_sixth_item])
        return job_url

    elif "myworkdayjobs.com" in job_url:
        return job_url.split('?')[0]

    elif "ashbyhq.com" in job_url:
        return job_url.split('?')[0]

    elif "icims.com" in job_url:
        return job_url.split('?')[0]

    elif "careers.oracle.com" in job_url:
        return job_url.split('?')[0]

    elif "smartrecruiters.com" in job_url:
        return job_url.split('?')[0]

    return job_url

def getJobInfo(url):
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return None

        soup = BeautifulSoup(response.text, 'html.parser')
        jobDetails = {
            'Company Name': 'N/A',
            'Job Title': 'N/A',
            'Location': 'N/A',
            'Job Description': 'N/A',
            'url': url
        }

        if 'greenhouse' in url:
            jobDetails = _parseGreenhouse(soup, url, jobDetails)
        elif 'lever' in url:
            jobDetails = _parseLever(soup, url, jobDetails)
        elif 'myworkdayjobs.com' in url:
            jobDetails = _parseWorkday(soup, url, jobDetails)
        elif 'ashbyhq.com' in url:
            jobDetails = _parseAshby(soup, url, jobDetails)
        elif 'icims.com' in url:
            jobDetails = _parseICIMS(soup, url, jobDetails)
        elif 'careers.oracle.com' in url:
            jobDetails = _parseOracle(soup, url, jobDetails)
        elif 'smartrecruiters.com' in url:
            jobDetails = _parseGeneric(soup, url, jobDetails)

        return jobDetails

    except requests.exceptions.RequestException as e:
        print(f"HTTP request failed: {e}")
        return None


def _parseGreenhouse(soup, url, d):
    job_title_div = soup.find('div', class_='job__title')
    job_loc_div = soup.find('div', class_='job__location')
    job_desc_div = soup.find('div', class_=lambda c: c and 'job__description' in c)

    if job_title_div:
        h1 = job_title_div.find('h1') or job_title_div
        d['Job Title'] = h1.text.strip()
        d['Location'] = job_loc_div.text.strip() if job_loc_div else 'N/A'
        page_title = soup.find('title')
        if page_title and ' at ' in page_title.text:
            d['Company Name'] = page_title.text.rsplit(' at ', 1)[-1].strip()
        elif 'job-boards.greenhouse.io/' in url:
            slug = url.split('job-boards.greenhouse.io/')[-1].split('/')[0]
            d['Company Name'] = slug.replace('-', ' ').title()
        d['Job Description'] = job_desc_div.text.strip() if job_desc_div else 'N/A'
    else:
        company_tag = soup.find('span', class_='company-name')
        d['Company Name'] = company_tag.text.strip().lstrip('at ') if company_tag else 'N/A'
        title_tag = soup.find('h1', class_='app-title')
        d['Job Title'] = title_tag.text.strip() if title_tag else 'N/A'
        loc_tag = soup.find('div', class_='location')
        d['Location'] = loc_tag.text.strip() if loc_tag else 'N/A'
        desc_tag = soup.find('div', id='content')
        d['Job Description'] = desc_tag.text.strip() if desc_tag else 'N/A'

    if d['Company Name'] == 'N/A':
        if 'job-boards.greenhouse.io/' in url:
            slug = url.split('job-boards.greenhouse.io/')[-1].split('/')[0]
            d['Company Name'] = slug.replace('-', ' ').title()
        elif 'boards.greenhouse.io/' in url:
            slug = url.split('boards.greenhouse.io/')[-1].split('/')[0]
            d['Company Name'] = slug.replace('-', ' ').title()
    return d


def _parseLever(soup, url, d):
    titleTag = soup.find('title').text if soup.find('title') else ''
    if titleTag:
        parts = titleTag.split(' - ')
        if len(parts) >= 2:
            d['Company Name'] = parts[0].strip()
            d['Job Title'] = parts[1].strip()

    location_tag = soup.find('div', class_='posting-categories')
    if location_tag:
        location_div = location_tag.find('div', class_='location')
        d['Location'] = location_div.text.strip() if location_div else 'N/A'

    desc_tag = soup.find('div', attrs={'data-qa': 'job-description'})
    d['Job Description'] = desc_tag.text.strip() if desc_tag else 'N/A'
    return d


def _parseWorkday(soup, url, d):
    """Workday is JS-rendered, so extract info from the URL path."""
    try:
        d['Company Name'] = url.split('//')[1].split('.wd')[0].replace('-', ' ').title()
        parts = url.split('/')
        job_idx = parts.index('job')
        if job_idx + 2 < len(parts):
            loc_raw = parts[job_idx + 1]
            d['Location'] = loc_raw.replace('---', ', ').replace('-', ' ').strip()
            title_slug = parts[job_idx + 2]
            title_slug = title_slug.rsplit('_', 1)[0] if '_' in title_slug else title_slug
            d['Job Title'] = title_slug.replace('--', ' - ').replace('-', ' ').strip()
    except (ValueError, IndexError):
        pass
    return d


def _parseAshby(soup, url, d):
    """Try Ashby's public API first, fall back to URL parsing."""
    try:
        parts = url.rstrip('/').split('/')
        company_slug = parts[3]
        d['Company Name'] = company_slug.replace('-', ' ').title()
        job_id = parts[4] if len(parts) > 4 else None

        if job_id:
            api_url = f"https://api.ashbyhq.com/posting-api/job-board/{company_slug}"
            resp = requests.get(api_url, timeout=8)
            if resp.status_code == 200:
                for job in resp.json().get('jobs', []):
                    if job.get('id') == job_id:
                        d['Job Title'] = job.get('title', 'N/A')
                        d['Location'] = job.get('location', 'N/A')
                        desc = job.get('descriptionPlain') or job.get('descriptionHtml', '')
                        if '<' in desc:
                            desc = BeautifulSoup(desc, 'html.parser').get_text()
                        d['Job Description'] = desc[:2000] if desc else 'N/A'
                        return d
    except Exception:
        pass
    return d


def _parseICIMS(soup, url, d):
    """Extract from iCIMS URL path and page title."""
    try:
        d['Company Name'] = url.split('//')[1].split('.icims')[0].replace('careers-', '').replace('-', ' ').title()
        parts = url.split('/')
        if len(parts) > 5:
            title_slug = urllib.parse.unquote(parts[5])
            d['Job Title'] = title_slug.replace('-', ' ').title()
    except (ValueError, IndexError):
        pass

    page_title = soup.find('title')
    if page_title and page_title.text.strip():
        d['Job Description'] = page_title.text.strip()
    return d


def _parseOracle(soup, url, d):
    """Oracle provides og:title and og:description meta tags."""
    d['Company Name'] = 'Oracle'
    og_title = soup.find('meta', property='og:title')
    if og_title and og_title.get('content'):
        d['Job Title'] = og_title['content'].strip()
    og_desc = soup.find('meta', property='og:description')
    if og_desc and og_desc.get('content'):
        d['Job Description'] = og_desc['content'].strip()
    return d


def _parseGeneric(soup, url, d):
    """Fallback: extract from page title and meta tags."""
    page_title = soup.find('title')
    if page_title and page_title.text.strip():
        d['Job Title'] = page_title.text.strip()

    og_title = soup.find('meta', property='og:title')
    if og_title and og_title.get('content'):
        d['Job Title'] = og_title['content'].strip()

    og_desc = soup.find('meta', property='og:description')
    if og_desc and og_desc.get('content'):
        d['Job Description'] = og_desc['content'].strip()

    return d

def inUSA(location):
    keywords = ['None', 'remote', 'na', 'silicon valley', 'alabama', 'al', 'kentucky', 'ky', 'ohio', 'oh', 'alaska', 'ak', 'louisiana', 'la', 'oklahoma', 'ok', 'arizona', 'az', 'maine', 'me', 'oregon', 'or', 'arkansas', 'ar', 'maryland', 'md', 'pennsylvania', 'pa', 'american samoa', 'as', 'massachusetts', 'ma', 'puerto rico', 'pr', 'california', 'ca', 'michigan', 'mi', 'rhode island', 'ri', 'colorado', 'co', 'minnesota', 'mn', 'south carolina', 'sc', 'connecticut', 'ct', 'mississippi', 'ms', 'south dakota', 'sd', 'delaware', 'de', 'missouri', 'mo', 'tennessee', 'tn', 'district of columbia', 'dc', 'montana', 'mt', 'texas', 'tx', 'florida', 'fl', 'nebraska', 'ne', 'trust territories', 'tt', 'georgia', 'ga', 'nevada', 'nv', 'utah', 'ut', 'guam', 'gu', 'new hampshire', 'nh', 'vermont', 'vt', 'hawaii', 'hi', 'new jersey', 'nj', 'virginia', 'va', 'idaho', 'id', 'new mexico', 'nm', 'virgin islands', 'vi', 'illinois', 'il', 'new york', 'ny', 'washington', 'wa', 'indiana', 'in', 'north carolina', 'nc', 'west virginia', 'wv', 'iowa', 'ia', 'north dakota', 'nd', 'wisconsin', 'wi', 'kansas', 'ks', 'northern mariana islands', 'mp', 'wyoming', 'wy', 'usa', 'united states', 'united', 'states', 'us', 'new york', 'los angeles', 'chicago', 'houston', 'phoenix', 'philadelphia', 'san antonio', 'san diego', 'dallas', 'san jose', 'austin', 'jacksonville', 'fort worth', 'columbus', 'charlotte', 'san francisco', 'indianapolis', 'seattle', 'denver', 'washington', 'boston', 'el paso', 'nashville', 'detroit', 'oklahoma city', 'portland', 'las vegas', 'memphis', 'louisville', 'baltimore', 'milwaukee', 'albuquerque', 'tucson', 'fresno', 'sacramento', 'mesa', 'kansas city', 'atlanta', 'long beach', 'omaha', 'raleigh', 'colorado springs', 'miami', 'virginia beach', 'oakland', 'minneapolis', 'tulsa', 'arlington', 'tampa', 'new orleans', 'wichita', 'cleveland', 'bakersfield', 'aurora', 'anaheim', 'honolulu', 'santa ana', 'riverside', 'corpus christi', 'lexington', 'stockton', 'henderson', 'saint paul', 'st. louis', 'cincinnati', 'pittsburgh', 'greensboro', 'anchorage', 'plano', 'lincoln', 'orlando', 'irvine', 'newark', 'toledo', 'durham', 'chula vista', 'fort wayne', 'jersey city', 'st. petersburg', 'laredo', 'madison', 'chandler', 'buffalo', 'lubbock', 'scottsdale', 'reno', 'glendale', 'gilbert', 'winston–salem', 'north las vegas', 'norfolk', 'chesapeake', 'garland', 'irving', 'hialeah', 'fremont', 'boise', 'richmond', 'baton rouge', 'spokane', 'des moines', 'tacoma', 'san bernardino', 'modesto', 'fontana', 'santa clarita', 'birmingham', 'oxnard', 'fayetteville', 'moreno valley', 'rochester', 'glendale', 'huntington beach', 'salt lake city', 'grand rapids', 'amarillo', 'yonkers', 'aurora', 'montgomery', 'akron', 'little rock', 'huntsville', 'augusta', 'port st. lucie', 'grand prairie', 'columbus', 'tallahassee', 'overland park', 'tempe', 'mckinney', 'mobile', 'cape coral', 'shreveport', 'frisco', 'knoxville', 'worcester', 'brownsville', 'vancouver', 'fort lauderdale', 'sioux falls', 'ontario', 'chattanooga', 'providence', 'newport news', 'rancho cucamonga', 'santa rosa', 'oceanside', 'salem', 'elk grove', 'garden grove', 'pembroke pines', 'peoria', 'eugene', 'corona', 'cary', 'springfield', 'fort collins', 'jackson', 'alexandria', 'hayward', 'lancaster', 'lakewood', 'clarksville', 'palmdale', 'salinas', 'springfield', 'hollywood', 'pasadena', 'sunnyvale', 'macon', 'pomona', 'escondido', 'killeen', 'naperville', 'joliet', 'bellevue', 'rockford', 'savannah', 'paterson', 'torrance', 'bridgeport', 'mcallen', 'mesquite', 'syracuse', 'midland', 'pasadena', 'murfreesboro', 'miramar', 'dayton', 'fullerton', 'olathe', 'orange', 'thornton', 'roseville', 'denton', 'waco', 'surprise', 'carrollton', 'west valley city', 'charleston', 'warren', 'hampton', 'gainesville', 'visalia', 'coral springs', 'columbia', 'cedar rapids', 'sterling heights', 'new haven', 'stamford', 'concord', 'kent', 'santa clara', 'elizabeth', 'round rock', 'thousand oaks', 'lafayette', 'athens', 'topeka', 'simi valley', 'fargo', 'norman', 'columbia', 'abilene', 'wilmington', 'hartford', 'victorville', 'pearland', 'vallejo', 'ann arbor', 'berkeley', 'allentown', 'richardson', 'odessa', 'arvada', 'cambridge', 'sugar land', 'beaumont', 'lansing', 'evansville', 'rochester', 'independence', 'fairfield', 'provo', 'clearwater', 'college station', 'west jordan', 'carlsbad', 'el monte', 'murrieta', 'temecula', 'springfield', 'palm bay', 'costa mesa', 'westminster', 'north charleston', 'miami gardens', 'manchester', 'high point', 'downey', 'clovis', 'pompano beach', 'pueblo', 'elgin', 'lowell', 'antioch', 'west palm beach', 'peoria', 'everett', 'wilmington', 'ventura', 'centennial', 'lakeland', 'gresham', 'richmond', 'billings', 'inglewood', 'broken arrow', 'sandy springs', 'jurupa valley', 'hillsboro', 'waterbury', 'santa maria', 'boulder', 'greeley', 'daly city', 'meridian', 'lewisville', 'davie', 'west covina', 'league city', 'tyler', 'norwalk', 'san mateo', 'green bay', 'wichita falls', 'sparks', 'lakewood', 'burbank', 'rialto', 'allen', 'el cajon', 'las cruces', 'renton', 'davenport', 'south bend', 'vista', 'tuscaloosa', 'clinton', 'edison', 'woodbridge', 'san angelo', 'kenosha', 'vacaville', 'south gate', 'roswell', 'new bedford', 'yuma', 'longmont', 'brockton', 'quincy', 'sandy', 'waukegan', 'gulfport', 'hesperia', 'bossier city', 'suffolk', 'rochester hills', 'bellingham', 'gary', 'arlington heights', 'livonia', 'tracy', 'edinburg', 'kirkland', 'trenton', 'medford', 'milpitas', 'mission viejo', 'blaine', 'newton', 'upland', 'chino', 'san leandro', 'reading', 'norwalk', 'lynn', 'dearborn', 'new rochelle', 'plantation', 'baldwin park', 'scranton', 'eagan', 'lynnwood', 'utica', 'redwood city', 'dothan', 'carmel', 'merced', 'brooklyn park', 'tamarac', 'burnsville', 'charleston', 'alafaya', 'tustin', 'mount vernon', 'meriden', 'baytown', 'taylorsville', 'turlock', 'apple valley', 'fountain valley', 'leesburg', 'longview', 'bristol', 'valdosta', 'champaign', 'new braunfels', 'san marcos', 'flagstaff', 'manteca', 'santa barbara', 'kennewick', 'roswell', 'harlingen', 'caldwell', 'long beach', 'dearborn', 'murray', 'bryan', 'gainesville', 'lauderhill', 'madison', 'albany', 'joplin', 'missoula', 'iowa city', 'johnson city', 'rapid city', 'sugar land', 'oshkosh', 'mountain view', 'cranston', 'bossier city', 'lawrence', 'bismarck', 'anderson', 'bristol', 'bellingham', 'gulfport', 'dothan', 'farmington', 'redding', 'bryan', 'riverton', 'folsom', 'rock hill', 'new britain', 'carmel', 'temple', 'coral gables', 'concord', 'santa monica', 'wichita falls', 'sioux city', 'hesperia', 'warwick', 'boynton beach', 'troy', 'rosemead', 'missouri city', 'jonesboro', 'perris', 'apple valley', 'hemet', 'whittier', 'carson', 'milpitas', 'midland', 'eastvale', 'upland', 'bolingbrook', 'highlands ranch', 'st. cloud', 'west allis', 'rockville', 'cape coral', 'bowie', 'dubuque', 'broomfield', 'germantown', 'west sacramento', 'north little rock', 'pinellas park', 'casper', 'lancaster', 'gilroy', 'san ramon', 'new rochelle', 'kokomo', 'southfield', 'indian trail', 'cuyahoga falls', 'alameda', 'fort smith', 'kettering', 'carlsbad', 'cedar park', 'twin falls', 'portsmouth', 'sanford', 'chino hills', 'wheaton', 'largo', 'sarasota', 'aliso viejo', 'port orange', 'oak lawn', 'chapel hill', 'redmond', 'milford', 'apopka', 'avondale', 'plainfield', 'auburn', 'doral', 'bozeman', 'jupiter', 'west haven', 'hoboken', 'hoffman estates', 'eagan', 'blaine', 'apex', 'tinley park', 'palo alto', 'orland park', "coeur d'alene", 'burleson', 'casa grande', 'pittsfield', 'decatur', 'la habra', 'dublin', 'marysville', 'north port', 'valdosta', 'twin falls', 'blacksburg', 'perris', 'caldwell', 'largo', 'bartlett', 'middletown', 'decatur', 'warwick', 'conroe', 'waterloo', 'oakland park', 'bartlesville', 'wausau', 'harrisonburg', 'farmington hills', 'la crosse', 'enid', 'pico rivera', 'newark', 'palm coast', 'wellington', 'calexico', 'lancaster', 'north miami', 'riverton', 'blacksburg', 'goodyear', 'roseville', 'homestead', 'hoffman estates', 'montebello', 'casa grande', 'morgan hill', 'milford', 'murray', 'jackson', 'blaine', 'port arthur', 'kearny', 'bullhead city', 'castle rock', 'st. cloud', 'grand island', 'rockwall', 'westfield', 'little elm', 'la puente', 'lehi', 'diamond bar', 'keller', 'harrisonburg', 'saginaw', 'sammamish', 'kendall', 'georgetown', 'owensboro', 'trenton', 'keller', 'findlay', 'lakewood', 'leander', 'rocklin', 'san clemente', 'sheboygan', 'kennewick', 'draper', 'menifee', 'cuyahoga falls', 'johnson city', 'manhattan', 'rowlett', 'san bruno', 'coon rapids', 'murray', 'revere', 'sheboygan', 'east orange', 'south jordan', 'highland', 'la quinta', 'alamogordo', 'madison', 'broomfield', 'beaumont', 'newark', 'weston', 'peabody', 'union city', 'coachella', 'palatine', 'montebello', 'taylorsville', 'twin falls', 'east lansing', 'alamogordo', 'la mesa', 'blaine', 'pittsburg', 'caldwell', 'hoboken', 'huntersville', 'south whittier', 'redlands', 'janesville', 'beverly', 'burien', 'owensboro', 'wheaton', 'redmond', 'glenview', 'leominster', 'bountiful', 'oak creek', 'florissant', 'commerce city', 'pflugerville', 'westfield', 'auburn', 'shawnee', 'san rafael', 'alamogordo', 'murray', 'brentwood', 'revere', 'pflugerville', 'aliso viejo', 'auburn', 'florissant', 'national city', 'la mesa', 'leominster', 'pico rivera', 'castle rock', 'springfield']

    if location == 'N/A':
        return True

    delimiters = [",", "/", "-", "(", ")"]
    pattern = '|'.join(map(re.escape, delimiters))

    words = re.split(pattern, location)
    for word in words:
        if word.strip().lower() in keywords:
            return True
    return False

def isRelevantRole(jobTitle, jobDescription, keywords):
    if jobTitle == 'N/A' and jobDescription == 'N/A':
        return False

    titleLower = jobTitle.lower()
    descriptionLower = jobDescription.lower()

    if any(ignored in titleLower for ignored in ignoreDict['title']):
        return False
    if any(ignored in descriptionLower for ignored in ignoreDict['description']):
        return False

    return any(keyword.lower() in titleLower for keyword in keywords) or any(keyword.lower() in descriptionLower for keyword in keywords)


def loadExistingData():
    """Load existing jobs from the Excel file. Returns (new_jobs_df, history_df)."""
    if not os.path.exists(EXCEL_FILE):
        return pd.DataFrame(), pd.DataFrame()

    try:
        existing_new = pd.read_excel(EXCEL_FILE, sheet_name='New Jobs')
    except (ValueError, KeyError):
        existing_new = pd.DataFrame()

    try:
        existing_history = pd.read_excel(EXCEL_FILE, sheet_name='Previous Jobs')
    except (ValueError, KeyError):
        existing_history = pd.DataFrame()

    return existing_new, existing_history


def saveToExcel(jobList, jobListNoDetails, jobListRejected):
    """Save jobs to a single Excel file. Old 'New Jobs' get moved to 'Previous Jobs'."""
    os.makedirs(DATA_DIR, exist_ok=True)

    existing_new, existing_history = loadExistingData()

    # Merge old "New Jobs" into "Previous Jobs"
    if not existing_new.empty:
        existing_history = pd.concat([existing_new, existing_history], ignore_index=True)
        if 'url' in existing_history.columns:
            existing_history.drop_duplicates(subset='url', keep='first', inplace=True)

    current_time = datetime.now().strftime("%Y-%m-%d %H:%M")

    df_new = pd.DataFrame(jobList + jobListNoDetails)
    if not df_new.empty:
        df_new['Found At'] = current_time

    df_rejected = pd.DataFrame(jobListRejected)

    with pd.ExcelWriter(EXCEL_FILE, engine='xlsxwriter') as writer:
        df_new.to_excel(writer, sheet_name='New Jobs', index=False)
        existing_history.to_excel(writer, sheet_name='Previous Jobs', index=False)
        df_rejected.to_excel(writer, sheet_name='Rejected Jobs', index=False)

    print(f"Data saved to {EXCEL_FILE}")
    return df_new


def getSeenUrls():
    """Get all URLs already seen from the Excel file to avoid duplicates across runs."""
    seen = set()
    if not os.path.exists(EXCEL_FILE):
        return seen

    for sheet in ['New Jobs', 'Previous Jobs']:
        try:
            df = pd.read_excel(EXCEL_FILE, sheet_name=sheet)
            if 'url' in df.columns:
                seen.update(df['url'].dropna().tolist())
        except (ValueError, KeyError):
            pass
    return seen


def sendEmail(jobList, jobListNoDetails):
    """Send email notification with new job listings."""
    if not EMAIL_APP_PASSWORD:
        print("Email not configured (set GMAIL_APP_PASSWORD env var). Skipping notification.")
        return

    total = len(jobList) + len(jobListNoDetails)
    if total == 0:
        print("No new jobs to notify about.")
        return

    current_time = datetime.now().strftime("%Y-%m-%d %H:%M")

    rows = ""
    for job in jobList:
        rows += f"""<tr>
            <td style="padding:8px;border:1px solid #ddd">{job.get('Company Name','N/A')}</td>
            <td style="padding:8px;border:1px solid #ddd">{job.get('Job Title','N/A')}</td>
            <td style="padding:8px;border:1px solid #ddd">{job.get('Location','N/A')}</td>
            <td style="padding:8px;border:1px solid #ddd"><a href="{job.get('url','')}">Apply</a></td>
        </tr>"""

    for job in jobListNoDetails:
        rows += f"""<tr>
            <td style="padding:8px;border:1px solid #ddd">{job.get('Company Name','N/A')}</td>
            <td style="padding:8px;border:1px solid #ddd">{job.get('Job Title','N/A')}</td>
            <td style="padding:8px;border:1px solid #ddd">{job.get('Location','N/A')}</td>
            <td style="padding:8px;border:1px solid #ddd"><a href="{job.get('url','')}">Apply</a></td>
        </tr>"""

    html = f"""
    <h2>Job Scraper Alert - {total} New Jobs Found</h2>
    <p>Scan time: {current_time}</p>
    <table style="border-collapse:collapse;width:100%">
        <tr style="background:#4472C4;color:white">
            <th style="padding:8px;border:1px solid #ddd">Company</th>
            <th style="padding:8px;border:1px solid #ddd">Title</th>
            <th style="padding:8px;border:1px solid #ddd">Location</th>
            <th style="padding:8px;border:1px solid #ddd">Link</th>
        </tr>
        {rows}
    </table>
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Job Alert: {total} new jobs found - {current_time}"
    msg["From"] = EMAIL_SENDER
    msg["To"] = EMAIL_RECIPIENT
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(EMAIL_SENDER, EMAIL_APP_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECIPIENT, msg.as_string())
        print(f"Email sent to {EMAIL_RECIPIENT}")
    except Exception as e:
        print(f"Failed to send email: {e}")


def scrapeJobsOnce(numResults, timePeriod, whichRoles):
    """Run a single scrape cycle."""
    query = '(intitle:"software engineer" OR intitle:"software developer" OR intitle:"backend" OR intitle:"full stack" OR intitle:"frontend" OR intitle:"platform engineer") (site:lever.co OR site:greenhouse.io OR site:myworkdayjobs.com OR site:ashbyhq.com OR site:icims.com OR site:smartrecruiters.com OR site:careers.oracle.com) -intitle:staff -intitle:senior -intitle:manager -intitle:lead -intitle:principal -intitle:director -intitle:intern'

    keywords = selectRoles(whichRoles)
    resultsPerPage = 100
    urls = []

    print(f"\n{'='*60}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting scrape (period: qdr:{timePeriod})...")
    print(f"{'='*60}")

    print("Starting browser...")
    driver = createDriver()
    print("Fetching results...")

    try:
        if numResults == "max":
            i = 0
            while True:
                results = doGoogleSearch(driver, query, resultsPerPage, timePeriod, i * resultsPerPage)
                urls.extend(results)
                print(f"  Page {i+1}: {len(results)} results")
                if len(results) < resultsPerPage:
                    break
                i += 1
                time.sleep(2)
        else:
            remaining = int(numResults)
            for start in range(math.ceil(int(numResults) / resultsPerPage)):
                resultsToReturn = min(remaining, resultsPerPage)
                results = doGoogleSearch(driver, query, resultsToReturn, timePeriod, start * resultsPerPage)
                urls.extend(results)
                print(f"  Page {start+1}: {len(results)} results")
                remaining -= resultsToReturn
                time.sleep(2)
    finally:
        driver.quit()

    print(f"{len(urls)} total results fetched.")

    urls = list(set(urls))
    print("Duplicates removed, total unique results fetched:", len(urls))

    # Filter out URLs we've already seen in previous runs
    seenUrls = getSeenUrls()
    newUrls = [u for u in urls if u not in seenUrls]
    print(f"Already seen: {len(urls) - len(newUrls)}, truly new: {len(newUrls)}")

    jobList = []
    jobListNoDetails = []
    jobListRejected = []

    if newUrls:
        print("Retrieving job information...")
        for url in newUrls:
            jobInfo = getJobInfo(url)
            if jobInfo:
                if inUSA(jobInfo['Location']) and isRelevantRole(jobInfo['Job Title'], jobInfo['Job Description'], keywords):
                    if jobInfo['Job Title'] != 'N/A':
                        jobList.append(jobInfo)
                    else:
                        jobListNoDetails.append(jobInfo)
                else:
                    jobListRejected.append(jobInfo)

    jobList = sorted(jobList, key=lambda x: x['Company Name'])
    jobListNoDetails = sorted(jobListNoDetails, key=lambda x: x['Company Name'])
    jobListRejected = sorted(jobListRejected, key=lambda x: x['Company Name'])

    print(f"{len(jobList)} relevant jobs found!")
    print(f"{len(jobListNoDetails)} relevant (maybe) jobs found!")

    saveToExcel(jobList, jobListNoDetails, jobListRejected)
    sendEmail(jobList, jobListNoDetails)

    return jobList, jobListNoDetails, jobListRejected


def scrapeJobsMain():
    import sys

    interactive = "--auto" not in sys.argv

    if interactive:
        numResults = input("How many results to fetch ('max', or an integer > 0): ").strip()
        if not numResults:
            numResults = DEFAULT_NUM_RESULTS
            print(f"Set to default: {numResults}")

        timePeriodInput = input("How recent should the results be (e.g. h, d, w, m, y, or 3-hours, 2-days): ").strip()
        if not timePeriodInput:
            timePeriod = DEFAULT_TIME_PERIOD
            print(f"Set to default: {timePeriod}")
        else:
            timePeriod = parseTimePeriod(timePeriodInput)
            print(f"Time filter: qdr:{timePeriod}")

        whichRoles = input("What roles are you interested in (fullstack, backend, devops, all): ").strip()
        if not whichRoles:
            whichRoles = DEFAULT_ROLES
            print(f"Set to default: {whichRoles}")

        scrapeJobsOnce(numResults, timePeriod, whichRoles)

    else:
        is_ci = os.environ.get('CI')

        if is_ci:
            print("Running single scrape (CI mode)...")
            scrapeJobsOnce(DEFAULT_NUM_RESULTS, DEFAULT_TIME_PERIOD, DEFAULT_ROLES)
        else:
            print(f"Running in automated mode every {SCHEDULE_INTERVAL_MIN} minutes.")
            print(f"Email notifications: {'ON' if EMAIL_APP_PASSWORD else 'OFF (set GMAIL_APP_PASSWORD)'}")
            print("Press Ctrl+C to stop.\n")

            while True:
                try:
                    scrapeJobsOnce(DEFAULT_NUM_RESULTS, DEFAULT_TIME_PERIOD, DEFAULT_ROLES)
                except Exception as e:
                    print(f"Error during scrape: {e}")

                print(f"\nNext run in {SCHEDULE_INTERVAL_MIN} minutes (sleeping until then)...")
                time.sleep(SCHEDULE_INTERVAL_MIN * 60)


if __name__ == '__main__':
    scrapeJobsMain()
