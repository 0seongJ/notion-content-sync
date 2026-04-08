import os
import requests
import xml.etree.ElementTree as ET
import re
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

NOTION_TOKEN        = os.environ['NOTION_TOKEN']
NOTION_DB_ID        = '42fb5ee8-7ba8-42cf-8013-633869bb823d'
NAVER_CLIENT_ID     = os.environ['NAVER_CLIENT_ID']
NAVER_CLIENT_SECRET = os.environ['NAVER_CLIENT_SECRET']

CUTOFF_HOURS = 25

notion_headers = {
    'Authorization': f'Bearer {NOTION_TOKEN}',
    'Content-Type': 'application/json',
    'Notion-Version': '2022-06-28'
}

def clean_html(text):
    return re.sub('<[^<]+?>', '', text or '').strip()

def get_cutoff():
    return datetime.now(timezone.utc) - timedelta(hours=CUTOFF_HOURS)

def fetch_rss(url, is_atom=False):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    cutoff = get_cutoff()
    try:
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        ns = {'a': 'http://www.w3.org/2005/Atom'}
        items = []
        if is_atom:
            for entry in root.findall('a:entry', ns):
                title   = clean_html(entry.findtext('a:title', '', ns))
                link_el = entry.find('a:link', ns)
                link    = link_el.get('href', '') if link_el is not None else ''
                pub_str = entry.findtext('a:published', '', ns) or ''
                try:
                    pub = datetime.fromisoformat(pub_str.replace('Z', '+00:00'))
                    if pub.tzinfo is None:
                        pub = pub.replace(tzinfo=timezone.utc)
                except:
                    pub = datetime.now(timezone.utc)
                if pub >= cutoff:
                    items.append({'title': title, 'link': link, 'desc': '', 'date': pub.strftime('%Y-%m-%d')})
        else:
            for item in root.findall('.//item'):
                title   = clean_html(item.findtext('title'))
                link    = (item.findtext('link') or '').strip()
                desc    = clean_html(item.findtext('description'))[:500]
                pub_str = item.findtext('pubDate') or ''
                try:
                    pub = parsedate_to_datetime(pub_str)
                    if pub.tzinfo is None:
                        pub = pub.replace(tzinfo=timezone.utc)
                except:
                    pub = datetime.now(timezone.utc)
                if pub >= cutoff:
                    items.append({'title': title, 'link': link, 'desc': desc, 'date': pub.strftime('%Y-%m-%d')})
        return items, True
    except Exception as e:
        print(f'  RSS 실패 ({url}): {e}')
        return [], False

def fetch_naver_api(blog_address):
    headers = {
        'X-Naver-Client-Id':     NAVER_CLIENT_ID,
        'X-Naver-Client-Secret': NAVER_CLIENT_SECRET
    }
    today     = datetime.now().strftime('%Y%m%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    seen = {}
    for query in ['음식', '요리', '레시피', '식품', '맛집']:
        try:
            r = requests.get(
                'https://openapi.naver.com/v1/search/blog.json',
                headers=headers,
                params={'query': query, 'display': 100, 'sort': 'date'},
                timeout=10
            )
            if r.status_code != 200:
                continue
            norm_addr = blog_address.lower().replace('-', '').replace('_', '')
            for item in r.json().get('items', []):
                bl = item.get('bloggerlink', '').lower().replace('-', '').replace('_', '')
                if norm_addr not in bl:
                    continue
                pd = item.get('postdate', '')
                if pd not in [today, yesterday]:
                    continue
                link = item.get('link', '')
                if link in seen:
                    continue
                seen[link] = {
                    'title': clean_html(item.get('title', '')),
                    'link':  link,
                    'desc':  clean_html(item.get('description', ''))[:500],
                    'date':  f'{pd[:4]}-{pd[4:6]}-{pd[6:]}'
                }
        except:
            continue
    return list(seen.values())

def classify_brand(title, desc):
    text = (title + ' ' + desc).lower()
    if any(k in text for k in ['곱창','막창','냉동','청류','쌀과자','쌀강정','쌀가공','밀키트','간편식','유아','아기','키즈','정금영연구소']):
        return '정금영연구소'
    if any(k in text for k in ['나이스픽','nicepick','라이브','특가','그립','클릭메이트','라이브커머스']):
        return '나이스픽'
    if any(k in text for k in ['푸드잇다','fooditda','반찬','찌개']):
        return '푸드잇다'
    return ''

def url_exists_in_notion(url):
    r = requests.post(
        f'https://api.notion.com/v1/databases/{NOTION_DB_ID}/query',
        headers=notion_headers,
        json={'filter': {'property': '업로드 링크', 'url': {'equals': url}}}
    )
    if r.status_code == 200:
        return len(r.json().get('results', [])) > 0
    return False

def create_notion_page(title, channel, notion_id, brand, manager, url, date):
    props = {
        '주제':        {'title': [{'text': {'content': title}}]},
        '채널':        {'select': {'name': channel}},
        '아이디':      {'select': {'name': notion_id}},
        '업로드 링크': {'url': url},
        '상태':        {'select': {'name': '업로드 완료'}},
    }
    if date:
        props['실제 업로드일'] = {'date': {'start': date}}
    if brand:
        props['브랜드'] = {'select': {'name': brand}}
    if manager:
        props['담당자'] = {'select': {'name': manager}}
    r = requests.post(
        'https://api.notion.com/v1/pages',
        headers=notion_headers,
        json={'parent': {'database_id': NOTION_DB_ID}, 'properties': props}
    )
    if r.status_code == 200:
        return True
    print(f'  노션 등록 실패 ({r.status_code}): {r.text[:200]}')
    return False

ACCOUNTS = [
    ('ys03000',        '네이버 블로그', 'N_메인_ys03000',           '푸드잇다',    '',       'naver'),
    ('jfoodtion',      '네이버 블로그', 'N_ 메인_jfoodtion',        '정금영연구소','',       'naver'),
    ('npick2025',      '네이버 블로그', 'N_메인_npick2025',          '나이스픽',   '',       'naver'),
    ('shoongni',       '네이버 블로그', 'N_부_시윤_tbd03000',        'AUTO',       '김시윤', 'naver'),
    ('ytty090',        '네이버 블로그', 'N_부_윤택_pond21237',       'AUTO',       '김시윤', 'naver'),
    ('090tyyt',        '네이버 블로그', 'N_부_윤택_sorry21237',      'AUTO',       '김시윤', 'naver'),
    ('chungsfam_',     '네이버 블로그', 'N_부_이사_cdg03000',        'AUTO',       '김시윤', 'naver'),
    ('chungsfamillly', '네이버 블로그', 'N_부_이사_sdcoop2013',      'AUTO',       '김시윤', 'naver'),
    ('deeep-',         '네이버 블로그', 'N_부_슬기_zzi90com',        'AUTO',       '배슬기', 'naver'),
    ('https://cbg03000.tistory.com/rss',  '티스토리', 'T_부_이사_cbg03000',  'AUTO', '김시윤', 'tistory'),
    ('https://1ovreview.tistory.com/rss', '티스토리', 'T_부_슬기_1ovreview', 'AUTO', '배슬기', 'tistory'),
    ('https://www.youtube.com/feeds/videos.xml?channel_id=UC0FKLLsftVKnxvXsGa8vcJQ',
                       '유튜브',        'Y_메인_나이스픽(jgy03000)', '나이스픽',   '',       'youtube'),
]

def main():
    created = 0
    print(f'동기화 시작: {datetime.now().strftime("%Y-%m-%d %H:%M")} (최근 {CUTOFF_HOURS}시간)')
    for addr, channel, notion_id, brand, manager, acc_type in ACCOUNTS:
        print(f'\n{notion_id}')
        posts = []
        if acc_type == 'naver':
            posts, ok = fetch_rss(f'https://rss.blog.naver.com/{addr}.xml')
            if not ok:
                posts = fetch_naver_api(addr)
        elif acc_type == 'tistory':
            posts, _ = fetch_rss(addr)
        elif acc_type == 'youtube':
            posts, _ = fetch_rss(addr, is_atom=True)
        if not posts:
            print('  새 게시물 없음')
            continue
        for post in posts:
            actual_brand = brand if brand != 'AUTO' else classify_brand(post['title'], post['desc'])
            if url_exists_in_notion(post['link']):
                print(f'  이미 등록됨: {post["title"][:40]}')
                continue
            if create_notion_page(post['title'], channel, notion_id, actual_brand, manager, post['link'], post['date']):
                created += 1
                print(f'  등록: {post["title"][:40]}')
    print(f'\n완료: 총 {created}개 노션 등록')

if __name__ == '__main__':
    main()
