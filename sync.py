import os
import requests
import xml.etree.ElementTree as ET
import re
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
 
# ── 환경변수에서 키 로드 ──────────────────────────────────────────────────────
NOTION_TOKEN        = os.environ['NOTION_TOKEN']
NOTION_DB_ID        = '7ca9bcf1-1644-4558-bfaf-c44887e03af2'
NAVER_CLIENT_ID     = os.environ['NAVER_CLIENT_ID']
NAVER_CLIENT_SECRET = os.environ['NAVER_CLIENT_SECRET']
YOUTUBE_API_KEY     = os.environ['YOUTUBE_API_KEY']  # 추가: YouTube Data API 키
 
CUTOFF_HOURS = 49  # 2일치 여유로 누락 방지
 
notion_headers = {
    'Authorization': f'Bearer {NOTION_TOKEN}',
    'Content-Type': 'application/json',
    'Notion-Version': '2022-06-28'
}
 
# ── 유틸 함수 ─────────────────────────────────────────────────────────────────
def clean_html(text):
    return re.sub('<[^<]+?>', '', text or '').strip()
 
def get_cutoff():
    return datetime.now(timezone.utc) - timedelta(hours=CUTOFF_HOURS)
 
# ── YouTube Data API 수집 (RSS 대체) ──────────────────────────────────────────
def fetch_youtube_api(channel_id):
    """YouTube Data API v3로 최근 영상(라이브 포함) 수집"""
    cutoff = get_cutoff()
    # 채널 ID(UC...)를 업로드 플레이리스트 ID(UU...)로 변환
    playlist_id = 'UU' + channel_id[2:]
 
    try:
        r = requests.get(
            'https://www.googleapis.com/youtube/v3/playlistItems',
            params={
                'part': 'snippet',
                'playlistId': playlist_id,
                'maxResults': 50,
                'key': YOUTUBE_API_KEY
            },
            timeout=15
        )
        r.raise_for_status()
        data = r.json()
 
        items = []
        total = len(data.get('items', []))
 
        for item in data.get('items', []):
            snippet  = item.get('snippet', {})
            title    = snippet.get('title', '')
            video_id = snippet.get('resourceId', {}).get('videoId', '')
            link     = f'https://www.youtube.com/watch?v={video_id}'
            pub_str  = snippet.get('publishedAt', '')
 
            try:
                pub = datetime.fromisoformat(pub_str.replace('Z', '+00:00'))
                if pub.tzinfo is None:
                    pub = pub.replace(tzinfo=timezone.utc)
            except:
                pub = datetime.now(timezone.utc)
 
            print(f'    📅 {title[:30]} | {pub.strftime("%Y-%m-%d %H:%M UTC")}')
 
            if pub >= cutoff:
                items.append({
                    'title': title,
                    'link':  link,
                    'desc':  snippet.get('description', '')[:500],
                    'date':  pub.strftime('%Y-%m-%d')
                })
 
        if total > 0 and not items:
            print(f'  → API 조회 성공 (전체 {total}개) / 최근 {CUTOFF_HOURS}시간 이내 영상 없음')
        elif total == 0:
            print(f'  → 채널에 영상 없음')
 
        return items, True
 
    except requests.exceptions.HTTPError as e:
        print(f'  ❌ YouTube API HTTP 오류: {e}')
        return [], False
    except Exception as e:
        print(f'  ⚠️  YouTube API 실패: {e}')
        return [], False
 
# ── RSS/Atom 피드 수집 (네이버/티스토리/인스타용) ────────────────────────────
def fetch_rss(url, is_atom=False):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    cutoff = get_cutoff()
    try:
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        ns = {'a': 'http://www.w3.org/2005/Atom'}
        items = []
        total_entries = 0
 
        if is_atom:
            entries = root.findall('a:entry', ns)
            total_entries = len(entries)
            for entry in entries:
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
                total_entries += 1
                title   = clean_html(item.findtext('title'))
                link    = (item.findtext('link') or '').strip()
                desc    = clean_html(item.findtext('description'))[:1000]
                pub_str = item.findtext('pubDate') or ''
                try:
                    pub = parsedate_to_datetime(pub_str)
                    if pub.tzinfo is None:
                        pub = pub.replace(tzinfo=timezone.utc)
                except:
                    pub = datetime.now(timezone.utc)
                if pub >= cutoff:
                    items.append({'title': title, 'link': link, 'desc': desc, 'date': pub.strftime('%Y-%m-%d')})
 
        if total_entries > 0 and not items:
            print(f'  → 피드 조회 성공 (전체 {total_entries}개) / 최근 {CUTOFF_HOURS}시간 이내 게시물 없음')
        elif total_entries == 0:
            print(f'  → 빈 피드')
 
        return items, True
    except requests.exceptions.HTTPError as e:
        print(f'  ❌ RSS 피드 HTTP 오류 ({url}): {e}')
        return [], False
    except Exception as e:
        print(f'  ⚠️  RSS 실패 ({url}): {e}')
        return [], False
 
# ── Naver Open API 폴백 ───────────────────────────────────────────────────────
def fetch_naver_api(blog_address):
    headers = {
        'X-Naver-Client-Id':     NAVER_CLIENT_ID,
        'X-Naver-Client-Secret': NAVER_CLIENT_SECRET
    }
    today     = datetime.now().strftime('%Y%m%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    seen = {}
 
    for query in [
        '음식', '요리', '레시피', '식품', '맛집', '먹거리',
        '고구마', '죽순', '양배추', '감자', '당근', '무', '파', '마늘', '양파', '호박', '버섯', '브로콜리',
        '딸기', '사과', '배', '포도', '감귤', '수박', '참외',
        '꽃게', '해산물', '수산', '생선', '새우', '전복', '굴', '오징어', '낙지', '문어',
        '소고기', '돼지고기', '닭고기', '삼겹살', '한우', '곱창', '막창',
        '반찬', '밀키트', '간편식', '간식', '음료', '냉동', '즉석',
        '특가', '할인', '공동구매', '직구', '주말특가',
    ]:
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
                    'desc':  clean_html(item.get('description', ''))[:1000],
                    'date':  f'{pd[:4]}-{pd[4:6]}-{pd[6:]}'
                }
        except:
            continue
 
    return list(seen.values())
 
# ── 쇼핑 커넥트 글 필터 ───────────────────────────────────────────────────────
SHOPPING_CONNECT_KEYWORDS = [
    '쇼핑커넥트', '쇼핑 커넥트', 'shopping connect', 'shoppingconnect',
    '네이버쇼핑', '스마트스토어커넥트', '커넥트상품', '커넥트 상품',
    # 네이버 쇼핑커넥트 고시 문구 (글 상단 삽입 문구)
    '쇼핑커넥트 활동의 일환', '수수료를 제공받습니다', '판매 발생 시 수수료',
]
 
def is_shopping_connect(title, desc):
    text = (title + ' ' + desc).lower().replace(' ', '')
    return any(k.replace(' ', '') in text for k in SHOPPING_CONNECT_KEYWORDS)
 
# ── 브랜드 자동 분류 ──────────────────────────────────────────────────────────
def classify_brand(title, desc):
    text = (title + ' ' + desc).lower()
    if any(k in text for k in ['정금영연구소', '정금영', '곱창', '막창', '청류', '쌀과자', '쌀강정', '쌀가공', '유아간식', '아기간식', '키즈간식', '이유식']):
        return '정금영연구소'
    if any(k in text for k in ['나이스픽', 'nicepick', '그립tv', '그립 tv', '클릭메이트', '라이브방송', '라이브쇼핑']):
        return '나이스픽'
    return '푸드잇다'
 
# ── 노션 중복 확인 ────────────────────────────────────────────────────────────
def url_exists_in_notion(url):
    r = requests.post(
        f'https://api.notion.com/v1/databases/{NOTION_DB_ID}/query',
        headers=notion_headers,
        json={'filter': {'property': '업로드 링크', 'url': {'equals': url}}}
    )
    if r.status_code == 200:
        return len(r.json().get('results', [])) > 0
    return False
 
# ── 노션 페이지 생성 ──────────────────────────────────────────────────────────
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
    print(f'  ❌ 노션 등록 실패 ({r.status_code}): {r.text[:200]}')
    return False
 
# ── 계정 목록 ─────────────────────────────────────────────────────────────────
ACCOUNTS = [
    ('ys03000',        '네이버 블로그', 'N_메인_ys03000',            '푸드잇다',    '',      'naver'),
    ('jfoodtion',      '네이버 블로그', 'N_ 메인_jfoodtion',         '정금영연구소','',      'naver'),
    ('npick2025',      '네이버 블로그', 'N_메인_npick2025',           '나이스픽',   '',      'naver'),
    ('shoongni',       '네이버 블로그', 'N_부_시윤_tbd03000',         'AUTO',       '김시윤','naver'),
    ('ytty090',        '네이버 블로그', 'N_부_윤택_pond21237',        'AUTO',       '김시윤','naver'),
    ('090tyyt',        '네이버 블로그', 'N_부_윤택_sorry21237',       'AUTO',       '김시윤','naver'),
    ('chungsfam_',     '네이버 블로그', 'N_부_이사_cbg03000',         'AUTO',       '김시윤','naver'),
    ('chungsfamillly', '네이버 블로그', 'N_부_이사_sdcoop2013',       'AUTO',       '김시윤','naver'),
    ('deeep-',         '네이버 블로그', 'N_부_슬기_zzi90com',         'AUTO',       '배슬기','naver'),
    ('https://cbg03000.tistory.com/rss',  '티스토리', 'T_부_이사_cbg03000',  'AUTO', '김시윤','tistory'),
    ('https://1ovreview.tistory.com/rss', '티스토리', 'T_부_슬기_1ovreview', 'AUTO', '배슬기','tistory'),
    # 수정: RSS URL → 채널 ID만 사용 (YouTube Data API 방식으로 변경)
    ('UC0FKLLsftVKnxvXsGa8vcJQ', '유튜브', 'Y_메인_나이스픽(jgy03000)', '나이스픽', '', 'youtube'),
    ('https://rsshub.app/instagram/user/food_itda',     '인스타그램', 'I_메인_food_itda',     '푸드잇다', '', 'instagram'),
    ('https://rsshub.app/instagram/user/nicepick_2025', '인스타그램', 'I_메인_nicepick_2025', '나이스픽', '', 'instagram'),
]
 
# ── 메인 실행 ─────────────────────────────────────────────────────────────────
def main():
    created = 0
    print(f'🚀 동기화 시작: {datetime.now().strftime("%Y-%m-%d %H:%M")} (최근 {CUTOFF_HOURS}시간 확인)\n')
 
    for addr, channel, notion_id, brand, manager, acc_type in ACCOUNTS:
        print(f'📂 {notion_id}')
        posts = []
        fetch_ok = True
 
        if acc_type == 'naver':
            rss_url = f'https://rss.blog.naver.com/{addr}.xml'
            posts, fetch_ok = fetch_rss(rss_url)
            if not fetch_ok:
                print(f'  → RSS 차단, Naver API 폴백 시도')
                posts = fetch_naver_api(addr)
        elif acc_type == 'tistory':
            posts, fetch_ok = fetch_rss(addr)
        elif acc_type == 'youtube':
            # 수정: RSS 대신 YouTube Data API 사용 (라이브 방송 포함)
            posts, fetch_ok = fetch_youtube_api(addr)
            if not fetch_ok:
                print(f'  ⚠️  YouTube API 수집 실패 — YOUTUBE_API_KEY 시크릿을 확인하세요')
                print()
                continue
        elif acc_type == 'instagram':
            posts, fetch_ok = fetch_rss(addr)
 
        if not posts:
            status = '수집 실패' if not fetch_ok else f'최근 {CUTOFF_HOURS}시간 이내 업로드 없음'
            print(f'  → 새 게시물 없음 ({status})\n')
            continue
 
        for post in posts:
            if is_shopping_connect(post['title'], post['desc']):
                print(f'  🚫 쇼핑커넥트 제외: {post["title"][:40]}')
                continue
 
            actual_brand = brand if brand != 'AUTO' else classify_brand(post['title'], post['desc'])
 
            if url_exists_in_notion(post['link']):
                print(f'  ⏭️  이미 등록됨: {post["title"][:40]}')
                continue
 
            success = create_notion_page(
                title=post['title'], channel=channel, notion_id=notion_id,
                brand=actual_brand, manager=manager,
                url=post['link'], date=post['date']
            )
            if success:
                created += 1
                print(f'  ✅ 등록: {post["title"][:40]}')
        print()
 
    print(f'─' * 50)
    print(f'✅ 완료: 총 {created}개 노션 등록')
 
if __name__ == '__main__':
    main()
 
