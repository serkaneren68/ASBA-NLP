from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from urllib.parse import urlsplit, urlunsplit
import time
import re


####Database
import sqlite3, hashlib, time
from contextlib import closing

def init_db(db_path="reviews.db"):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY,
        url TEXT UNIQUE,
        title TEXT,
        first_seen_ts INTEGER
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS reviews (
        id INTEGER PRIMARY KEY,
        product_id INTEGER NOT NULL,
        review_hash TEXT NOT NULL,
        review_text TEXT NOT NULL,
        rating INTEGER,                -- ★ yeni
        page_no INTEGER,
        collected_ts INTEGER,
        FOREIGN KEY(product_id) REFERENCES products(id),
        UNIQUE(product_id, review_hash)
    );
    """)
    # Eski tabloda rating yoksa ekle (failsafe)
    try:
        conn.execute("ALTER TABLE reviews ADD COLUMN rating INTEGER;")
    except sqlite3.OperationalError:
        pass
    conn.execute("CREATE INDEX IF NOT EXISTS idx_reviews_product ON reviews(product_id);")
    return conn


def get_or_create_product_id(conn, product_url, title=None):
    cur = conn.cursor()
    cur.execute("SELECT id FROM products WHERE url = ?", (product_url,))
    row = cur.fetchone()
    if row: 
        return row[0]
    cur.execute(
        "INSERT OR IGNORE INTO products(url, title, first_seen_ts) VALUES(?,?,?)",
        (product_url, title, int(time.time()))
    )
    conn.commit()
    return get_or_create_product_id(conn, product_url, title)

def hash_review(text:str) -> str:
    return hashlib.sha256((text or "").strip().encode("utf-8")).hexdigest()

def save_reviews(conn, product_id:int, items:list, page_no:int):
    """
    items: [{'text': str, 'rating': int|None}, ...]
    """
    rows = []
    now = int(time.time())
    for it in items:
        t = (it.get('text') or '').strip()
        if not t:
            continue
        r = it.get('rating')
        rows.append((product_id, hash_review(t), t, r, page_no, now))
    if rows:
        with conn:
            conn.executemany(
                "INSERT OR IGNORE INTO reviews(product_id, review_hash, review_text, rating, page_no, collected_ts) "
                "VALUES(?,?,?,?,?,?)",
                rows
            )

from urllib.parse import urlsplit, urlunsplit, parse_qs, urlencode

def build_category_page_url(base_url: str, page_no: int) -> str:
    parts = urlsplit(base_url)
    q = parse_qs(parts.query)
    q["siralama"]="coksatan"
    q["sayfa"] = [str(page_no)]
    new_query = urlencode(q, doseq=True)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))



def category_page_has_products(driver, timeout=8) -> bool:
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, "article[class^='productCard-module']")
            )
        )
        return True
    except TimeoutException:
        return False


def wait_review_cards(driver, wait):
    return wait.until(
        EC.presence_of_all_elements_located(
            # Kart class'ı build'e göre değişebilir: prefix ile seç
            (By.XPATH, "//div[starts-with(@class,'hermes-ReviewCard-module-dY_oaYMIo0DJcUiSeaVW')]")
        )
    )

def wait_star_rating_cards(driver, wait):
    return wait.until(
        EC.presence_of_all_elements_located(
            # Kart class'ı build'e göre değişebilir: prefix ile seç
            (By.XPATH, "//div[starts-with(@class,'hermes-RatingPointer-module-UefD0t2XvgGWsKdLkNoX')]")
        )
    ) 

def extract_rating_from_card(card):
    try:
        stars = card.find_elements(
            By.XPATH,
            ".//div[starts-with(@class,'hermes-RatingPointer-module-')]"
            "//div[starts-with(@class,'star')]"
        )
        if not stars:
            return None

        filled = 0
        for s in stars:
            txt = (s.text or "").strip()
            cls = (s.get_attribute("class") or "").lower()
            style = (s.get_attribute("style") or "").lower()

            # 2) Sık görülen class ipuçları, "star is working now"
            if any(k in cls for k in ("full", "filled", "active", "selected", "star")):
                filled += 1
                continue

        # Güvenli sınırla
        filled = max(0, min(filled, 5))
        return filled if filled > 0 else None
    except Exception:
        return None


def extract_text_from_card(driver, card):
    # Önce <span> normalize edilmiş, yoksa p/div/span fallback
    for xp in [
        # ".//span[normalize-space()]",
        # ".//*[self::p or self::div or self::span][normalize-space()]",
        ".//div[starts-with(@class,'hermes-ReviewCard-module-KaU17BbDowCWcTZ9zzxw')]"
    ]:
        try:
            node = card.find_element(By.XPATH, xp)
            node_comment = node.find_element(By.XPATH, ".//span[normalize-space()]")
            text = driver.execute_script("return arguments[0].textContent;", node_comment)
            text = (text or "").strip()
            if text:
                return text
        except Exception:
            pass
    return None

def separate_by_review_stars(base_html_link):
    return [base_html_link + "?sayfa=1&filtre=" + str(index) for index in range(1,6)]

def scrape_comments_in_current_page(driver, wait):
    items = []
    cards = wait_review_cards(driver, wait)

    # son karta kaydır, lazy render varsa tetiklensin
    driver.execute_script("arguments[0].scrollIntoView({block:'end'});", cards[-1])
    time.sleep(0.2)

    for card in cards:
        text = extract_text_from_card(driver, card)
        rating = extract_rating_from_card(card)
        if text:
            items.append({"text": text, "rating": rating})
    return items



# def scrape_comments_in_current_page(driver, wait):
#     comments = []
#     cards = wait_review_cards(driver, wait)

#     ratings = wait_star_rating_cards(driver, wait)
#     driver.execute_script("arguments[0].scrollIntoView({block:'end'});", cards[-1])
    
#     time.sleep(0.2)
#     for rating in ratings:
#         how_many_stars = len(rating.find_elements(By.XPATH, ".//div[starts-with(@class,'star')]"))
        
#         print(how_many_stars)  

#     for card in cards:
#         try:
#             span = card.find_element(By.XPATH, ".//span[normalize-space()]")
#             text = driver.execute_script("return arguments[0].textContent;", span)
#             text = (text or "").strip()
#             if text:
#                 comments.append(text)
#                 continue
#         except Exception:
#             pass
#         try:
#             node = card.find_element(By.XPATH, ".//*[self::p or self::div or self::span][normalize-space()]")
#             text = driver.execute_script("return arguments[0].textContent;", node)
#             text = (text or "").strip()
#             if text:
#                 comments.append(text)
#         except Exception:
#             continue
#     return comments

def get_total_review_pages(driver):
    try:
        spans = driver.find_elements(
            By.XPATH,
            "//div[contains(@class,'paginationBarHolder')]"
            "//ul[contains(@class,'hermes-PaginationBar-module-')]"
            "//li[contains(@class,'hermes-PageHolder-module-')]//span[normalize-space()]"
        )
        nums = []
        for s in spans:
            t = (s.text or "").strip()
            if t.isdigit():
                nums.append(int(t))
        return max(nums) if nums else 1
    except Exception:
        return 1

def click_review_page(driver, wait, page_no):
    try:
        try:
            first_card = driver.find_element(By.XPATH, "(//div[starts-with(@class,'hermes-ReviewCard-module-')])[1]")
        except NoSuchElementException:
            first_card = None

        span = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((
                By.XPATH,
                "//div[contains(@class,'paginationBarHolder')]"
                "//ul[contains(@class,'hermes-PaginationBar-module-')]"
                f"//li[contains(@class,'hermes-PageHolder-module-')]//span[normalize-space()='{page_no}']"
            ))
        )
        li = span.find_element(By.XPATH, "./ancestor::li[1]")
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", li)
        time.sleep(0.1)
        try:
            li.click()
        except Exception:
            driver.execute_script("arguments[0].click();", li)

        if first_card:
            WebDriverWait(driver, 15).until(EC.staleness_of(first_card))
        wait_review_cards(driver, wait)
        return True
    except TimeoutException:
        return False

def scrape_all_reviews_of_product(driver, wait, product_url, limit_review_per_product_star, product_reviews_url, conn):
    # Ürünü (varsa başlıkla) kaydet/al
    product_id = get_or_create_product_id(conn, product_url, title=None)

    driver.get(product_reviews_url)
    time.sleep(0.5)

    total_pages = get_total_review_pages(driver)
    all_count = 0

    for p in range(1, total_pages + 1):
        if p > 1:
            moved = click_review_page(driver, wait, p)
            if not moved:
                break
            time.sleep(0.3)

        page_items = scrape_comments_in_current_page(driver, wait)
        if page_items:
            save_reviews(conn, product_id, page_items, page_no=p)
            all_count += len(page_items)
        if all_count > limit_review_per_product_star:
            break
    return all_count

# --- Kategori tarafı ---

def build_reviews_url_from_product_url(url: str) -> str:
    """
    Ürün URL'sinden güvenli şekilde '...-yorumlari' üretir.
    Sorgu parametrelerini atar, fragmanı korur.
    """
    parts = urlsplit(url)
    path = parts.path

    # bazı ürün linkleri zaten -yorumlari içerir
    if path.endswith("-yorumlari"):
        new_path = path
    else:
        # trailing slash varsa kaldır
        if path.endswith("/"):
            path = path[:-1]
        new_path = path + "-yorumlari"

    return urlunsplit((parts.scheme, parts.netloc, new_path, "", parts.fragment))

def get_product_urls_from_category_page(driver, wait, limit_per_page=None):
    """
    Kategori sayfasındaki ürün kartlarından ürün sayfası linklerini döndürür.
    Dinamik modül class'ları için prefix içerir.
    """
    # Ürün kartları yüklenene kadar bekle
    wait.until(
        EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, "article[class^='productCard-module']")
        )
    )

    # Linkler tipik olarak kartın içindeki <a> üzerinde
    anchors = driver.find_elements(
        By.CSS_SELECTOR,
        "article[class^='productCard-module'] a[class*='productCardLink-module']"
    )

    urls = []
    seen = set()
    for a in anchors:
        href = a.get_attribute("href") or ""
        # Reklam / event tracker / boş href'leri at
        if not href.startswith("http"):
            continue
        # Bazı linkler dış trackere (adservice) gidiyor olabilir; ürün detay sayfası olanları al
        if ("hepsiburada.com" not in href) or ("adservice" in href):
            continue
        # Kategori içinde aynı ürün birden fazla kere çıkabilir
        if href in seen:
            continue
        seen.add(href)
        urls.append(href)
        if limit_per_page and len(urls) >= limit_per_page:
            break
    return urls

def scrape_category_via_query(driver, base_category_url: str,
                              start_page=1, max_pages=None,
                              limit_products_per_page=None,limit_review_per_product_star=None,  sleep_between=0.4):
    """
    URL'deki ?sayfa=N parametresiyle kategori sayfalarını gezer.
    max_pages=None ise ürün kalmayana kadar devam eder.
    """
    wait = WebDriverWait(driver, 15)
    results = {}
    visited_products = set()

    page = start_page
    while True:
        if max_pages is not None and page > max_pages:
            break

        page_url = build_category_page_url(base_category_url, page)
        print(f"[Kategori] sayfa={page} → {page_url}")
        driver.get(page_url)

        # Bu sayfada ürün kartı var mı?
        if not category_page_has_products(driver):
            print("[Kategori] Ürün bulunamadı, durduruluyor.")
            break

        product_urls = get_product_urls_from_category_page(driver, wait, limit_per_page=limit_products_per_page)

        # sayfada hiç ürün linki çıkmadıysa bitir
        if not product_urls:
            print("[Kategori] Link bulunamadı, durduruluyor.")
            break

        for pu in product_urls:
            if pu in visited_products:
                continue
            visited_products.add(pu)

            reviews_url = build_reviews_url_from_product_url(pu)
            reviews_urls_by_star = separate_by_review_stars(reviews_url)
            
            for star_class in reviews_urls_by_star:
                try:
                    n = scrape_all_reviews_of_product(driver, wait,
                                                    product_url=pu,
                                                    limit_review_per_product_star = limit_review_per_product_star,
                                                    product_reviews_url=star_class,
                                                    conn=conn)   # ← SQLite bağlantın
                    results[pu] = n
                    print(f"[OK] {pu} → {n} yorum kaydedildi")
                except Exception as e:
                    print(f"[HATA] {pu}: {e}")
                time.sleep(sleep_between)
        page += 1
    return results

# def scrape_category(driver, start_category_url, max_category_pages=1, limit_products_per_page=None, sleep_between=0.4):
#     """
#     Kategori sayfasından ürün linkleri topla, her ürünün yorumlarını çek.
#     Dönen yapı: {product_url: [yorum1, yorum2, ...], ...}
#     """
#     wait = WebDriverWait(driver, 15)
#     driver.get(start_category_url)
#     time.sleep(0.8)

#     results = {}
#     visited_products = set()

#     for page_idx in range(1, max_category_pages + 1):
#         print(f"[Kategori] Sayfa {page_idx} işleniyor...")

#         # Bu sayfadaki ürün linkleri
#         product_urls = get_product_urls_from_category_page(driver, wait, limit_per_page=limit_products_per_page)

#         for pu in product_urls:
#             if pu in visited_products:
#                 continue
#             visited_products.add(pu)

#             reviews_url = build_reviews_url_from_product_url(pu)
#             print(f"  → Ürün: {pu}")
#             print(f"    Yorumlar: {reviews_url}")

#             try:
#                 comments = scrape_all_reviews_of_product(driver, wait, reviews_url)
#                 results[pu] = comments
#                 print(f"    Toplanan yorum: {len(comments)}")
#             except Exception as e:
#                 print(f"    Hata: {e}")
#             time.sleep(sleep_between)

#         # Sonraki kategori sayfasına geç
#         if page_idx == max_category_pages:
#             break
#         next_btn = find_next_category_page(driver)
#         if not next_btn:
#             print("[Kategori] Daha fazla sayfa yok.")
#             break
#         driver.execute_script("arguments[0].scrollIntoView({block:'center'});", next_btn)
#         time.sleep(0.2)
#         try:
#             next_btn.click()
#         except Exception:
#             driver.execute_script("arguments[0].click();", next_btn)
#         # ürün kartlarının yenilenmesini bekle
#         WebDriverWait(driver, 15).until(
#             EC.presence_of_all_elements_located(
#                 (By.CSS_SELECTOR, "article[class^='productCard-module']")
#             )
#         )
#         time.sleep(0.4)

#     return results


driver = webdriver.Edge()
conn = init_db("reviews_V2.db")  # önceki SQLite fonksiyonların

#category_url = "https://www.hepsiburada.com/bilgisayar-sistemleri-ve-ekipmanlari-c-2147483646"

category_urls= [#"https://www.hepsiburada.com/yapi-market-hirdavatlar-c-2147483620",
                #"https://www.hepsiburada.com/giyim-ayakkabi-c-2147483636", #Moda
                #"https://www.hepsiburada.com/spor-fitness-urunleri-c-2147483635", #Spor Ürünleri
                #"https://www.hepsiburada.com/kozmetik-c-2147483603",#Kozmetik
                #"https://www.hepsiburada.com/supermarket-c-2147483619", #SüperMarket
                #"https://www.hepsiburada.com/kitaplar-c-2147483645", #Kitap
                # "https://www.hepsiburada.com/mutfak-gerecleri-c-22500" #Mutfak
                # "https://www.hepsiburada.com/elektrikli-ev-aletleri-c-17071", #Elektrikli ev aletleri
                # "https://www.hepsiburada.com/mobilyalar-c-18021299", #Mobilya
                # "https://www.hepsiburada.com/oto-aksesuarlari-c-2147483631", #Oto aksesuarları
                "https://www.hepsiburada.com/anne-bebek-oyuncak-c-2147483639" #Anne Bebek
                ]

for category_url in category_urls:
    
    summary = scrape_category_via_query(
        driver,
        base_category_url=category_url,
        start_page=1,
        max_pages=3,                # istersen None bırak, ürün bitene kadar gider
        limit_products_per_page=20,  # her sayfadan ilk 5 ürün
        limit_review_per_product_star = 100,
        sleep_between=0.3
    )

conn.close()
driver.quit()