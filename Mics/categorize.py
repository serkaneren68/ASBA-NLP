import sqlite3
from contextlib import closing
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time

# ================== AYARLAR ==================
DB_PATH = r"C:\\Projects\\NLP\\ASBA\\Scrapper\\reviews_V2.db"
TABLE_NAME = "products"
URL_COLUMN = "url"
ID_COLUMN = "id"
SELENIUM_TIMEOUT = 10


# ================== DB YARDIMCI FONKSIYONLARI ==================
def add_columns_if_missing():
    """Tabloya yeni sütunlar ekler (yoksa)."""
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.cursor()
        # Var olan kolonları kontrol et
        cur.execute(f"PRAGMA table_info({TABLE_NAME})")
        existing_cols = [row[1] for row in cur.fetchall()]

        new_columns = {
            "categories": "TEXT"
        }

        for col, dtype in new_columns.items():
            if col not in existing_cols:
                cur.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN {col} {dtype};")
                print(f"Kolon eklendi: {col}")

        conn.commit()


def fetch_urls(limit=None):
    """Ana tablodan (id, url) listesi çeker."""
    query = f"SELECT {ID_COLUMN}, {URL_COLUMN} FROM {TABLE_NAME} WHERE {URL_COLUMN} IS NOT NULL"
    if limit:
        query += f" LIMIT {limit}"
    with closing(sqlite3.connect(DB_PATH)) as conn:
        return conn.execute(query).fetchall()


def update_row(product_id, categories):
    """Scrape edilen verileri aynı satıra yazar."""
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(f"""
        UPDATE {TABLE_NAME}
        SET categories=?
        WHERE {ID_COLUMN}=?
        """, (categories, product_id))
        conn.commit()


# ================== SELENIUM ==================
def create_driver(headless=True):
    driver = webdriver.Edge()
    return driver


def scrape_page(driver, url):
    """Selenium ile sayfadan verileri çeker."""
    try:
        driver.get(url)
        WebDriverWait(driver, SELENIUM_TIMEOUT).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

        # --- Burayı kendi sayfana göre değiştir ---
        try:
            categories = driver.find_elements(By.XPATH, "//a[starts-with(@class,'IFt9fjR3dfhAnos3ylNg')]")
        except:
            categories = None


        return ",".join([i.text for i in categories])

    except TimeoutException:
        print(f"[TIMEOUT] {url}")
    except Exception as e:
        print(f"[ERROR] {url} -> {e}")

    return None


# ================== ANA AKIŞ ==================
def main():
    add_columns_if_missing()
    rows = fetch_urls(limit=1000)
    print(f"{len(rows)} link bulundu.")

    driver = create_driver(headless=True)
    try:
        for i, (pid, url) in enumerate(rows, 1):
            print(f"[{i}/{len(rows)}] {url}")
            categories = scrape_page(driver, url)
            update_row(pid, categories)
            time.sleep(0.5)
    finally:
        driver.quit()


if __name__ == "__main__":
    main()
