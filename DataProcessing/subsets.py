import sqlite3
import pandas as pd

# ---------------------------------------------------------------------
# Ayarlar
# ---------------------------------------------------------------------
DB_PATH = r"C:\Projects\NLP\ASBA\Scrapper\reviews_V2.db"  # kendi yoluna göre değiştir


RANDOM_STATE=40
# ---------------------------------------------------------------------
# Yardımcı fonksiyonlar
# ---------------------------------------------------------------------
def select_diverse_products(
    df_merged,
    cat_col="leaf_category",
    max_products_per_cat=50,
    random_state=RANDOM_STATE
):
    """
    Mümkün olduğunca farklı kategorilerden ürün seçer.
    Her kategori için en fazla max_products_per_cat adet product_id alır.
    """
    # Her ürün için kategori bilgisini al (her ürün bir kez)
    prod_cat = (
        df_merged[["product_id", cat_col]]
        .drop_duplicates(subset=["product_id"])
        .dropna(subset=[cat_col])
    )

    # Kategoriye göre grupla ve ürün seç
    selected_ids = []
    for cat, group in prod_cat.groupby(cat_col):
        # Her kategori için en fazla max_products_per_cat ürün
        n_take = min(max_products_per_cat, len(group))
        chosen = group.sample(n=n_take, random_state=random_state)["product_id"].tolist()
        selected_ids.extend(chosen)

    print(f"Toplam seçilen ürün sayısı (farklı kategorilerden): {len(selected_ids)}")
    return set(selected_ids)

def build_diverse_balanced_sample(
    df_merged,
    target_total=5000,
    cat_col="leaf_category",
    max_products_per_cat=50,
    rating_col="rating",
    random_state=RANDOM_STATE
):
    """
    1) Mümkün olduğunca farklı kategorilerden ürün seçer
    2) Bu ürünlerin yorumlarından her rating sınıfından
       yaklaşık eşit sayıda örnek alarak target_total kadar örnek üretir.
    """

    # 1) Ürün seçimi (kategori çeşitliliği için)
    selected_products = select_diverse_products(
        df_merged,
        cat_col=cat_col,
        max_products_per_cat=max_products_per_cat,
        random_state=random_state
    )

    df_pool = df_merged[df_merged["product_id"].isin(selected_products)].copy()
    print("Diverse ürün havuzundaki toplam review sayısı:", len(df_pool))

    # Eğer havuz zaten çok küçükse direkt dön
    if len(df_pool) <= target_total:
        print(f"Havuz {len(df_pool)} adet, target_total={target_total}, hepsi alınacak.")
        return df_pool.sample(frac=1.0, random_state=random_state)

    # 2) Rating-balanced örnekleme
    unique_ratings = sorted(df_pool[rating_col].dropna().unique())
    if not unique_ratings:
        print("Uyarı: rating kolonu boş, normal sample ile dönecek.")
        return df_pool.sample(n=target_total, random_state=random_state)

    per_class_target = target_total // len(unique_ratings)
    print(f"Rating sınıfları: {unique_ratings}, her sınıf için hedef ~{per_class_target}")

    # Mevcut balanced_by_rating fonksiyonunu kullanıyoruz
    df_bal = balanced_by_rating(
        df_pool,
        rating_col=rating_col,
        target_per_class=per_class_target,
        random_state=random_state
    )

    # Eğer toplam target_total'dan küçükse, kalan kısmı rastgele tamamla
    if len(df_bal) < target_total:
        remaining = target_total - len(df_bal)
        print(f"Balanced sonrası {len(df_bal)} satır var, {remaining} adet daha random eklenecek.")

        remaining_pool = df_pool.drop(df_bal.index)
        if len(remaining_pool) > 0:
            extra = remaining_pool.sample(
                n=min(remaining, len(remaining_pool)),
                random_state=random_state
            )
            df_final = pd.concat([df_bal, extra]).sample(frac=1.0, random_state=random_state)
        else:
            df_final = df_bal
    else:
        # Fazla çıktıysa, sadece target_total kadar random kırp
        df_final = df_bal.sample(n=target_total, random_state=random_state)

    print("Son diverse + balanced sample boyutu:", len(df_final))
    return df_final


def prepare_for_sql(df: pd.DataFrame) -> pd.DataFrame:
    """
    SQLite'e yazmadan önce DataFrame'i temizler:
    - cat_list gibi list kolonlarını string'e çevirir.
    """
    df_sql = df.copy()
    if "cat_list" in df_sql.columns:
        df_sql["cat_list"] = df_sql["cat_list"].apply(
            lambda x: "|".join(x) if isinstance(x, list) else x
        )
    return df_sql


def parse_categories(cat_str):
    """products.categories kolonunu list'e çevirir."""
    if cat_str is None:
        return []
    s = str(cat_str)
    # Baştaki gereksiz virgülü kaldır
    if s.startswith(","):
        s = s[1:]
    # Split & trim
    parts = [c.strip() for c in s.split(",")]
    # Boşları at
    parts = [c for c in parts if c]
    return parts


def get_or_none(lst, idx):
    """Liste'den güvenli index alma."""
    try:
        return lst[idx]
    except IndexError:
        return None


def balanced_by_rating(df, rating_col="rating", target_per_class=1000, random_state=RANDOM_STATE):
    """
    Rating'e göre dengeli subset döndürür.
    Her sınıftan en fazla target_per_class örnek alır.
    """
    parts = []
    unique_ratings = sorted(df[rating_col].dropna().unique())
    print("Balanced subset için rating sınıfları:", unique_ratings)

    for r in unique_ratings:
        grp = df[df[rating_col] == r]
        take = min(target_per_class, len(grp))
        if take == 0:
            continue
        print(f"Rating {r}: {len(grp)} satır var, {take} adet alınıyor.")
        parts.append(grp.sample(n=take, random_state=random_state))

    if not parts:
        print("Uyarı: Hiç sınıf seçilemedi, boş DataFrame dönüyor.")
        return df.iloc[0:0]

    df_bal = pd.concat(parts).sample(frac=1.0, random_state=random_state)
    print("Balanced subset toplam boyut:", len(df_bal))
    return df_bal


def subset_by_category(
    df_merged,
    main_category=None,
    leaf_category=None,
    min_reviews_per_product=0,
    max_total_reviews=None,
    random_state=RANDOM_STATE
):
    """
    reviews + products birleşmiş DataFrame'den kategori bazlı subset alır.

    Parametreler:
      - main_category: filtrelemek istediğin ana kategori adı (opsiyonel)
      - leaf_category: filtrelemek istediğin leaf kategori adı (opsiyonel)
      - min_reviews_per_product: sadece en az X review'u olan ürünler
      - max_total_reviews: toplam review sayısını sınırla (ör: 5000)
    """
    df = df_merged.copy()

    if main_category is not None:
        df = df[df["main_category"] == main_category]

    if leaf_category is not None:
        df = df[df["leaf_category"] == leaf_category]

    # Her ürünün kaç review'u olduğunu hesapla
    # Burada "id_rev" reviews tablosundaki id sütununun merge sonrası adı
    review_id_col = "id_rev"
    if review_id_col not in df.columns:
        # Eğer sütun adları farklıysa otomatik bulmaya çalış
        # id_x / id_y gibi adlar olabilir
        id_cols = [c for c in df.columns if c.startswith("id")]
        print("Uyarı: 'id_rev' kolonunu bulamadım, mevcut id kolonları:", id_cols)
        # Kullanıcı burada manuel düzeltme yapmalı
        raise KeyError("Lütfen subset_by_category içinde review_id_col ismini verisetine göre düzelt.")

    counts = df.groupby("product_id")[review_id_col].count().rename("review_cnt")
    df = df.merge(counts, on="product_id")

    if min_reviews_per_product > 0:
        df = df[df["review_cnt"] >= min_reviews_per_product]

    # Toplam review sayısını sınırla
    if (max_total_reviews is not None) and (len(df) > max_total_reviews):
        df = df.sample(n=max_total_reviews, random_state=random_state)

    return df


# ---------------------------------------------------------------------
# Ana akış
# ---------------------------------------------------------------------
def main():
    # -------------------------------------------------------------
    # 1) Veritabanına bağlan
    # -------------------------------------------------------------
    conn = sqlite3.connect(DB_PATH)
    print("DB bağlantısı açıldı:", DB_PATH)

    # -------------------------------------------------------------
    # 2) Products tablosunu oku
    # -------------------------------------------------------------
    df_products = pd.read_sql_query("""
        SELECT id, url, title, first_seen_ts, review_count, categories
        FROM products
    """, conn)

    print("Products satır sayısı:", len(df_products))
    print("Products kolonları:", list(df_products.columns))

    # categories -> cat_list, main_category, leaf_category
    df_products["cat_list"] = df_products["categories"].apply(parse_categories)
    df_products["main_category"]   = df_products["cat_list"].apply(lambda x: get_or_none(x, 0))
    df_products["second_category"] = df_products["cat_list"].apply(lambda x: get_or_none(x, 1))
    df_products["leaf_category"]   = df_products["cat_list"].apply(lambda x: get_or_none(x, -1))

    print("\nÖrnek product kategorileri:")
    print(df_products[["id", "categories", "cat_list", "main_category", "leaf_category"]].head())

    # Kategori dağılımlarına bakmak istersen:
    print("\nMain category dağılımı (ilk 20):")
    print(df_products["main_category"].value_counts().head(20))

    print("\nLeaf category dağılımı (ilk 20):")
    print(df_products["leaf_category"].value_counts().head(20))

    # -------------------------------------------------------------
    # 3) Reviews tablosunu oku
    # -------------------------------------------------------------
    # Örneğe göre sütun sırası:
    # id, product_id, review_hash, review_text, rating, page_no, first_seen_ts
    df_reviews = pd.read_sql_query("""
        SELECT
            id,
            product_id,
            review_hash,
            review_text,
            rating,
            page_no,
            collected_ts
        FROM reviews
    """, conn)

    print("\nReviews satır sayısı:", len(df_reviews))
    print("Reviews kolonları:", list(df_reviews.columns))

    print("\nRating dağılımı:")
    print(df_reviews["rating"].value_counts(dropna=False))

    # -------------------------------------------------------------
    # 4) Reviews + Products merge
    # -------------------------------------------------------------
    df_merged = df_reviews.merge(
        df_products,
        left_on="product_id",
        right_on="id",
        suffixes=("_rev", "_prod")
    )


    print("\nBirleşik (merged) satır sayısı:", len(df_merged))
    print("Merged kolonları:", list(df_merged.columns))

    print("\nMerged örnek satırlar:")
    print(df_merged[[
        "product_id", "review_text", "rating",
        "main_category", "leaf_category"
    ]].head())

    # -------------------------------------------------------------
    # 4.5) 5000 adet: farklı kategorilerden ürün + rating balanced örnek
    # -------------------------------------------------------------
    sample_5k = build_diverse_balanced_sample(
        df_merged,
        target_total=5000,
        cat_col="leaf_category",   # istersen "main_category" yapabilirsin
        max_products_per_cat=50,   # her leaf_category için max 50 ürün
        rating_col="rating",
        random_state=RANDOM_STATE
    )

    prepare_for_sql(sample_5k).to_sql(
        "subset_5k_diverse_products_balanced_ratings",
        conn,
        if_exists="replace",
        index=False
    )
    print("\nsubset_5k_diverse_products_balanced_ratings tablosu oluşturuldu.")    





    print("\nBirleşik (merged) satır sayısı:", len(df_merged))
    print("Merged kolonları:", list(df_merged.columns))

    print("\nMerged örnek satırlar:")
    print(df_merged[[
        "product_id", "review_text", "rating",
        "main_category", "leaf_category"
    ]].head())

    # İstersen debug için küçük bir subset:
    DEBUG_SIZE = 2000
    df_debug = df_merged.sample(
        n=min(DEBUG_SIZE, len(df_merged)),
        random_state=RANDOM_STATE
    )
    prepare_for_sql(df_debug).to_sql(
        "reviews_debug_merged",
        conn,
        if_exists="replace",
        index=False
    )
    print(f"\nDebug merged subset 'reviews_debug_merged' tablosuna yazıldı. Boyut: {len(df_debug)}")

    # -------------------------------------------------------------


    # -------------------------------------------------------------
    # Bağlantıyı kapat
    # -------------------------------------------------------------
    conn.close()
    print("\nDB bağlantısı kapatıldı.")


if __name__ == "__main__":
    main()
