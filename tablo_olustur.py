import psycopg2

baglanti_bilgileri = {
    "host": "127.0.0.1",
    "port": "5433",  # Başarılı olan portumuz
    "database": "eticaret_db",
    "user": "admin",
    "password": "123456"
}

create_table_query = """
CREATE TABLE IF NOT EXISTS raw_events (
    event_id SERIAL PRIMARY KEY,
    USER_ID INT,
    product_id INT,
    action VARCHAR(50),
    platform VARCHAR(50),
    event_time TIMESTAMP
);
"""

try:
    # 1. Veritabanına bağlan ve bir 'imleç' (cursor) oluştur
    conn = psycopg2.connect(**baglanti_bilgileri)
    cursor = conn.cursor()

    cursor.execute(create_table_query)
    
    # 3. Değişiklikleri kalıcı hale getir
    conn.commit()
    
    print("✅ 'raw_events' tablosu veritabanında başarıyla oluşturuldu!")
 
    cursor.close()
    conn.close()

except Exception as e:
    print("❌ Tablo oluşturulurken hata yaşandı:")
    print(repr(e))