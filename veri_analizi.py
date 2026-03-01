import psycopg2

db_config = {
    "host": "127.0.0.1",
    "port": "5433",
    "database": "eticaret_db",
    "user": "admin",
    "password": "123456"
}

analiz_sorgusu = """
SELECT 
    product_id,
    COUNT(CASE WHEN action = 'view' THEN 1 END) AS goruntuleme,
    COUNT(CASE WHEN action = 'add_to_cart' THEN 1 END) AS sepete_ekleme,
    COUNT(CASE WHEN action = 'purchase' THEN 1 END) AS satin_alma
FROM raw_events
GROUP BY product_id
HAVING COUNT(CASE WHEN action = 'purchase' THEN 1 END) > 0
ORDER BY satin_alma DESC
LIMIT 10;
"""

def analizi_calistir():
    try:

        #**db_config kullanımı, sözlükteki ayarları tek tek yazmak yerine "unpacking" yöntemiyle aktarır
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        print("\n📊 EN ÇOK SATAN İLK 10 ÜRÜNÜN DÖNÜŞÜM HUNİSİ (FUNNEL) ANALİZİ")
        print("-" * 65)
        print(f"{'Ürün ID':<10} | {'Görüntüleme':<15} | {'Sepete Ekleme':<15} | {'Satın Alma':<10}")
        print("-" * 65)
        
        # Sorguyu çalıştır ve verileri çek
        cursor.execute(analiz_sorgusu)
        sonuclar = cursor.fetchall()
        
        # Sonuçları ekrana düzgün bir tablo formatında yazdır
        for satir in sonuclar:
            urun_id, goruntuleme, sepet, satin_alma = satir
            print(f"{urun_id:<10} | {goruntuleme:<15} | {sepet:<15} | {satin_alma:<10}")
            
        print("-" * 65)
        print("💡 İpucu: Arka planda data_generator.py çalıştıkça bu rakamlar artacaktır!\n")
            
        cursor.close()
        conn.close()
    except Exception as e:
        print("❌ Analiz sırasında hata oluştu:", e)

if __name__ == "__main__":
    analizi_calistir()