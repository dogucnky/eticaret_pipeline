import psycopg2
import random
import time
from datetime import datetime

# Veritabanı bağlantı ayarlarımız (Çalışan 5433 portuyla)
db_config = {
    "host": "127.0.0.1",
    "port": "5433",
    "database": "eticaret_db",
    "user": "admin",
    "password": "123456"
}

def generate_event():
    """Rastgele bir e-ticaret müşteri hareketi üretir."""
    actions = ['view', 'view', 'view', 'view', 'add_to_cart', 'add_to_cart', 'purchase']
    
    return {
        "user_id": random.randint(1000, 5000),     # Müşteri ID'si
        "product_id": random.randint(1, 100),      # Ürün ID'si
        "action": random.choice(actions),          # Yapılan işlem
        "platform": random.choice(['mobile', 'desktop', 'tablet']), # Cihaz
        "event_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # O anki saat
    }

def main():
    print("🚀 Veri üreticisi başlatılıyor... (Durdurmak için terminalde CTRL+C tuşlarına basın)")
    
    try:
        # 1. Veritabanına bağlan
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # 2. SQL Ekleme Sorgusu (event_id'yi PostgreSQL SERIAL olduğu için kendi atayacak)
        insert_query = """
        INSERT INTO raw_events (user_id, product_id, action, platform, event_time)
        VALUES (%s, %s, %s, %s, %s);
        """
        
        sayac = 0
        # 3. Sonsuz Döngü (Veri Akışı Simülasyonu)
        while True:
            event = generate_event()
            
            # Veriyi gönder
            cursor.execute(insert_query, (
                event['user_id'], 
                event['product_id'], 
                event['action'], 
                event['platform'], 
                event['event_time']
            ))
            
            conn.commit() # Veriyi kalıcı olarak kaydet
            sayac += 1
            
            print(f"[{sayac}] Olay Kaydedildi: Müşteri {event['user_id']} -> {event['platform']} üzerinden {event['product_id']} numaralı ürünü '{event['action']}' yaptı.")
            
            # Gerçekçi olması için işlemleri 1 ile 3 saniye arasında rastgele beklet
            time.sleep(random.uniform(1, 3))
            
    except KeyboardInterrupt:
        print("\n🛑 Veri üretimi sizin tarafınızdan durduruldu.")
    except Exception as e:
        print(f"❌ Beklenmeyen bir hata oluştu: {e}")
    finally:
        # Kod durduğunda veya hata verdiğinde veritabanı kapılarını temizce kapat
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        print("🔌 Veritabanı bağlantısı güvenli bir şekilde kapatıldı.")

if __name__ == "__main__":
    main()