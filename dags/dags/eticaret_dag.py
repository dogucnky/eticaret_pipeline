from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import logging

# Veritabanı bağlantı ayarları
# Docker ağında 'veritabani' ismiyle PostgreSQL'e ulaşıyoruz.
db_config = {
    "host": "veritabani",
    "port": "5432",
    "database": "eticaret_db",
    "user": "admin",
    "password": "123456"
}

# GÖREV 1: Tablo Hazırlama Fonksiyonu
def tablo_hazirla_func():
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_events (
                event_id SERIAL PRIMARY KEY,
                user_id INT,
                product_id INT,
                action VARCHAR(50),
                platform VARCHAR(50),
                event_time TIMESTAMP
            );
        """)
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("✅ Tablo kontrol edildi ve veritabanı hazır.")
    except Exception as e:
        logging.error(f"❌ Tablo hazirlanirken hata olustu: {e}")
        raise

# GÖREV 2: Analiz Raporu Fonksiyonu
def analiz_yap_func():
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
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
        
        cursor.execute(analiz_sorgusu)
        sonuclar = cursor.fetchall()
        
        logging.info("--- GÜNLÜK SATIŞ ANALİZ RAPORU ---")
        if not sonuclar:
            logging.info("Henüz analiz edilecek yeterli veri (satın alma) bulunamadı.")
        else:
            for satir in sonuclar:
                # Tüm sütunları (ID, Görüntüleme, Sepet, Satış) logluyoruz
                msg = f"Ürün ID: {satir[0]} | Görüntüleme: {satir[1]} | Sepet: {satir[2]} | Satın Alma: {satir[3]}"
                logging.info(msg)
                print(msg) # Hem print hem logging garanti olsun
        
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"❌ Analiz yapilirken hata olustu: {e}")
        raise

# 2. DAG Tanımı
default_args = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='eticaret_veri_boruhatti',
    default_args=default_args,
    description='Uçtan uca e-ticaret veri analiz pipeline',
    schedule_interval=timedelta(minutes=3), # 3 dakikada bir otomatik çalışır
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    gorev_tablo = PythonOperator(
        task_id='tablo_kontrol_etme',
        python_callable=tablo_hazirla_func
    )

    gorev_analiz = PythonOperator(
        task_id='satis_analizi_yapma',
        python_callable=analiz_yap_func
    )

    gorev_tablo >> gorev_analiz