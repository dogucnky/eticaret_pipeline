from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2


# Veritabanı bağlantı ayarları
# ÖNEMLİ: Docker içindeki Airflow, diğer konteynere 'veritabani' ismiyle ulaşır.
db_config = {
    "host": "veritabani", # Docker servis adı
    "port": "5432",        # Konteyner içi port
    "database": "eticaret_db",
    "user": "admin",
    "password": "123456"
}

# GÖREV 1: Tablo Hazırlama Fonksiyonu
def tablo_hazirla_func():
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
    print("✅ Tablo kontrol edildi ve hazır.")

# GÖREV 2: Analiz Raporu Fonksiyonu
def analiz_yap_func():
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
    
    print("\n--- GÜNLÜK SATIŞ ANALİZ RAPORU ---")
    for satir in sonuclar:
        print(f"Ürün ID: {satir[0]} | Görüntüleme: {satir[1]} | Sepete Ekleme: {satir[2]} | Satış: {satir[3]}")
    
    cursor.close()
    conn.close()

# 2. DAG Tanımı (Planlama)
default_args = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='eticaret_veri_boruhatti',
    default_args=default_args,
    description='Tabloyu hazırlar ve analiz raporu sunar',
    schedule_interval=timedelta(minutes=3),
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    # 3. Görevlerin Oluşturulması
    gorev_tablo = PythonOperator(
        task_id='tablo_kontrol_etme',
        python_callable=tablo_hazirla_func
    )

    gorev_analiz = PythonOperator(
        task_id='satis_analizi_yapma',
        python_callable=analiz_yap_func
    )

    # 4. Akış Sırası (Önce tablo, sonra analiz)
    gorev_tablo >> gorev_analiz