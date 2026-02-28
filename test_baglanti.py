import psycopg2

#veritabanına bağlanma
try:
    connection = psycopg2.connect(
       host="127.0.0.1",  
        port="5433",        #Docker'da 5432 portunu 5433'e yönlendirdiğimiz için burada 5433'ü kullanıyoruz
        database="eticaret_db",
        user="admin",
        password="123456"
    )
    print("✅ Basariyla baglanildi!")
    connection.close()
    
except Exception as e:
    print("❌ Baglanti hatasi olustu detay:")
    print(repr(e)) # Hatayı ekrana zorla yazdırmak için