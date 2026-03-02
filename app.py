from flask import Flask, render_template_string
import pandas as pd
import psycopg2

app = Flask(__name__)

def get_data():
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5433",
            database="eticaret_db",
            user="admin",
            password="123456"
        )
        query = """
        SELECT 
            product_id AS "Urun_ID",
            COUNT(CASE WHEN action = 'view' THEN 1 END) AS "Tiklama",
            COUNT(CASE WHEN action = 'add_to_cart' THEN 1 END) AS "Sepet",
            COUNT(CASE WHEN action = 'purchase' THEN 1 END) AS "Satis"
        FROM raw_events
        GROUP BY product_id
        ORDER BY "Satis" DESC
        LIMIT 10;
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Hata: {e}")
        return pd.DataFrame()

@app.route('/')
def index():
    df = get_data()
    
    # İstatistikler
    total_sales = int(df['Satis'].sum()) if not df.empty else 0
    top_product = df.iloc[0]['Urun_ID'] if not df.empty else "-"
    
    # HTML Tablo Satırları
    rows = ""
    for _, row in df.iterrows():
        # Satış başarısına göre renkli bar genişliği (basit bir CSS hilesi)
        bar_width = min(row['Satis'] * 10, 100) 
        rows += f"""
        <tr>
            <td><span class="badge bg-secondary">#{int(row['Urun_ID'])}</span></td>
            <td>{int(row['Tiklama'])}</td>
            <td>{int(row['Sepet'])}</td>
            <td>
                <div class="d-flex align-items-center">
                    <div class="progress flex-grow-1 me-2" style="height: 10px;">
                        <div class="progress-bar bg-success" style="width: {bar_width}%"></div>
                    </div>
                    <strong>{int(row['Satis'])}</strong>
                </div>
            </td>
        </tr>
        """

    html_template = f"""
    <!DOCTYPE html>
    <html lang="tr">
    <head>
        <meta charset="UTF-8">
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
        <title>Data Engineer Dashboard</title>
        <style>
            body {{ background-color: #f4f7f6; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }}
            .navbar {{ background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%); }}
            .stat-card {{ border: none; border-radius: 15px; transition: transform 0.3s; }}
            .stat-card:hover {{ transform: translateY(-5px); }}
            .main-table {{ border-radius: 15px; overflow: hidden; background: white; }}
            .refresh-btn {{ border-radius: 20px; padding: 10px 25px; font-weight: bold; }}
        </style>
    </head>
    <body>
        <nav class="navbar navbar-dark shadow-sm mb-4">
            <div class="container">
                <span class="navbar-brand mb-0 h1"><i class="fas fa-database me-2"></i> E-Ticaret Veri Boru Hattı Paneli</span>
            </div>
        </nav>

        <div class="container">
            <div class="row mb-4">
                <div class="col-md-4">
                    <div class="card stat-card shadow-sm p-3 bg-white text-dark">
                        <div class="d-flex align-items-center">
                            <div class="icon-box bg-primary text-white p-3 rounded-circle me-3"><i class="fas fa-shopping-cart"></i></div>
                            <div><p class="text-muted mb-0">Toplam Satış</p><h3>{total_sales}</h3></div>
                        </div>
                    </div>
                </div>
                <div class="col-md-4">
                    <div class="card stat-card shadow-sm p-3 bg-white text-dark">
                        <div class="d-flex align-items-center">
                            <div class="icon-box bg-warning text-dark p-3 rounded-circle me-3"><i class="fas fa-star"></i></div>
                            <div><p class="text-muted mb-0">En Çok Satan</p><h3>Ürün {top_product}</h3></div>
                        </div>
                    </div>
                </div>
                <div class="col-md-4 d-flex align-items-center justify-content-end">
                    <button class="btn btn-primary shadow-sm refresh-btn" onclick="window.location.reload();">
                        <i class="fas fa-sync-alt me-2"></i> Verileri Güncelle
                    </button>
                </div>
            </div>

            <div class="card shadow main-table p-4">
                <h5 class="mb-3 text-muted">Ürün Bazlı Performans Analizi</h5>
                <table class="table table-hover align-middle">
                    <thead class="table-light">
                        <tr>
                            <th>Ürün ID</th>
                            <th>Tıklama</th>
                            <th>Sepet</th>
                            <th>Satış Başarısı (Adet)</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows}
                    </tbody>
                </table>
            </div>
        </div>
    </body>
    </html>
    """
    return render_template_string(html_template)

if __name__ == '__main__':
    app.run(debug=True, port=5000)