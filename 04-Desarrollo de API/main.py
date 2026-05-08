from fastapi import FastAPI
import psycopg2

import os

app = FastAPI()

## Conexion a base
# lo hacemos de forma segura con variables de entorno que se crean en un archivo .env que esta en el .gitignore

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

## Endpoints

# Endpoint: test

@app.get("/")
def root():
    return {"status": "ok", "service": "recommendation-api"}

@app.get("/test-db")
def test_db():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT 1")
    return {"db": cur.fetchone()}

# Endpoint: recommendations
@app.get('/recommendations/{adv}/{modelo}')
def recommendations(adv: str, modelo: str):

    conn = get_connection()
    cur = conn.cursor()

    ## Validamos Modelo

    if modelo == "ctr":
        table = "top_ctr"
        order_col = "ctr"

    elif modelo == "product":
        table = "top_product"
        order_col = "views"

    else:
        return {"error": "Modelo inválido (usar 'ctr' o 'product')"}

    ## Validamos Advertiser

    query = f"""
    SELECT 1
    FROM {table}
    WHERE advertiser_id = %s
    AND date = CURRENT_DATE
    LIMIT 1
    """
    cur.execute(query, (adv,))

    if cur.fetchone() is None:
        return {"error": "advertiser_id no esta activo el dia de hoy"}

    query = f"""
        SELECT advertiser_id, product_id, {order_col}
        FROM {table}
        WHERE advertiser_id = %s
        AND date = CURRENT_DATE
        ORDER BY {order_col} DESC
        LIMIT 20
    """

    cur.execute(query, (adv,))
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return {
        "advertiser_id": adv,
        "model": modelo,
        "recommendations": [
            {"product_id": r[1], "score": r[2]}
            for r in rows
        ]
    }

# Endpoint: stats
@app.get("/stats/")
def stats():
    conn = get_connection()
    cur = conn.cursor()

    # Cantidad de advertisers
    cur.execute("""
        SELECT COUNT(DISTINCT advertiser_id)
        FROM top_ctr
    """)
    total_active_advs = cur.fetchone()[0]

    # Cantidad de productos
    cur.execute("""
        SELECT COUNT(DISTINCT product_id)
        FROM top_ctr
    """)
    total_products = cur.fetchone()[0]

    # Cantidad de productos sin clicks
    cur.execute("""
        SELECT COUNT(DISTINCT product_id)
        FROM top_ctr
        WHERE clicks = 0        
    """)
    total_products_no_clicks = cur.fetchone()[0]

    # Coincidencia entre modelos (ctr vs views)
    cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT advertiser_id, product_id
            FROM top_ctr
            INTERSECT
            SELECT advertiser_id, product_id
            FROM top_product
        ) t
    """)

    overlap = cur.fetchone()[0]

    cur.close()
    conn.close()

    return {
        "total_active_advertisers": total_active_advs,
        "total_products": total_products,
        "products_0_clicks": total_products_no_clicks,     
        "model_overlap_products": overlap
    }

# Endpoint: History
@app.get("/history/{adv}")
def history(adv: str):
    conn = get_connection()
    cur = conn.cursor()

    query = f"""
    SELECT 1
    FROM top_ctr
    WHERE advertiser_id = %s
    AND date = CURRENT_DATE
    LIMIT 1
    """
    cur.execute(query, (adv,))

    if cur.fetchone() is None:
        return {"error": "advertiser_id no esta activo el dia de hoy"}

    query = """ SELECT 
        a.advertiser_id,
        a.product_id,
        a.date,
        a.ctr,
        b.views
    FROM top_ctr AS a
    JOIN top_product AS b
        ON a.advertiser_id = b.advertiser_id
        AND a.product_id = b.product_id
        AND a.date = b.date
    WHERE a.advertiser_id = %s
    AND a.date >= CURRENT_DATE - INTERVAL '7 days'
    ORDER BY a.date DESC
"""

    cur.execute(query, (adv,))
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return {
        "advertiser_id": adv,
        "history": [
            {
                "product_id": r[1],
                "ctr": r[3],
                "views": r[4],
                "date": str(r[2])
            }
            for r in rows
        ]
    }
