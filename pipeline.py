

import pandas as pd
import pyodbc
import os

# ─────────────────────────────────────────────────────
#  CONFIGURACIÓN
# ─────────────────────────────────────────────────────
SERVER   = "localhost"      
DATABASE = "AnalisisOpiniones"

CONN_STR = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
)


BASE_DIR = os.path.dirname(__file__)

# ─────────────────────────────────────────────────────
#  TRANSFORMACIÓN
# ─────────────────────────────────────────────────────
def transform_fuentes(df):
    df.columns = df.columns.str.strip()
    df = df[df['IdFuente'] != 'IdFuente']
    df = df.rename(columns={
        'IdFuente':   'id_fuente',
        'TipoFuente': 'nombre_tipo',
        'FechaCarga': 'fecha_carga'
    })
    df['id_fuente']   = df['id_fuente'].str.extract(r'(\d+)').astype(int)
    df['fecha_carga'] = pd.to_datetime(df['fecha_carga'], errors='coerce').dt.date
    df = df.dropna().drop_duplicates(subset=['id_fuente'])

    tipos = df[['nombre_tipo']].drop_duplicates().reset_index(drop=True)
    tipos['id_tipo_fuente'] = tipos.index + 1
    df = df.merge(tipos, on='nombre_tipo')

    fuentes = df[['id_fuente', 'id_tipo_fuente', 'fecha_carga']].copy()
    tipos   = tipos[['id_tipo_fuente', 'nombre_tipo']].copy()
    return fuentes, tipos


def transform_clientes(df):
    df.columns = df.columns.str.strip()
    df = df.rename(columns={
        'IdCliente': 'id_cliente',
        'Nombre':    'nombre',
        'Email':     'email'
    })
    df['id_cliente'] = pd.to_numeric(df['id_cliente'], errors='coerce')
    df = df.dropna(subset=['id_cliente', 'nombre', 'email'])
    df = df.drop_duplicates(subset=['id_cliente'])
    return df[['id_cliente', 'nombre', 'email']].copy()


def transform_productos(df):
    df.columns = df.columns.str.strip()
    df = df.rename(columns={
        'IdProducto': 'id_producto',
        'Nombre':     'nombre',
        'Categoría':  'nombre_categoria',
        'Categoria':  'nombre_categoria'
    })
    df['id_producto'] = pd.to_numeric(df['id_producto'], errors='coerce')
    df = df.dropna(subset=['id_producto', 'nombre', 'nombre_categoria'])
    df = df.drop_duplicates(subset=['id_producto'])

    cats = df[['nombre_categoria']].drop_duplicates().reset_index(drop=True)
    cats['id_categoria'] = cats.index + 1
    df = df.merge(cats, on='nombre_categoria')

    productos  = df[['id_producto', 'nombre', 'id_categoria']].copy()
    categorias = cats[['id_categoria', 'nombre_categoria']].copy()
    return productos, categorias


def transform_social(df):
    df.columns = df.columns.str.strip()
    df = df.rename(columns={
        'IdComment':  'id_comentario',
        'IdCliente':  'id_cliente',
        'IdProducto': 'id_producto',
        'Fuente':     'fuente',
        'Fecha':      'fecha',
        'Comentario': 'comentario'
    })
    df['id_comentario'] = df['id_comentario'].astype(str).str.extract(r'(\d+)').astype('Int64')
    df['id_cliente']    = df['id_cliente'].astype(str).str.extract(r'(\d+)')
    df['id_producto']   = df['id_producto'].astype(str).str.extract(r'(\d+)').astype('Int64')
    df['id_fuente']     = 1
    df['fecha']         = pd.to_datetime(df['fecha'], errors='coerce').dt.date
    df['comentario']    = df['comentario'].astype(str).str.strip()
    df = df.dropna(subset=['id_comentario', 'id_producto'])
    df = df.drop_duplicates(subset=['id_comentario'])
    return df[['id_comentario', 'id_cliente', 'id_producto', 'id_fuente', 'fecha', 'comentario']].copy()


def transform_surveys(df):
    df.columns = df.columns.str.strip()
    df = df.rename(columns={
        'IdOpinion':           'id_opinion',
        'IdCliente':           'id_cliente',
        'IdProducto':          'id_producto',
        'Fecha':               'fecha',
        'Comentario':          'comentario',
        'Clasificación':       'clasificacion',
        'Clasificacion':       'clasificacion',
        'PuntajeSatisfacción': 'puntaje_satisfaccion',
        'PuntajeSatisfaccion': 'puntaje_satisfaccion',
        'Fuente':              'fuente'
    })
    df['id_opinion']           = pd.to_numeric(df['id_opinion'],           errors='coerce').astype('Int64')
    df['id_cliente']           = pd.to_numeric(df['id_cliente'],           errors='coerce').astype('Int64')
    df['id_producto']          = pd.to_numeric(df['id_producto'],          errors='coerce').astype('Int64')
    df['puntaje_satisfaccion'] = pd.to_numeric(df['puntaje_satisfaccion'], errors='coerce').astype('Int64')
    df['id_fuente']            = 1
    df['fecha']                = pd.to_datetime(df['fecha'], errors='coerce').dt.date
    df['comentario']           = df['comentario'].astype(str).str.strip()
    df['clasificacion']        = df['clasificacion'].astype(str).str.strip()
    df = df.dropna(subset=['id_opinion', 'id_cliente', 'id_producto'])
    df = df.drop_duplicates(subset=['id_opinion'])

    # Filtrar solo clientes que existen en clients.csv
    clientes_validos = pd.read_csv(os.path.join(BASE_DIR, 'clients.csv'), encoding='utf-8-sig')
    clientes_validos.columns = clientes_validos.columns.str.strip()
    ids_validos = pd.to_numeric(clientes_validos['IdCliente'], errors='coerce').dropna().astype(int).tolist()
    df = df[df['id_cliente'].astype(int).isin(ids_validos)]

    return df[['id_opinion', 'id_cliente', 'id_producto', 'fecha',
                'comentario', 'clasificacion', 'puntaje_satisfaccion', 'id_fuente']].copy()


def transform_reviews(df):
    df.columns = df.columns.str.strip()
    df = df.rename(columns={
        'IdReview':   'id_review',
        'IdCliente':  'id_cliente',
        'IdProducto': 'id_producto',
        'Fecha':      'fecha',
        'Comentario': 'comentario',
        'Rating':     'rating'
    })
    df['id_review']   = df['id_review'].astype(str).str.extract(r'(\d+)').astype('Int64')
    df['id_cliente']  = df['id_cliente'].astype(str).str.extract(r'(\d+)').astype('Int64')
    df['id_producto'] = df['id_producto'].astype(str).str.extract(r'(\d+)').astype('Int64')
    df['rating']      = pd.to_numeric(df['rating'], errors='coerce').astype('Int64')
    df['id_fuente']   = 1
    df['fecha']       = pd.to_datetime(df['fecha'], errors='coerce').dt.date
    df['comentario']  = df['comentario'].astype(str).str.strip()
    df = df.dropna(subset=['id_review', 'id_cliente', 'id_producto'])
    df = df.drop_duplicates(subset=['id_review'])
    return df[['id_review', 'id_cliente', 'id_producto', 'fecha', 'comentario', 'rating', 'id_fuente']].copy()


# ─────────────────────────────────────────────────────
#  Carga para SQL SERVER
# ─────────────────────────────────────────────────────
def insertar(cursor, tabla, df, cols):
    pk = cols[0]
    col_names    = ', '.join(cols)
    placeholders = ', '.join(['?' for _ in cols])
    sql = (
        f"IF NOT EXISTS (SELECT 1 FROM {tabla} WHERE {pk} = ?) "
        f"INSERT INTO {tabla} ({col_names}) VALUES ({placeholders})"
    )
    ok = 0
    errores = 0
    for row in df[cols].itertuples(index=False):
        fila = []
        for v in row:
            try:
                if pd.isna(v):
                    fila.append(None)
                    continue
            except (TypeError, ValueError):
                pass
            if hasattr(v, 'item'):
                fila.append(int(v.item()))
            elif isinstance(v, float) and v != v:
                fila.append(None)
            else:
                fila.append(v)
        fila = tuple(fila)
        try:
            cursor.execute(sql, (fila[0],) + fila)
            ok += 1
        except Exception as e:
            errores += 1
            if errores <= 3:
                print(f"    ⚠  {tabla}: {e}")
    estado = f" {ok} insertados" + (f", {errores} errores" if errores else "")
    print(f"  {tabla:<22} → {estado}")


# ─────────────────────────────────────────────────────
#  este seria el pipeline principal
# ─────────────────────────────────────────────────────
def run_pipeline():
    print("=" * 52)
    print("   Pipeline ETL - customer_opinion")
    print("=" * 52)

    # ── 1. EXTRACCION Y TRANSFORMACION ───────────────
    print("\n[1/3] Leyendo y transformando CSVs...")
    print(f"      Carpeta: {BASE_DIR}\n")

    fuentes, tipos  = transform_fuentes(pd.read_csv(os.path.join(BASE_DIR, 'fuente_datos.csv'),   encoding='utf-8-sig'))
    clientes        = transform_clientes(pd.read_csv(os.path.join(BASE_DIR, 'clients.csv'),        encoding='utf-8-sig'))
    productos, cats = transform_productos(pd.read_csv(os.path.join(BASE_DIR, 'products.csv'),      encoding='utf-8-sig'))
    social          = transform_social(pd.read_csv(os.path.join(BASE_DIR, 'social_comments.csv'),  encoding='utf-8-sig'))
    opiniones       = transform_surveys(pd.read_csv(os.path.join(BASE_DIR, 'surveys_part1.csv'),   encoding='utf-8-sig'))
    reviews         = transform_reviews(pd.read_csv(os.path.join(BASE_DIR, 'web_reviews.csv'),     encoding='utf-8-sig'))

    print(f"  tipo_fuente          → {len(tipos)} registros")
    print(f"  fuente               → {len(fuentes)} registros")
    print(f"  categoria            → {len(cats)} registros")
    print(f"  producto             → {len(productos)} registros")
    print(f"  cliente              → {len(clientes)} registros")
    print(f"  comentario_social    → {len(social)} registros")
    print(f"  opinion              → {len(opiniones)} registros")
    print(f"  review_web           → {len(reviews)} registros")

    # ── 2. CONEXION ──────────────────────────────────
    print("\n[2/3] Conectando a SQL Server...")
    try:
        conn   = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        print(f"   Conectado a [{SERVER}] → {DATABASE}")
    except Exception as e:
        print(f"\n   Error de conexión: {e}")
        print("  → Verifica que SQL Server esté corriendo y el valor de SERVER en el script.")
        return

    # ── 3. CARGA (respetando orden de FK) ────────────
    print("\n[3/3] Insertando en SQL Server...")

    insertar(cursor, 'tipo_fuente',       tipos,     ['id_tipo_fuente', 'nombre_tipo'])
    insertar(cursor, 'fuente',            fuentes,   ['id_fuente', 'id_tipo_fuente', 'fecha_carga'])
    insertar(cursor, 'categoria',         cats,      ['id_categoria', 'nombre_categoria'])
    insertar(cursor, 'producto',          productos, ['id_producto', 'nombre', 'id_categoria'])
    insertar(cursor, 'cliente',           clientes,  ['id_cliente', 'nombre', 'email'])
    insertar(cursor, 'comentario_social', social,    ['id_comentario', 'id_cliente', 'id_producto', 'id_fuente', 'fecha', 'comentario'])
    insertar(cursor, 'opinion',           opiniones, ['id_opinion', 'id_cliente', 'id_producto', 'fecha', 'comentario', 'clasificacion', 'puntaje_satisfaccion', 'id_fuente'])
    insertar(cursor, 'review_web',        reviews,   ['id_review', 'id_cliente', 'id_producto', 'fecha', 'comentario', 'rating', 'id_fuente'])

    conn.commit()
    cursor.close()
    conn.close()

    print("\n" + "=" * 52)
    print("    Pipeline se ha completado correctamente")
    print("=" * 52)


if __name__ == "__main__":
    run_pipeline()