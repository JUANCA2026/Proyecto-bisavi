import requests
import pandas as pd
import gspread
import time
import json
import os

from math import ceil
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials


SHEET_ID = "1NYMJPor7PQMXz3cyo2UXnvS3pJ2TVbLwIplI3whKkSU"


def limpiar_valor(valor):
    try:
        return int(round(float(valor)))
    except:
        return ""


def _safe_get_pagination(data):
    pag = data.get("pagination") or {}
    if not pag and isinstance(data.get("metadata"), dict):
        pag = data["metadata"].get("pagination") or {}
    return pag


def obtener_todos_los_resultados(endpoint, nombre_hoja, api_headers):
    page = 1
    page_size = 100
    todos_los_resultados = []
    total_items = 0
    total_pages = None

    print(f"\nDescargando {nombre_hoja}...")

    while True:
        url = f"{endpoint}?page={page}&page_size={page_size}"

        intentos = 0
        espera = 2
        while True:
            response = requests.get(url, headers=api_headers)
            if response.status_code == 200:
                break
            intentos += 1
            if intentos >= 3:
                raise Exception(f"Error en página {page}: {response.text}")
            time.sleep(espera)
            espera *= 2

        data = response.json()

        if total_pages is None:
            pag = _safe_get_pagination(data)
            total_results = pag.get("total_results")
            if total_results:
                total_pages = ceil(total_results / page_size)

        resultados = data.get("results", []) or []

        print(f"Página {page}: {len(resultados)} registros")

        todos_los_resultados.extend(resultados)
        total_items += len(resultados)

        if total_pages and page >= total_pages:
            break

        if not total_pages and len(resultados) < page_size:
            break

        page += 1
        time.sleep(1)

    print(f"Total en {nombre_hoja}: {total_items}\n")
    return todos_los_resultados


# ---------- PROCESAMIENTO (DESANIDADO) ----------

def procesar_invoices(api_headers):
    data = obtener_todos_los_resultados(
        "https://api.siigo.com/v1/invoices",
        "facturas_venta",
        api_headers
    )
    filas = []

    for doc in data:
        for item in (doc.get("items", []) or []):
            fila = {
                "id": doc.get("id"),
                "fecha": doc.get("date"),
                "cliente": doc.get("customer", {}).get("identification"),
                "servicio": item.get("description"),
                "cantidad": item.get("quantity"),
                "precio": limpiar_valor(item.get("price")),
                "total": limpiar_valor(
                    (item.get("quantity") or 0) * (item.get("price") or 0)
                )
            }
            filas.append(fila)

    return filas


def procesar_purchases(api_headers):
    data = obtener_todos_los_resultados(
        "https://api.siigo.com/v1/purchases",
        "facturas_compra",
        api_headers
    )
    filas = []

    for doc in data:
        for item in (doc.get("items", []) or []):
            fila = {
                "id": doc.get("id"),
                "fecha": doc.get("date"),
                "proveedor": doc.get("supplier", {}).get("identification"),
                "servicio": item.get("description"),
                "cantidad": item.get("quantity"),
                "precio": limpiar_valor(item.get("price")),
                "total": limpiar_valor(
                    (item.get("quantity") or 0) * (item.get("price") or 0)
                )
            }
            filas.append(fila)

    return filas


def procesar_journals(api_headers):
    data = obtener_todos_los_resultados(
        "https://api.siigo.com/v1/journals",
        "comprobantes",
        api_headers
    )
    filas = []

    for doc in data:
        for item in (doc.get("items", []) or []):
            fila = {
                "id": doc.get("id"),
                "fecha": doc.get("date"),
                "descripcion": item.get("description"),
                "valor": limpiar_valor(item.get("value")),
                "movimiento": item.get("account", {}).get("movement")
            }
            filas.append(fila)

    return filas


def procesar_payment_receipts(api_headers):
    data = obtener_todos_los_resultados(
        "https://api.siigo.com/v1/payment-receipts",
        "pagos",
        api_headers
    )
    filas = []

    for doc in data:
        for item in (doc.get("items", []) or []):
            fila = {
                "id": doc.get("id"),
                "fecha": doc.get("date"),
                "descripcion": item.get("description"),
                "valor": limpiar_valor(item.get("value"))
            }
            filas.append(fila)

    return filas


# ---------- GOOGLE SHEETS ----------

def conectar_google_sheets():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    creds_dict = json.loads(creds_json)

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    credentials = Credentials.from_service_account_info(
        creds_dict,
        scopes=scopes
    )

    gc = gspread.authorize(credentials)
    return gc.open_by_key(SHEET_ID)


def subir_dataframe(sh, nombre, df):
    try:
        ws = sh.worksheet(nombre)
    except:
        ws = sh.add_worksheet(title=nombre, rows=1000, cols=20)

    ws.clear()
    set_with_dataframe(ws, df)
    print(f"✔ Subido: {nombre}")


# ---------- MAIN ----------

def main():
    print("Inicio del proceso SIIGO")

    username = os.environ.get("SIIGO_USERNAME")
    password = os.environ.get("SIIGO_PASSWORD")
    access_key = os.environ.get("SIIGO_ACCESS_KEY")

    auth = requests.post(
        "https://api.siigo.com/auth",
        headers={"Content-Type": "application/json"},
        json={
            "username": username,
            "password": password,
            "access_key": access_key
        }
    )

    access_token = auth.json()["access_token"]
    print("Token generado correctamente")

    api_headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    sh = conectar_google_sheets()

    datasets = {
        "ventas": procesar_invoices(api_headers),
        "compras": procesar_purchases(api_headers),
        "contable": procesar_journals(api_headers),
        "pagos": procesar_payment_receipts(api_headers)
    }

    for nombre, data in datasets.items():
        df = pd.DataFrame(data)
        subir_dataframe(sh, nombre, df)

    print("Fin del proceso")


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
