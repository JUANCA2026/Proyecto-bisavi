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
    total_pages = None

    print(f"\nDescargando {nombre_hoja}...")

    while True:
        url = f"{endpoint}?page={page}&page_size={page_size}"
        response = requests.get(url, headers=api_headers)

        if response.status_code != 200:
            print("HEADERS ENVIADOS:", api_headers)
            raise Exception(f"Error en página {page}: {response.text}")

        data = response.json()

        if total_pages is None:
            pag = _safe_get_pagination(data)
            total = pag.get("total_results", 0)
            total_pages = ceil(total / page_size) if total else 1

        resultados = data.get("results", []) or []

        print(f"Página {page}: {len(resultados)} registros")

        todos_los_resultados.extend(resultados)

        if page >= total_pages:
            break

        page += 1
        time.sleep(1)

    return todos_los_resultados


# -------- PROCESAMIENTO --------

def procesar_invoices(api_headers):
    data = obtener_todos_los_resultados(
        "https://api.siigo.com/v1/invoices",
        "facturas_venta",
        api_headers
    )
    filas = []

    for doc in data:
        for item in (doc.get("items", []) or []):
            filas.append({
                "id": doc.get("id"),
                "fecha": doc.get("date"),
                "cliente": doc.get("customer", {}).get("identification"),
                "servicio": item.get("description"),
                "cantidad": item.get("quantity"),
                "precio": limpiar_valor(item.get("price")),
                "total": limpiar_valor((item.get("quantity") or 0) * (item.get("price") or 0))
            })

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
            filas.append({
                "id": doc.get("id"),
                "fecha": doc.get("date"),
                "proveedor": doc.get("supplier", {}).get("identification"),
                "servicio": item.get("description"),
                "cantidad": item.get("quantity"),
                "precio": limpiar_valor(item.get("price")),
                "total": limpiar_valor((item.get("quantity") or 0) * (item.get("price") or 0))
            })

    return filas


def procesar_journals(api_headers):
    data = obtener_todos_los_resultados(
        "https://api.siigo.com/v1/journals",
        "contable",
        api_headers
    )
    filas = []

    for doc in data:
        items = doc.get("items") or []
        if isinstance(items, dict):
            items = [items]

        for item in items:
            filas.append({
                "id": doc.get("id"),
                "fecha": doc.get("date"),
                "descripcion": item.get("description"),
                "valor": limpiar_valor(item.get("value")),
                "movimiento": item.get("account", {}).get("movement")
            })

    return filas


def procesar_payment_receipts(api_headers):
    data = obtener_todos_los_resultados(
        "https://api.siigo.com/v1/payment-receipts",
        "pagos",
        api_headers
    )
    filas = []

    for doc in data:
        items = doc.get("items") or []
        if isinstance(items, dict):
            items = [items]

        for item in items:
            filas.append({
                "id": doc.get("id"),
                "fecha": doc.get("date"),
                "descripcion": item.get("description"),
                "valor": limpiar_valor(item.get("value"))
            })

    return filas


# -------- GOOGLE SHEETS --------

def conectar_google_sheets():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    creds_dict = json.loads(creds_json)

    credentials = Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
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


# -------- MAIN --------

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

    if auth.status_code != 200:
        raise Exception(f"Error autenticando: {auth.text}")

    access_token = auth.json()["access_token"]
    print("Token generado correctamente")

    api_headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Partner-Id": "DashboardDDG"
    }

    print("HEADERS QUE SE ESTAN ENVIANDO:")
    print(api_headers)

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
