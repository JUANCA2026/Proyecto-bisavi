import requests
import pandas as pd
import gspread
import time
import json
import os

from math import ceil
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials


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

    print(f"\nDescargando {nombre_hoja.replace('_', ' ')}...")

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
                raise Exception(
                    f"Error en página {page} del endpoint {endpoint}: "
                    f"{response.status_code}, {response.text}"
                )
            time.sleep(espera)
            espera *= 2

        data = response.json()

        if total_pages is None:
            pag = _safe_get_pagination(data)
            total_results = pag.get("total_results")
            if isinstance(total_results, int) and total_results >= 0:
                total_pages = ceil(total_results / page_size) if page_size else None

        resultados = data.get("results", []) or []

        num_items_pagina = 0
        for doc in resultados:
            items = doc.get("items") or []
            if isinstance(items, dict):
                items = [items]
            num_items_pagina += len(items)

        print(f"Página {page} de {nombre_hoja}: {num_items_pagina} registros")
        todos_los_resultados.extend(resultados)
        total_items += num_items_pagina

        if total_pages is not None:
            if page >= total_pages:
                break
        else:
            if len(resultados) < page_size:
                break

        page += 1
        time.sleep(1)

    print(f"Total registros en {nombre_hoja}: {total_items}\n")
    return todos_los_resultados


def procesar_invoices(api_headers):
    data = obtener_todos_los_resultados(
        "https://api.siigo.com/v1/invoices",
        "facturas_venta",
        api_headers
    )
    filas = []

    for doc in data:
        for item in (doc.get("items", []) or []):
            try:
                qty = float(item.get("quantity", 0) or 0)
            except:
                qty = 0.0

            try:
                unit_price = float(item.get("price", 0) or 0)
            except:
                unit_price = 0.0

            pago_total_item = int(round(qty * unit_price))
            pagos = doc.get("payments", []) or []
            pago = pagos[0] if pagos else {}

            fila = {
                "id_documento": doc.get("id"),
                "tipo_documento": "factura_venta",
                "movimiento": "ingreso",
                "consecutivo_documento": doc.get("name"),
                "fecha_documento": doc.get("date"),
                "id_cliente/proveedor": doc.get("customer", {}).get("identification", ""),
                "id_servicio": item.get("code", ""),
                "servicio": item.get("description", ""),
                "cantidad_servicio": item.get("quantity", ""),
                "precio_unitario_servicio": limpiar_valor(item.get("price", "")),
                "id_pago": pago.get("id", ""),
                "medio_de_pago": pago.get("name", ""),
                "pago_total": pago_total_item,
                "debito_credito": "",
                "fuente": "Siigo",
                "url_publica": doc.get("public_url")
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
        pagos = doc.get("payments", []) or []
        pago = pagos[0] if pagos else {}

        for item in (doc.get("items", []) or []):
            try:
                qty = float(item.get("quantity", 0) or 0)
            except:
                qty = 0.0

            try:
                unit_price = float(item.get("price", 0) or 0)
            except:
                unit_price = 0.0

            pago_total_item = int(round(qty * unit_price))

            fila = {
                "id_documento": doc.get("id"),
                "tipo_documento": "factura_compra",
                "movimiento": "egreso",
                "consecutivo_documento": doc.get("name"),
                "fecha_documento": doc.get("date"),
                "id_cliente/proveedor": doc.get("supplier", {}).get("identification", ""),
                "id_servicio": item.get("code", ""),
                "servicio": item.get("description", ""),
                "cantidad_servicio": item.get("quantity", ""),
                "precio_unitario_servicio": limpiar_valor(item.get("price", "")),
                "id_pago": pago.get("id", ""),
                "medio_de_pago": pago.get("name", ""),
                "pago_total": pago_total_item,
                "debito_credito": "",
                "fuente": "Siigo",
                "url_publica": doc.get("public_url")
            }
            filas.append(fila)

    return filas


def procesar_journals(api_headers):
    data = obtener_todos_los_resultados(
        "https://api.siigo.com/v1/journals",
        "comprobantes_contables",
        api_headers
    )
    filas = []

    for doc in data:
        items = doc.get("items") or []
        if isinstance(items, dict):
            items = [items]

        for item in items:
            account = item.get("account", {}) or {}
            customer = item.get("customer", {}) or {}

            fila = {
                "id_documento": doc.get("id"),
                "tipo_documento": "comprobante_contable",
                "movimiento": "egreso",
                "consecutivo_documento": doc.get("name"),
                "fecha_documento": doc.get("date"),
                "id_cliente/proveedor": customer.get("identification", ""),
                "id_servicio": account.get("code", ""),
                "servicio": item.get("description", ""),
                "cantidad_servicio": "",
                "precio_unitario_servicio": "",
                "id_pago": "",
                "medio_de_pago": "",
                "pago_total": limpiar_valor(item.get("value", "")),
                "debito_credito": account.get("movement", ""),
                "fuente": "Siigo",
                "url_publica": doc.get("public_url")
            }
            filas.append(fila)

    return filas


def procesar_payment_receipts(api_headers):
    data = obtener_todos_los_resultados(
        "https://api.siigo.com/v1/payment-receipts",
        "recibos_pago_egreso",
        api_headers
    )
    filas = []

    for doc in data:
        items = doc.get("items") or []
        if isinstance(items, dict):
            items = [items]

        for item in items:
            customer_id = item.get("customer", {}).get("identification", "")

            fila = {
                "id_documento": doc.get("id"),
                "tipo_documento": "recibo_pago_egreso",
                "movimiento": item.get("account", {}).get("movement", ""),
                "consecutivo_documento": doc.get("name"),
                "fecha_documento": doc.get("date"),
                "id_cliente/proveedor": customer_id,
                "id_servicio": "",
                "servicio": item.get("description", ""),
                "cantidad_servicio": "",
                "precio_unitario_servicio": "",
                "id_pago": "",
                "medio_de_pago": "",
                "pago_total": limpiar_valor(item.get("value", "")),
                "debito_credito": "",
                "fuente": "Siigo",
                "url_publica": doc.get("public_url")
            }
            filas.append(fila)

    return filas


def conectar_google_sheets():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_json:
        raise Exception("No se encontró GOOGLE_CREDENTIALS en GitHub Secrets")

    creds_dict = json.loads(creds_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    credentials = Credentials.from_service_account_info(
        creds_dict,
        scopes=scopes
    )

    gc = gspread.authorize(credentials)
    return gc.open("DDG_data_2026")


def obtener_o_crear_worksheet(sh, nombre_hoja, filas=1000, columnas=50):
    try:
        worksheet = sh.worksheet(nombre_hoja)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sh.add_worksheet(title=nombre_hoja, rows=filas, cols=columnas)
    return worksheet


def subir_dataframe_a_hoja(sh, nombre_hoja, df):
    worksheet = obtener_o_crear_worksheet(
        sh,
        nombre_hoja,
        filas=max(len(df) + 10, 1000),
        columnas=max(len(df.columns) + 5, 20)
    )
    worksheet.clear()
    set_with_dataframe(worksheet, df)
    print(f"Datos enviados a la hoja: {nombre_hoja}")


def main():
    print("Inicio del proceso SIIGO")

    username = os.environ.get("SIIGO_USERNAME")
    password = os.environ.get("SIIGO_PASSWORD")
    access_key = os.environ.get("SIIGO_ACCESS_KEY")

    if not username or not password or not access_key:
        raise Exception("Faltan secrets de SIIGO en GitHub")

    auth_url = "https://api.siigo.com/auth"
    auth_headers = {"Content-Type": "application/json"}
    auth_data = {
        "username": username,
        "password": password,
        "access_key": access_key
    }

    response = requests.post(auth_url, headers=auth_headers, json=auth_data)

    if response.status_code == 200:
        access_token = response.json()["access_token"]
        print("Token generado correctamente")
    else:
        raise Exception(f"Error en autenticación: {response.status_code}, {response.text}")

    api_headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Partner-Id": "DashboardDDG"
    }

    sh = conectar_google_sheets()

    datasets = {
        "facturas_venta": procesar_invoices(api_headers),
        "facturas_compra": procesar_purchases(api_headers),
        "comprobantes_contables": procesar_journals(api_headers),
        "recibos_pago_egreso": procesar_payment_receipts(api_headers),
    }

    for nombre_hoja, filas in datasets.items():
        df = pd.DataFrame(filas)
        print(f"{nombre_hoja}: {len(df)} filas desanidadas")
        subir_dataframe_a_hoja(sh, nombre_hoja, df)

    print("Fin del proceso")


if __name__ == "__main__":
    main()
