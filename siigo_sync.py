import requests
import pandas as pd
import base64
import gspread
import time
from math import ceil
from gspread_dataframe import set_with_dataframe

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

def main():
    print("Inicio del proceso SIIGO")

    # Credenciales SIIGO
    username = "Docdanielgarciac@gmail.com"
    password = "Rafael2019*"
    access_key = "NmEyMWVhMWQtMWUxMC00MjQ0LWFkY2YtMzBkNmE1NTA5MjVlOjMxPC8xT0B3ME8="

    auth_url = "https://api.siigo.com/auth"

    auth_headers = {
        "Content-Type": "application/json"
    }

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

    endpoint = "https://api.siigo.com/v1/invoices"
    resultados = obtener_todos_los_resultados(
        endpoint=endpoint,
        nombre_hoja="facturas_venta",
        api_headers=api_headers
    )

    print(f"Se descargaron {len(resultados)} documentos")
    print("Fin del proceso")

if __name__ == "__main__":
    main() 
