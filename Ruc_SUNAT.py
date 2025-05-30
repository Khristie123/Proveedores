import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import WebDriverException, TimeoutException
from bs4 import BeautifulSoup
import multiprocessing
from multiprocessing import Process, cpu_count
from datetime import datetime
import os
from multiprocessing import Manager
import re
from bs4 import BeautifulSoup, Tag, NavigableString

def dividir_lista(lista, n):
    """Divide una lista en 'n' partes aproximadamente iguales."""
    k, m = divmod(len(lista), n)
    print(f"Dividiendo lista de {len(lista)} elementos en {n} partes. {k} elementos por parte, con {m} partes adicionales.")
    return [lista[i*k + min(i, m):(i+1)*k + min(i+1, m)] for i in range(n)]


def ejecutar_scraping(ruc_lista_parcial, idx_proceso, dict_dataframes):
    """
    Ejecuta el scraping para un subconjunto de RUCs.
    Cada proceso ejecutará esta función.
    """
    resultados = []
    driver = None
    try:
        # Configuración del navegador en modo headless para evitar abrir ventana
        options = webdriver.ChromeOptions()
        #options.add_argument("--headless")
        options.add_argument("--incognito")
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument("--window-size=1920,1080")
        # Crear instancia del navegador
        driver = webdriver.Chrome(options=options)
        print("Abriendo el navegador...")
        wait = WebDriverWait(driver, 10)

        for idx, ruc in enumerate(ruc_lista_parcial, start=1):
            try:
                # Abrir la página de consulta SUNAT
                driver.get("https://e-consultaruc.sunat.gob.pe/cl-ti-itmrconsruc/jcrS00Alias")
                wait.until(EC.presence_of_element_located((By.NAME, "search1")))
                print("Cargando consultas..")
                input_ruc = driver.find_element(By.NAME, "search1")
                input_ruc.clear()
                input_ruc.send_keys(str(ruc))
                driver.find_element(By.ID, "btnAceptar").click()
                print(f"[{idx}]Buscando RUC: ", ruc)
                # Esperar a que cargue la página
                wait.until(EC.presence_of_element_located((By.XPATH, "//h4[contains(text(),'Fecha de Inscripción:')]")))
                time.sleep(1)
                # Obtener HTML completo de la página
                html = driver.page_source
                # Parsear con BeautifulSoup
                soup = BeautifulSoup(html, "html.parser")

                def extraer_valor(etiqueta):
                    h4 = soup.find("h4", string=lambda text: text and etiqueta in text)
                    if h4:
                        contenedor = h4.find_parent("div")
                        if contenedor:
                            siguiente_div = contenedor.find_next_sibling("div")
                            if siguiente_div:
                                # Buscar un <br> dentro de siguiente_div
                                p = siguiente_div.find("p")
                                if p:
                                    texto = ""
                                    for elem in p.children:
                                        if isinstance(elem, Tag) and elem.name == "br":
                                            break
                                        if isinstance(elem, NavigableString):
                                            texto += elem.strip() + " "
                                        elif isinstance(elem, Tag):
                                            texto += elem.get_text(strip=True) + " "
                                    valor = texto.strip()
                                else:
                                    # Si no hay <br>, obtener todo el texto normalmente
                                    valor = siguiente_div.get_text(strip=True)
                                return valor
                    return ""
                # Extraer datos
                fecha_inscripcion = extraer_valor("Fecha de Inscripción:")
                fecha_inicio_actividades = extraer_valor("Fecha de Inicio de Actividades:")
                estado_contribuyente = extraer_valor("Estado del Contribuyente:")
                condicion_contribuyente = extraer_valor("Condición del Contribuyente:")
                domicilio_fiscal = extraer_valor("Domicilio Fiscal:")
                distrito = domicilio_fiscal.rsplit("-", 1)[-1].strip()
                razonsoc = extraer_valor("Número de RUC:")
                razon_social = razonsoc.split("-", 1)[-1] if "-" in razonsoc else razonsoc

                # Llamar a funciones adicionales
                deuda_data = obtener_deuda_coactiva(driver)
                driver.back()
                time.sleep(1)
                cantidad_trabajadores = obtener_cantidad_trabajadores(driver)
                driver.back()
                time.sleep(1)
                datos_adicionales = obtener_representante_legal(driver)

                # Guardar resultados
                for periodo, deuda in deuda_data:
                    resultados.append({
                        "RUC": ruc,
                        "Razón Social": razon_social,
                        "F.INSCRIPCION": fecha_inscripcion,
                        "F.INICIO ACTIV.": fecha_inicio_actividades,
                        "Dirección": distrito,
                        "Deuda Coactiva": deuda,
                        "Periodo Tributario": periodo,
                        "Cantidad Trabajadores": cantidad_trabajadores,
                        "Estado": estado_contribuyente,
                        "Condición": condicion_contribuyente,
                        "Representante Legal": datos_adicionales
                    })
   
            except Exception as e:
                with open(f"error_{idx_proceso}.log", "w") as f:
                    f.write(f"Error inesperado: {str(e)}\n")
                print(f"[Proceso {idx_proceso}] Error con RUC {ruc}: {e}")
                continue
    except KeyboardInterrupt:
        print("\nEjecución interrumpida manualmente. Guardando resultados...")
    except Exception as e:
        print(f"Error inesperado: {e}")
        return f"Error de consulta - {e}"
    finally:
        # Guardar resultados incluso si hay interrupción
        if resultados:
            try:
                df_resultado = pd.DataFrame(resultados)
                dict_dataframes[f"df_resultado_{idx_proceso}"] = df_resultado
                print(f"[Proceso {idx_proceso}] Resultados almacenados en memoria como 'df_resultado_{idx_proceso}' con {len(resultados)} registros.")
            except Exception as e:
                print(f"[Proceso {idx_proceso}] Error al almacenar resultados: {e}")

        # Cerrar navegador si fue creado
        try:
            if driver:
                driver.quit()
                print("Cerrando el navegador... ")
        except Exception as e:
            print(f"Error al cerrar el navegador: {e}")


def obtener_representante_legal(driver):
    try:

        WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.NAME, "formRepLeg")))

        boton_formulario = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//form[@name='formRepLeg']//button")))
        boton_formulario.click()
        nuevo_contenido = ""
        time.sleep(1)  # Esperar un momento para que se cargue el contenido
        if driver.find_elements(By.XPATH, "//*[contains(text(), 'The requested URL was rejected. Please consult with your administrator.')]"):
             # Si aparece este mensaje, no continuar
            nuevo_contenido = "No se pudo extraer el dato"
            print("Se detectó un mensaje alternativo. Proceso interrumpido.")
            return nuevo_contenido
        try:
            WebDriverWait(driver, 12).until(EC.presence_of_element_located((By.XPATH, "//h1[contains(text(),'REPRESENTANTES LEGALES')]")))
        # Extraer los nuevos datos
            try:
                    WebDriverWait(driver, 8).until(
                                EC.presence_of_element_located((By.XPATH, "//table")) )
                    nuevo_contenido = driver.find_element(By.XPATH, "//table//tbody//tr[position() = 1]/td[3]").text

                    return nuevo_contenido
            except TimeoutException:
                    nuevo_contenido = "-"
                    return nuevo_contenido
        except TimeoutException:
            print("El contenido no se cargó en el tiempo esperado")
            nuevo_contenido = ""
            return nuevo_contenido

    except TimeoutException:
        print("El botón representante legal no fue clickeable.")
        nuevo_contenido = "-"
        return nuevo_contenido
    
def obtener_deuda_coactiva(driver):
    try:
            WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.NAME, "formInfoDeudaCoactiva")))
            boton_formulario = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//form[@name='formInfoDeudaCoactiva']//button")))
            boton_formulario.click()
            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//h3[contains(text(),'DEUDA COACTIVA REMITIDA')]")))
                # Esperar tabla
                try:
                    resultados = []
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, "//table//tbody"))
                    )
                    cuerpo_tabla = driver.find_element(By.XPATH, "//table//tbody")
                    filas = cuerpo_tabla.find_elements(By.TAG_NAME, "tr")

                    for fila in filas:
                        columnas = fila.find_elements(By.TAG_NAME, "td")
                        if len(columnas) >= 2:
                            deuda = columnas[0].text.strip()
                            periodo = columnas[1].text.strip()
                            resultados.append((periodo, deuda))

                    return resultados if resultados else [(0, 0)]
                except TimeoutException:
                    resultados = [("", "-")]
                    return resultados

            except TimeoutException:
                print("el contenido no se cargó en el tiempo esperado")
                resultados = [("", 0)]
                return resultados 

    except TimeoutException:
        print("El botón deuda coactiva no fue clickeable.")
        resultados = [("No se pudo extraer el dato", "No se pudo extraer el dato")]
        return resultados
    
def obtener_cantidad_trabajadores(driver):
    try:
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.NAME, "formNumTrabajd")))

        boton_formulario = WebDriverWait(driver, 8).until(EC.element_to_be_clickable((By.XPATH, "//form[@name='formNumTrabajd']//button")))
        boton_formulario.click()
        nuevo_contenido = ""
        time.sleep(1)  # Esperar un momento para que se cargue el contenido
        if driver.find_elements(By.XPATH, "//*[contains(text(), 'The requested URL was rejected. Please consult with your administrator.')]"):
            # Si aparece este mensaje, no continuar
            nuevo_contenido = "No se pudo extraer el dato"
            print("Se detectó un mensaje alternativo. Proceso interrumpido.")
            return nuevo_contenido
        try:    
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, "//h3[contains(text(),'CANTIDAD DE TRABAJADORES')]")))
            # Esperando a que la tabla aparezca
            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//table")))
                nuevo_contenido = driver.find_element(By.XPATH, "//table//tbody//tr[last()]/td[2]").text
                return nuevo_contenido
            except TimeoutException:
                nuevo_contenido = "-"
                return nuevo_contenido
        except TimeoutException:
            print("El contenido no se cargó en el tiempo esperado")
            nuevo_contenido = ""
            return nuevo_contenido

    except TimeoutException:
        print("El botón cantidad de trabajadores no fue clickeable.")
        nuevo_contenido = "-"
        return nuevo_contenido 

# Función para unir todos los archivos Excel en uno solo
def unir_archivos_excel(dict_dataframes):
    dfs = []
    for key in sorted(dict_dataframes.keys()): 
        df = dict_dataframes[key]
        if isinstance(df, pd.DataFrame):
            dfs.append(df)
        else:
            print(f"[UnirDataFrames] '{key}' no es un DataFrame válido y se omitirá.")
    if dfs:
        final_df = pd.concat(dfs, ignore_index=True)
        if 'RUC' not in final_df.columns:
         print("[UnirExcel] Error: No se encontró la columna 'RUC'. No se puede generar la numeración personalizada.")
        else:
            ruc_to_index = {}
            current_index = 1
            numero_columna = []

            for ruc in final_df['RUC']:
                if ruc not in ruc_to_index:
                    ruc_to_index[ruc] = current_index
                    current_index += 1
                numero_columna.append(ruc_to_index[ruc])

            # Insertar o sobrescribir la columna 'N°'
            if 'N°' in final_df.columns:
                final_df['N°'] = numero_columna
            else:
                final_df.insert(0, 'N°', numero_columna)
        final_filename = "resultado_rucs_final.xlsx"

         # Verificar si el archivo ya está abierto
        if os.path.exists(final_filename):
            try:
                os.rename(final_filename, final_filename)  # Si falla, está en uso
            except PermissionError:
                print(f"[UnirExcel] Error: Cierra el archivo '{final_filename}' antes de continuar.")
                return

        final_df.to_excel(final_filename, index=False)
        print(f"[UnirExcel] Archivo final guardado como '{final_filename}'")
    else:
        print("[UnirExcel] No se encontraron archivos para unir.")

def main():
    # Número de procesos en paralelo
    cpu_count = os.cpu_count() or 2  # Por si devuelve None
    num_procesos =  max(cpu_count - 3, cpu_count // 2)  # Máximo 4 procesos o la mitad de los núcleos

    print(f"Usando hasta {num_procesos} procesos en paralelo.")
    # Cargar archivo Excel (cambia esta ruta)
    #excel_path = r"COPIAR RUTA DE ACCESO EXCEL"  # <-- CAMBIAR AQUÍ
    try:
        ruta_input = input("Ingrese la ruta del archivo Excel con los RUCs: ")
        excel_path = ruta_input.strip('"').strip("'")
        if not os.path.exists(excel_path):
            print(f"[Main] Error: El archivo '{excel_path}' no existe. Por favor, verifica la ruta.")
            return
        
        hojas = pd.ExcelFile(excel_path).sheet_names
        print("\nHojas disponibles:")
        for i, hoja in enumerate(hojas):
            print(f"{i + 1}. {hoja}")
        hoja_seleccionada = input("\nSeleccione el número de la hoja que contiene los RUCs: ")
        try:
            hoja_seleccionada = int(hoja_seleccionada) - 1
            if hoja_seleccionada < 0 or hoja_seleccionada >= len(hojas):
                print("[Main] Error: Número de hoja inválido.")
                return
            print(f"[Main] Cargando datos de la hoja: {hojas[hoja_seleccionada]}")
        except ValueError:
            print("[Main] Error: Entrada no válida. Debe ser un número.")
            return
        proveedores_df = pd.read_excel(excel_path, sheet_name=hojas[hoja_seleccionada])

        # Normalizar nombres de columna y seleccionar columna de RUC
        proveedores_df.columns = proveedores_df.columns.map(str).str.strip().str.upper()
            # Mostrar las columnas disponibles
        print("\nColumnas disponibles:")
        for i, col in enumerate(proveedores_df.columns, start=1):
            print(f"{i}. {col}")

        while True:
            try:
                seleccion = int(input("\nIngresa el número de la columna que quieres seleccionar: "))
                if 1 <= seleccion <= len(proveedores_df.columns):
                    columna_seleccionada = proveedores_df.columns[seleccion - 1]
                    print(f"Has seleccionado la columna: {columna_seleccionada}")
                    break
                else:
                    print(f"Por favor, ingresa un número entre 1 y {len(proveedores_df.columns)}.")
            except ValueError:
                print("Entrada inválida. Debes ingresar un número entero.")

        ruc_lista = proveedores_df[proveedores_df[columna_seleccionada].notna()][columna_seleccionada].astype(str)
        ruc_lista = ruc_lista.dropna().astype(float).astype(int).unique()
        # Dividir la lista de RUCs para paralelizar
        lotes = dividir_lista(list(ruc_lista), num_procesos)
        print(len(lotes), "lotes de RUCs creados para procesamiento paralelo.")
        manager = multiprocessing.Manager()
        dict_dataframes = manager.dict()
        print(dict_dataframes)
        procesos = []
    except KeyboardInterrupt:
        print("\nProceso interrumpido por el usuario. Saliendo...")
        return  # o sys.exit() para terminar el programa sin error
    # Lanzar procesos de scraping
    try:

            for idx, lote in enumerate(lotes):
                print(f"[Main] Iniciando proceso {idx} con {len(lote)} RUCs.")
                p = Process(target=ejecutar_scraping, args=(lote, idx, dict_dataframes))
                p.start()
                procesos.append(p)
                
            for p in procesos:
                p.join()
                print(f"[Main] Proceso {p.pid} finalizado.")
    except KeyboardInterrupt:
        print("\n[Main] Ejecución interrumpida manualmente. Terminando procesos...")
    finally:
        print("[Main] Uniendo archivos generados...")
        unir_archivos_excel(dict_dataframes)
        
        
if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()

