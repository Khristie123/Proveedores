import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, TimeoutException
import pandas as pd

def consultar_ruc():
    driver = None
    try:
        # Leer archivo Excel
        excel_path = r"COPIAR RUTA DE EXCEL AQUI"

        proveedores_df = pd.read_excel(excel_path)
        proveedores_df.columns = proveedores_df.columns.str.strip().str.upper()
        proveedores_df = proveedores_df.rename(columns={proveedores_df.columns[21]: 'RUC'})
 
        if 'RUC' not in proveedores_df.columns:
            print("La columna 'RUC' no se encontró en el archivo Excel", proveedores_df.columns.tolist() )

        # Filtrar RUCs que comienzan con "20"
        ruc20_lista = proveedores_df[proveedores_df['RUC'].astype(str).str.startswith("20")]['RUC']
        ruc20_lista = ruc20_lista.dropna().astype(int).unique()
        print("RUCs filtrados:", ruc20_lista, len(ruc20_lista))

        options = webdriver.ChromeOptions()
        #options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument("--window-size=1920,1080")
        driver = webdriver.Chrome(options=options)
        print("Abriendo el navegador...")

        driver.set_page_load_timeout(30) 

        resultados = []
        for idx, ruc in enumerate(ruc20_lista, start=1):
            ruc = str(ruc).strip()
            driver.get("https://e-consultaruc.sunat.gob.pe/cl-ti-itmrconsruc/jcrS00Alias")

            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.NAME, "search1")))
            print("Esperando a que cargue la página...", ruc)
            time.sleep(1)
            driver.find_element(By.NAME, "search1").send_keys(ruc)
            print(f"[{idx}]Buscando RUC: ", ruc)

            
            boton_consultar = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "btnAceptar"))
            )
            boton_consultar.click()
            time.sleep(2)
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//h4[contains(text(),'Fecha de Inscripción:')]")))

            fecha_inscripcion = driver.find_element(By.XPATH, "//h4[contains(text(),'Fecha de Inscripción:')]/ancestor::div/following-sibling::div/*").text
            fecha_inicio_actividades = driver.find_element(By.XPATH, "//h4[contains(text(),'Fecha de Inicio de Actividades:')]/ancestor::div/following-sibling::div/*").text
            estado_contribuyente = driver.find_element(By.XPATH, "//h4[contains(text(),'Estado del Contribuyente:')]/ancestor::div/following-sibling::div/*").text
            condicion_contribuyente = driver.find_element(By.XPATH, "//h4[contains(text(),'Condición del Contribuyente:')]/ancestor::div/following-sibling::div/*").text
            domicilio_fiscal = driver.find_element(By.XPATH, "//h4[contains(text(),'Domicilio Fiscal:')]/ancestor::div/following-sibling::div/*").text
            distrito = domicilio_fiscal.rsplit("-", 1)[-1].strip()
            razonsoc = driver.find_element(By.XPATH, "//h4[contains(text(),'Número de RUC:')]/ancestor::div/following-sibling::div/*").text
            razon_social = razonsoc.split("-", 1)[-1]

            # Llamar a la nueva función para hacer clic en otro botón y extraer más datos
            datos_adicionales = obtener_representante_legal(driver)
            driver.back()
            time.sleep(2)
            cantidad_trabajadores = obtener_cantidad_trabajadores(driver)
            driver.back()
            time.sleep(1)
            deuda_data = obtener_deuda_coactiva(driver)

            print("Razón Social:", razon_social)
            print("Dirección:", distrito)
            print("Deuda coactiva:", deuda_data)

            for periodo, deuda in deuda_data:

                resultados.append({
                    "N°": idx,
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

            
    except KeyboardInterrupt:
        print("\nEjecución interrumpida manualmente. Guardando resultados...")
    except Exception as e:
        print(f"Error inesperado: {e}")
        return f"Error de consulta - {e}"
    finally:
        if driver:
            driver.quit()
            print("Cerrando el navegador...")
        if resultados:
            resultados_df = pd.DataFrame(resultados)
            resultados_df.to_excel("resultado_rucs.xlsx", index=False)
            print("Archivo Excel 'resultado_rucs.xlsx' generado correctamente con los datos acumulados.")
        else:
             print("No se encontraron resultados para guardar.")

def obtener_representante_legal(driver):
    try:

        WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.NAME, "formRepLeg")))

        boton_formulario = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//form[@name='formRepLeg']//button")))
        boton_formulario.click()

        # Esperar a que el contenido aparezca después de hacer clic
        WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.XPATH, "//h1[contains(text(),'REPRESENTANTES LEGALES')]")))
        # Extraer los nuevos datos
        WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((By.XPATH, "//table"))
            )
        nuevo_contenido = driver.find_element(By.XPATH, "//table//tbody//tr[position() = 1]/td[3]").text

        return nuevo_contenido if nuevo_contenido else "No se encontró el representante legal"

    except TimeoutException:
        print("El botón representante legal no fue clickeable o el contenido no se cargó en el tiempo esperado.")
        nuevo_contenido = "No se pudo extraer el dato"
        return nuevo_contenido
    
def obtener_deuda_coactiva(driver):
    try:
        WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.NAME, "formInfoDeudaCoactiva")))
        boton_formulario = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//form[@name='formInfoDeudaCoactiva']//button")))
        boton_formulario.click()
        # Esperar a que el contenido aparezca después de hacer clic
        WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.XPATH, "//h3[contains(text(),'DEUDA COACTIVA REMITIDA')]")))
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

            return resultados if resultados else [(0, 0)]  # Si no hay filas, devuelve 0,0

        except TimeoutException:
            resultados = [("", 0)]
            return resultados  # No hay tabla = sin deuda

    except TimeoutException:
        print("El botón deuda coactiva no fue clickeable o el contenido no se cargó en el tiempo esperado.")
        resultados = [("No se pudo extraer el dato", "No se pudo extraer el dato")]
        return resultados  # No se pudo extraer los datos.
    
def obtener_cantidad_trabajadores(driver):
    try:
        WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.NAME, "formNumTrabajd")))

        boton_formulario = WebDriverWait(driver, 8).until(EC.element_to_be_clickable((By.XPATH, "//form[@name='formNumTrabajd']//button")))
        boton_formulario.click()
        # Esperar a que el contenido aparezca después de hacer clic
        WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.XPATH, "//h3[contains(text(),'CANTIDAD DE TRABAJADORES')]")))
        # Esperando a que la tabla aparezca
        WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((By.XPATH, "//table"))
            )
        nuevo_contenido = driver.find_element(By.XPATH, "//table//tbody//tr[last()]/td[2]").text

        return nuevo_contenido if nuevo_contenido else "No se encontró la cantidad de trabajadores"

    except TimeoutException:
        print("El botón cantidad de trabajadores no fue clickeable o el contenido no se cargó en el tiempo esperado.")
        nuevo_contenido = "No se pudo extraer el dato"
        return nuevo_contenido 

consultar_ruc()
