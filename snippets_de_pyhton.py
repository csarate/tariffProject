import datetime
import tabula
import pandas as pd

from IPython.display import display

from scrapers.utils_test import util

pd.options.display.max_rows = None

import pdfplumber as pl
import numpy as np

import requests
import requests_html
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup


# --------------------------------------------------------------------
# PRUEBA TABULA-PY
# --------------------------------------------------------------------
def prueba_tabula():
    # Read remote pdf into a list of DataFrame
    link = "https://oceba.gba.gov.ar/nueva_web/PDFS/cuadros-tarifarios/julio-2022/edelap-junio-con-subsidio.pdf"
    # convert PDF into CSV
    dfs = tabula.read_pdf(link, stream=True, pages="all")
    # read_pdf returns list of DataFrames
    print(len(dfs))
    for i in range(len(dfs)):
        print(dfs[i])


# --------------------------------------------------------------------
# PRUEBA TABULA-PDFPLUMBER
# --------------------------------------------------------------------
def prueba_tabla_pdfplumber():
    path = '/home/claudio/Documentos/Proyectos ML/BID/tariffProject/local_s3/el_salvador/Pliego Tarifario a partir del 15 de julio de 2016.pdf'
    pdf = pl.open(path)
    
    #   Armado del dataframe con las tablas del PDF
    pages = pdf.pages
    df_tabla = pd.DataFrame()
    df_page = pd.DataFrame()
    lista_lit = []
    for p, page in enumerate(pages):
        texto = page.extract_text()
        lista_lit.append(texto.split("\n"))
        tables = page.extract_tables()
        for t, table in enumerate(tables):
            df_page = pd.DataFrame(table[::], columns=['Columna', 'Valor1', 'Valor2', 'Valor3',
                                                       'Valor4', 'Valor5', 'Valor6', 'Valor7', 'Valor8'])
            
            #         if p == 0:
            #             display(df_page)
            #         else:
            #             display(len(df_page))
            df_tabla = pd.concat([df_tabla, df_page])
    display(df_tabla)


def arma_desde_pagina(lista):
    df_lista = []
    
    linea_cargo = ""
    for f, fila in enumerate(lista):
        if "SUPER" in fila or "ELECTRICIDAD" in fila or "TARIFAS" in fila or "PRECIOS" in fila or "acuerdo" in fila:
            continue
        if "VIGENTES" in fila or "I." in fila or "II." in fila or "TENSION" in fila or "Tarifa" in fila \
                or "Bloque" in fila or "Uso" in fila or "Alumbrado" in fila:
            linea = [fila, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]
            df_lista.append(linea)
            continue
        if "CAESS" in fila:
            filas = fila.lstrip(" ")
            filas = filas.replace("DEL SUR", "DEL-SUR")
            filas = filas.split(" ")
            col = [np.nan]
            for c, columna in enumerate(filas):
                if "-" in columna:
                    columna = columna.replace("-", " ")
                col.append(columna)
            df_lista.append(col)
            continue
        if 'Cargo de Comercia' in fila or 'Cargo de Ener:' in fila or 'Cargo de Distri' in fila:
            if 'Cargo de Comercia' in fila:
                linea_cargo = fila + "\n "
                linea = [fila, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]
                df_lista.append(linea)
                ind = len(df_lista) - 1
                continue
            else:
                linea_cargo = linea_cargo + " " + fila + "\n  "
                continue
        else:
            filas = fila.lstrip(" ")
            filas = filas.replace("-mes", "-mes ")
            filas = filas.replace("Cargo ", "Cargo&")
            filas = filas.replace(" al Cliente", "&al&Cliente ")
            filas = filas.replace(" en ", "&en&")
            filas = filas.split(" ")
            col = [np.nan]
            for c, columna in enumerate(filas):
                if "." in columna:
                    col.append(columna)
                else:
                    columna = columna.replace("&", " ")
                    linea_cargo = linea_cargo + " " + columna + " "
            df_lista[ind][0] = linea_cargo
            df_lista.append(col)
    
    df = pd.DataFrame(df_lista, columns=['Columna', 'Valor1', 'Valor2', 'Valor3',
                                         'Valor4', 'Valor5', 'Valor6', 'Valor7', 'Valor8'])
    return df
    

# ------------------------------------
# Desanidación Tablas Anidadas
# ------------------------------------
def desanidar_lista(lista, lista_d):
    for e, elemento in enumerate(lista):
        if isinstance(elemento, list):
            lista_d = desanidar_lista(elemento, lista_d)
        else:
            lista_d.append(elemento.strip())
    return lista_d
        

def prueba_pdfplumber():
    path = '/home/claudio/Documentos/Proyectos ML/BID/tariffProject/local_s3/el_salvador/Index Pliegos julio - octubre 2022.pdf'
    pdf = pl.open(path)

    #   Armado del dataframe con las tablas del PDF
    pages = pdf.pages
    df_tabla = pd.DataFrame()
    lista_lit = []
    for p, page in enumerate(pages):
        texto = page.extract_text()
        lista_lit.append(texto.split("\n"))
        tables = page.extract_tables()
        for t, table in enumerate(tables):
            if p > 0 and t == 0:
                df_page = arma_desde_pagina(lista_lit[1])
                df_tabla = pd.concat([df_tabla, df_page])
                break
            else:
                df_page = arma_desde_pagina(lista_lit[0])
                df_tabla = pd.concat([df_tabla, df_page])
    
   
    # Elimina filas con valores nulos en la tabla
    df_tabla = df_tabla.replace({None: np.nan})
    df_tabla = df_tabla.replace({'': np.nan})
    df_tabla = df_tabla.dropna(how='all')
    df_tabla.reset_index(drop=True, inplace=True)
    display(df_tabla)

# -------------------------------------------------------------------
# Diccionario de niveles de busqueda
# -------------------------------------------------------------------
def tags_dict():
    tags = \
        {
        'level_0':
            'soup.find_all("div", class_="iksm-term iksm-term--id-7416 iksm-term--parent iksm-term--has-children")',
        'level_1':
            'soup.find_all("div", class_="iksm-term iksm-term--id-37524 iksm-term--parent iksm-term--has-children")',
        'level_2':
            'soup.find_all("div", class_="iksm-term iksm-term--id-37187 iksm-term--parent iksm-term--has-children")'
        }
    return tags


def connect_to_link(s, link):
    try:
        response = s.get(link, verify=False)
    except requests.ConnectionError as e:
        print('ConnectionError = {} trying to connect to {}'.format(e, link))
        return False
    except requests.Exception as e:
        print('Exception other than ConnectionError ocurred = {} '
              'trying to connect to {}'.format(e, link))
        return False
    else:
        # check if a 200 response is received to follow
        if response.status_code != 200:
            print('Received a status code {} trying to connect '
                  'to {}'.format(str(response.status_code), link)
                  )
            return False
    print('Connected to {}'.format(link))
    
    return response


# -------------------------------------------------------------------
# Funcion para construir niveles de profundidad de paginas a navegar
# -------------------------------------------------------------------
def build_deep_links(soup, level, last_iteration, list_of_links):
    tag_dict = tags_dict()
# script = '''

    nivel = "level_" + str(level)
    script = tag_dict.get(nivel)
    table_with_links = eval(script)

    # check if the table has the links

    if not table_with_links:
        print('Not logged on the right url')
        valor = False
        return valor

    list_of_links.extend(table_with_links[0].find_all("a", href=True))
    lista_urls = table_with_links[0].find_all("a")

    loop_list_links(lista_urls, last_iteration, level)
    

    return valor, pliegos, list_of_links

# '''
# exec(script)


def loop_list_links(lista_urls, last_iteration, level):
    for tl in lista_urls:
        if last_iteration:
            break
        
        tariff_link = tl.get('href')[:]
        
        if '#' in tariff_link:
            continue
        
        valor, soup = connect_to_link(soup, tariff_link)
        
        if not valor:
            continue
        
        level += 1
        valor, pliegos, list_of_links = build_deep_links(soup, level)
        
        if not url_pdf:
            print('Not logged on the right url')
            return False
    
    


# --------------------------------------------------------------------
# PRUEBA BS4
# --------------------------------------------------------------------
def prueba_bs4():
    session = requests_html.HTMLSession()
    retries = Retry(
        total=5,
        backoff_factor=0.1,
        status_forcelist=[500, 502, 503, 504],
        raise_on_status=True
    )
    session.mount('http://', HTTPAdapter(max_retries=retries))
    
    base_link = 'https://www.ine.gob.ni/index.php/electricidad/'
    
    r = connect_to_link(session, base_link)
    if not r:
        return False
    
    soup = BeautifulSoup(r.text, 'html5lib')
    
    level = 0
    last_iteration = False
    list_of_links = []
    
    valor, pliegos, list_of_links = build_deep_links(soup, level, last_iteration, list_of_links)

def rutina():
    import requests
    from bs4 import BeautifulSoup
    from csv import writer
    
    link = 'https://www.ine.gob.ni/index.php/electricidad/pliegos-tarifarios-disnorte-dissur/cargos-sociales-subsidiados-enero-2022/'
    nombre = link.split("/")[-2]
    print(nombre)
    mesyear = nombre.split("-")[-2] + nombre.split("-")[-1]
    mes_year = nombre.split("-")[-2] + "-" + nombre.split("-")[-1]
    print(mesyear)
    print(mes_year)
    
    response = requests.get(link, verify=False)
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    if "2021" in mes_year and ("aplicado-e-indicativo" in nombre or "cargos-sociales-subsidiados" in nombre):
        urls = soup.find_all('a', attrs={'class': 'cuerpo2'})
    else:
        urls = soup.find_all('a', attrs={'href': True})
    
    lista = []
    for tag in urls:
        link = tag.get("href").strip("=")
        if "#" in link:
            continue
        lista.append(link)
    # print(lista)
    
    links = []
    for p in lista:
        if "liegos" in p:
            links.append(p)
            continue
        if "tension" in p and ".pdf" in p:
            links.append(p)
            continue
        if "redes" in p and ".pdf" in p:
            links.append(p)
            continue
        if "comercial" in p and ".pdf" in p:
            links.append(p)
            continue
        if "ap_" in p and ".pdf" in p:
            links.append(p)
            continue
    
    print(links)


# ------------------------------------------------------------------
# Diccionario de Pliego Tarifario
# ------------------------------------------------------------------
def pliegos_validos():
    dict_p = \
        {'Index pliego tarif': 'Pliego Tarifario',
         'Index Pliegos': 'Pliego Tarifario',
         'pliego tarifario': 'Pliego Tarifario',
         'pliego_tarifa_social': 'Pliego Tarifario',
         'baja_tension': 'Pliego Tarifario',
         'media_tension': 'Pliego Tarifario',
         'uso_de_redes': 'Pliego Tarifario',
         'Pliego tarifario': 'Pliego Tarifario',
         'Pliego Tarifario': 'Pliego Tarifario',
         'Pliegos Tarifarios': 'Pliego Tarifario',
         'PLIEGOS TARIFARIOS': 'Pliego Tarifario',
         'Pliego_Tarifario': 'Pliego Tarifario',
         'Publicacion pliego': 'Pliego Tarifario',
         'Publicacion Pliegos': 'Pliego Tarifario'
         }
    return dict_p


# ----------------------------------------------------------------------
# Vefificación existencia y consistencia de datos en el nombre del pdf
# ----------------------------------------------------------------------
def verifica_link(link, date_limit, local, last_iteration):
    mes_dict = util.MonthDict()
    mes_year = ""
    extension = ""
    link_title = ""
    day = "01"
    valor = True
    
    try:
        words = link.split("/")
        for w, word in enumerate(words):
            
            if word.isdigit():
                if len(word) == 1:
                    mes = "0" + word
                    continue
                if len(word) == 2:
                    mes = word
                    continue
                if len(word) == 4:
                    year = word
                    continue
            
        if mes_dict.get(mes):
            month = mes_dict.get(mes)
        
        year_month = year + "-" + month
        year_month_day = year_month + "-" + day
        fecha = datetime.datetime.strptime(year_month_day, "%Y-%B-%d").strftime("%Y-%m-%d")
        anio_mes = datetime.datetime.strptime(year_month, "%Y-%B").strftime("%Y-%m")
        
        extension = link.split("/")[-1].split(".")[-1].split(" ")[0]
        link_title = link.split("/")[-1]
        link_title = link_title.strip()
    except:
        valor = False
        return valor, mes_year, extension, link_title, last_iteration
    
    # pliegos = pliegos_validos()
    # for p in pliegos.keys():
    #     if p in link_title:
    #         valor = True
    #         break
    #     valor = False
    
    if not valor:
        util.log(local, 'Did not found a price list format on the link = {}'.format(link))
        print('Did not found a price list format on the link = {}'.format(link))
        return valor, mes_year, extension, link_title, last_iteration
    
    if year < date_limit:
        valor = False
        last_iteration = True
        return valor, mes_year, extension, link_title, last_iteration
    
    # lista = mes_year.split(" ")
    # for mes in lista:
    #     if mes_dict.get(mes):
    #         valor = True
    #         break
    #     valor = False
    
    if not valor:
        util.log(local, 'Did not found a date format on the link = {}'.format(link))
        print(
            'Did not found a date format on the link = {}'.format(link)
        )
        return valor, mes_year, extension, link_title, last_iteration
    
    # month = mes_dict[mes]
    month_year = month + "-" + year
    mes_year = datetime.datetime.strptime(month_year, "%B-%Y").strftime("%m-%Y")
    valor = True
    return valor, mes_year, extension, link_title, last_iteration


def rutina_ver_links():
    urls_pdfs = [
        'https://www.ine.gob.ni/wp-content/uploads/2022/01/pliego_tarifa_social_energia_comercializacion_enero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/01/pliego_tarifa_social_alumbrado_publico_enero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/01/baja_tension_1_enero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/01/baja_tension_2_enero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/01/baja_tension_bombeo_comunitario_enero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/01/media_tension_3_enero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/01/media_tension_4_enero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/01/media_tension_bombeo_comunitario_enero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/01/uso_redes_enero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/01/comercializacion_enero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/01/ap_managua_enero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/01/ap_chinandega_enero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/01/ap_san_juan_sur_enero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/01/ap_resto_municipios_enero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/01/servicios_comerciales_enero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/01/generacion_distribuida_enero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/02/pliego_tarifa_social_energia_comercializacion_febrero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/02/pliego_tarifa_social_alumbrado_publico_febrero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/02/baja_tension_1_febrero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/02/baja_tension_2_febrero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/02/baja_tension_bombeo_comunitario_febrero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/02/media_tension_3_febrero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/02/media_tension_4_febreo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/02/media_tension_bombeo_comunitario_febrero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/02/uso_redes_febrero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/02/comercializacion_febrero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/02/ap_managua_febrero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/02/ap_chinandega_febrero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/02/ap_san_juan_sur_febrero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/02/ap_resto_municipios_febrero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/02/servicios_comerciales_febrero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/02/generacion_distribuida_febrero22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/03/pliego_tarifa_social_energia_comercializacion_marzo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/03/pliego_tarifa_social_alumbrado_publico_marzo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/03/baja_tension_1_marzo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/03/baja_tension_2_marzo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/03/baja_tension_bombeo_comunitario_marzo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/03/media_tension_3_marzo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/03/media_tension_4_marzo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/03/media_tension_bombeo_comunitario_marzo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/03/uso_redes_marzo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/03/comercializacion_marzo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/03/ap_managua_marzo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/03/ap_chinandega_marzo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/03/ap_san_juan_sur_marzo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/03/ap_resto_municipios_marzo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/03/servicios_comerciales_marzo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/03/generacion_distribuida_marzo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/04/pliego_tarifa_social_energia_comercializacion_abril22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/04/pliego_tarifa_social_alumbrado_publico_abril22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/04/baja_tension_1_abril22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/04/baja_tension_2_abril22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/04/baja_tension_bombeo_comunitario_abril22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/04/media_tension_3_abril22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/04/media_tension_4_abril22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/04/media_tension_bombeo_comunitario_abril22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/04/uso_redes_abril22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/04/comercializacion_abril22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/04/ap_managua_abril22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/04/ap_chinandega_abril22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/04/ap_san_juan_sur_abril22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/04/ap_resto_municipios_abril22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/04/servicios_comerciales_abril22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/04/generacion_distribuida_abril22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/05/pliego_tarifa_social_energia_comercializacion_mayo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/05/pliego_tarifa_social_alumbrado_publico_mayo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/05/baja_tension_1_mayo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/05/baja_tension_2_mayo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/05/baja_tension_bombeo_comunitario_mayo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/05/media_tension_3_mayo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/05/media_tension_4_mayo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/05/media_tension_bombeo_comunitario_mayo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/05/uso_redes_mayo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/05/comercializacion_mayol22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/05/ap_managua_mayo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/05/ap_chinandega_mayo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/05/ap_san_juan_sur_mayo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/05/ap_resto_municipios_mayo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/05/servicios_comerciales_mayo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/05/generacion_distribuida_mayo22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/06/pliego_tarifa_social_energia_comercializacion_junio22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/06/pliego_tarifa_social_alumbrado_publico_junio22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/09/pliego_tarifa_social_energia_comercializacion_septiembre22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/09/pliego_tarifa_social_alumbrado_publico_septiembre22.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/07/pliego_tarifa_social_energia_comercializacion_julio22-2.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/07/pliego_tarifa_social_alumbrado_publico_julio22-2.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/07/baja_tension_1_julio22-3.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/07/baja_tension_2_julio22-3.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/07/baja_tension_bombeo_comunitario_julio22-3.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/07/media_tension_3_julio22-3.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/07/media_tension_4_julio22-3.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/07/media_tension_bombeo_comunitario_julio22-3.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/07/uso_redes_julio22-2.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/07/comercializacion_julio22-3.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/07/ap_managua_julio22-3.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/07/ap_chinandega_julio22-3.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/07/ap_san_juan_sur_julio22-3.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/07/ap_resto_municipios_julio22-3.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/07/servicios_comerciales_julio22-2.pdf',
        'https://www.ine.gob.ni/wp-content/uploads/2022/07/generacion_distribuida_julio22-3.pdf']

    date_limit = "2012"
    last_iteration = False
    local = "local_nicaragua"
    
    for pdf in urls_pdfs:
        pdf_link = pdf
    
        valor, mes_year, extension, link_title, last_iteration = \
            verifica_link(pdf_link, date_limit, local, last_iteration)
    
        if last_iteration:
            break
    
        if not valor:
            continue


### ---------------------------------------------------------------------
#   Llamado de funciones a probar
### ---------------------------------------------------------------------

rutina_ver_links()
