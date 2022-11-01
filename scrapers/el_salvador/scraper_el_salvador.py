import calendar
import os
import re
import time
import numpy as np
import pdfplumber as pl
import pandas as pd
import requests
import datetime

from requests.adapters import HTTPAdapter, Retry
from ..utils_test import util

# ------------------------------------------------------------------
# Diccionario de Pliego Tarifario
# ------------------------------------------------------------------
def pliegos_validos():
    dict_p = \
        {'Index pliego tarif': 'Pliego Tarifario',
         'Index Pliegos': 'Pliego Tarifario',
         'pliego tarifario': 'Pliego Tarifario',
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
    
    try:
        mes_year = link.split("-")[-1].split("'")[0].split(".")[0].replace("%20", " ")
        # mes = re.findall('[a-z]+', mes_year, flags=re.IGNORECASE)[0]
        year = re.findall('[0-9]+', mes_year)[-1]
        extension = link.split("/")[-1].split(".")[-1].split(" ")[0]
        link_title = link.split("/")[-1].replace('%20', ' ')
        link_title = link_title.strip()
    except:
        valor = False
        return valor, mes_year, extension, link_title, last_iteration

    pliegos = pliegos_validos()
    for p in pliegos.keys():
        if p in link_title:
            valor = True
            break
        valor = False

    if not valor:
        util.log(local, 'Did not found a price list format on the link = {}'.format(link))
        print('Did not found a price list format on the link = {}'.format(link))
        return valor, mes_year, extension, link_title, last_iteration
    
    if year == "2016":
        print ("Año a verificar " + mes_year)
    
    if year < date_limit:
        valor = False
        last_iteration = True
        return valor, mes_year, extension, link_title, last_iteration
    
    lista = mes_year.split(" ")
    for mes in lista:
        if mes_dict.get(mes):
            valor = True
            break
        valor = False
    
    if not valor:
        util.log(local, 'Did not found a date format on the link = {}'.format(link))
        print(
            'Did not found a date format on the link = {}'.format(link)
        )
        return valor, mes_year, extension, link_title, last_iteration
    
    
    
    month = mes_dict[mes]
    month_year = month + "-" + year
    mes_year = datetime.datetime.strptime(month_year, "%B-%Y").strftime("%m-%Y")
    valor = True
    return valor, mes_year, extension, link_title, last_iteration


# ------------------------------------------------------------------
# Arma el df desde texto porque la tabla viene inconsistente
# ------------------------------------------------------------------
def arma_desde_pagina(lista):
    df_lista = []
    
    linea_cargo = ""
    for f, fila in enumerate(lista):
        if "SUPER" in fila or "ELECTRICIDAD" in fila or "TARIFAS" in fila or "TARIFA RES" in fila \
                or "PRECIOS" in fila or "acuerdo" in fila:
            continue
        if "VIGENTES" in fila or "I." in fila or "II." in fila or "TENSION" in fila or "Tarifa" in fila \
                or "Bloque" in fila or "Uso" in fila or "USO" in fila or "Alumbrado" in fila or "ALUMBRADO" in fila:
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
        if 'CCaarrggoo' in fila:
            fila = 'Cargo de Distribución:'
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
            filas = filas.replace("-mes", "-mes\n ")
            filas = filas.replace("Cargo ", "Cargo&")
            filas = filas.replace(" al Cliente", "&al&Cliente ")
            filas = filas.replace(" en ", "&en&")
            filas = filas.replace("gía:","gía:\n")
            filas = filas.replace("kWh","kWh\n")
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


# ------------------------------------------------------------------
# Descarga y grabación de CSVs
# ------------------------------------------------------------------
def arma_csv(link, path, date_ori, local):
    pais = "El Salvador"
    title = link[1]
    tariff_link = link[0]
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
    
    for t2 in range(len(df_tabla)):
        c = df_tabla.loc[t2, 'Columna']
        v1 = df_tabla.loc[t2, 'Valor1']
        v2 = df_tabla.loc[t2, 'Valor2']
        if c is np.nan and v1 is np.nan and v2 is not np.nan:
            df_tabla.loc[t2, 'Valor1'] = df_tabla.loc[t2+1, 'Valor1']
            df_tabla.loc[t2 + 1, 'Valor1'] = np.nan

    df_tabla = df_tabla.dropna(how='all')
    df_tabla.reset_index(drop=True, inplace=True)
    
   
    # Parseado de datos con devolución de dataframe para grabación
    df_csv, anio_mes, date_ori = pdf_tariff_parser(df_tabla, pais, tariff_link, date_ori)
    
    nombre = "elsalvador_tariff_siget_" + anio_mes
    nombre = f"{nombre}.csv"
    
    util.log(local, "CSV gathered " + nombre)
    df_csv.to_csv((os.path.join(util.setup_dic[local], nombre)), encoding='utf-8', sep=',')
    return date_ori


# ----------------------------------------------------------------
# Parseado de datos con devolución de dataframe para grabación
# ----------------------------------------------------------------
def pdf_tariff_parser(df_tabla, pais, tariff_link, date_ori):
    lista_csv = []
    first = True
    lista_2 = []
    lista_4 = []
    tariff_code = ''
    tariff_base = ''
    mes_dict = util.MonthDict()
    
    for t2 in range(len(df_tabla)):
        l = df_tabla.loc[t2, 'Columna']
        if l is not np.nan:
            if "VIGENTES" in l:
                if "DEL1" in l:
                    l = l.replace("DEL1", "DEL 1")
                words = l.split(" ")
                for w, word in enumerate(words):
                    if word.isdigit():
                        if len(word) == 1:
                            day = "0"+word
                            continue
                        if len(word) == 2:
                            day = word
                            continue
                        if len(word) == 4:
                            year = word
                            continue
                    if mes_dict.get(word):
                        month = mes_dict.get(word)
                year_month = year + "-" + month
                year_month_day = year_month + "-" + day
                fecha = datetime.datetime.strptime(year_month_day, "%Y-%B-%d").strftime("%Y-%m-%d")
                anio_mes = datetime.datetime.strptime(year_month, "%Y-%B").strftime("%Y-%m")
                continue
            if "I." in l or "II." in l or "III." in l:
                tariff_base = l
                continue
            if "BAJA TENSION" in l:
                if "BAJA TENSION CON" in l:
                    first = False
                else:
                    first = True
                    tariff_code = tariff_base + " " + l
                    tariff_base = tariff_code
                    continue
            if "MEDIA TENSION" in l:
                first = False
            if "Tarifa" in l:
                continue
            if "Bloque 1" in l:
                tariff_code = tariff_code + " " + l
                continue
            if first:
                lista_4 = arma_dato_fijos(df_tabla, t2)
                first = False
                continue
            else:
                lista_csv = arma_tabla(lista_2, lista_4, pais, tariff_link, tariff_code, fecha, date_ori, lista_csv)
                if "Bloque" in l or "Uso" in l or "Alumbrado" in l or "USO" in l or "ALUMBRADO" in l:
                    tariff_code = tariff_base + " " + l
                if "BAJA TENSION" in l:
                    tariff_code = tariff_base + " " + l
                if "MEDIA TENSION" in l:
                    tariff_code = tariff_base + " " + l

                lista_4 = []
                lista_2 = []
                first = True
                continue
        else:
            for v in range(1, 9):
                valorn = 'Valor' + str(v)
                lista_2.append(df_tabla.loc[t2, valorn])
                continue
            continue
            
    lista_csv = arma_tabla(lista_2, lista_4, pais, tariff_link, tariff_code, fecha, date_ori, lista_csv)
        
    df_csv = pd.DataFrame(lista_csv, columns=['Country', 'Document_link', 'Tariff_code', 'Component',
                                              'Unit', 'Utility', 'Charge', 'Date_start', 'Date_end', '$local'])
    return df_csv, anio_mes, fecha


# ------------------------------------------------------------------
# Estructuración Textos en columnas
# ------------------------------------------------------------------
def arma_dato_fijos(df_tabla, t2):
    lista_3 = df_tabla.loc[t2, 'Columna'].replace(' US$', '#US$').split("\n")
    lista_4 = []
    for k in range(len(lista_3)):
        if "#" in lista_3[k]:
            lista_4.append(lista_3[k].split("#"))
        else:
            lista_4.append(lista_3[k])
    lista_d = []
    lista_4 = util.desanidar_lista(lista_4, lista_d)
    return lista_4


# ------------------------------------------------------------------
# Estructuración de los datos parseados
# ------------------------------------------------------------------
def arma_tabla(lista_2, lista_4, pais, tariff_link, tariff_code, fecha, date_ori, lista_csv):
    for l2, valor in enumerate(lista_2):
        try:
            if "es" in valor:
                lista_2[l2] = valor.replace("es", "")
        except:
                print("Error")
                

    listan = []
    for r in range(int(len(lista_2) / 8)):
        lista = "listan_" + str(r + 1)
        globals()[lista] = [(lista_2[r * 8:(r + 1) * 8])]
        listan.append(globals()[lista])
    
    if len(listan) == 4:
        for l1, l2, l3, l4 in zip(listan[0], listan[1], listan[2], listan[3]):
            lista_x = (l1, l2, l3, l4)
    
    if len(listan) == 5:
        for l1, l2, l3, l4, l5 in zip(listan[0], listan[1], listan[2], listan[3], listan[4]):
            lista_x = (l1, l2, l3, l4, l5)
    
    if len(listan) == 6:
        for l1, l2, l3, l4, l5, l6 in zip(listan[0], listan[1], listan[2], listan[3],
                                          listan[4], listan[5]):
            lista_x = (l1, l2, l3, l4, l5, l6)
   
    if len(listan) == 7:
        for l1, l2, l3, l4, l5, l6, l7 in zip(listan[0], listan[1], listan[2], listan[3],
                                              listan[4], listan[5], listan[6]):
            lista_x = (l1, l2, l3, l4, l5, l6, l7)

    if len(listan) == 8:
        for l1, l2, l3, l4, l5, l6, l7, l8 in zip(listan[0], listan[1], listan[2], listan[3],
                                                  listan[4], listan[5], listan[6], listan[7]):
            lista_x = (l1, l2, l3, l4, l5, l6, l7, l8)

    if len(listan) == 9:
        for l1, l2, l3, l4, l5, l6, l7, l8, l9 in zip(listan[0], listan[1], listan[2], listan[3],
                                                      listan[4], listan[5], listan[6], listan[7], listan[8]):
            lista_x = (l1, l2, l3, l4, l5, l6, l7, l8, l9)
            
    for lx in range(len(lista_x[0])):
        util = ""
        for n in range(len(lista_x)):
            if not util:
                util = lista_x[n][lx]
            else:
                util = util + "/" + lista_x[n][lx]
        
        util.strip()
        list_uti = util.split("/")
        for l in range(len(list_uti)):
            try:
                float(list_uti[l])
            except:
                utility = list_uti[l]
                inicio = 0
                continue
            else:
                charge = list_uti[l]
                for c in range(inicio, len(lista_4)):
                    if "US$" in lista_4[c]:
                        unit = str(lista_4[c])
                        inicio = c + 1
                        break
                    if "Cargo de" in lista_4[c]:
                        component_base = str(lista_4[c])
                        continue
                    component = component_base + str(lista_4[c])
            
            lista_1 = [pais, tariff_link, tariff_code, component, unit, utility, charge, fecha, date_ori, "USD"]
            lista_csv.append(lista_1)
    
    return lista_csv


# -------------------------------------------
# Main de el Scraper de El Salvador
# -------------------------------------------
def el_salvador_tariff(time_to_search, output_path, event: dict, local):
    now = datetime.datetime.now()
    anio = now.strftime('%Y')
    mes = now.strftime('%m')
    ult_mes = calendar.monthrange(int(anio), int(mes))[1]
    ult_mes = anio + "-" + mes + "-" + str(ult_mes)
    date_ori = datetime.datetime.strptime(ult_mes, '%Y-%m-%d').strftime('%Y-%m-%d')
    date_limit = "2013"
    
    util.set_event(event)
    
    try:
        # create dir to save results if it is not created
        util.create_s3dirs()
        
        data, past_links = util.lectura_links_procesados(output_path, local)
        util.log(local, 'Started the scraping of El Salvador tariffs')
        
        try:
            # urls
            base_link = 'https://www.siget.gob.sv/gerencias/electricidad/tarifas-de-electricidad/'
            # counter for the already visited links
            previous_items = len(past_links)
            
            
            # list of the past_links
            list_of_links = []
            
            beginning_time = time.time()
            last_iteration = False
            
            while (time.time() - beginning_time < time_to_search) or last_iteration:
                s = requests.Session()
                retries = Retry(
                    total=5,
                    backoff_factor=0.1,
                    status_forcelist=[500, 502, 503, 504],
                    raise_on_status=True
                )
                s.mount('http://', HTTPAdapter(max_retries=retries))
                
                valor, soup, response = util.status_code_url(s, base_link, local)
                
                if not valor:
                    return False
                
                next_link = base_link
                
                if not soup.find('table'):
                    util.log(local,'Not logged on the right url = {}'.format(next_link))
                    return False
                
                table_with_links = soup.find('table')
                
                # check if the table has the links
                if not table_with_links:
                    util.log(local,'Not logged on the right url = {}'.format(next_link))
                    return False
                
                list_of_links.extend(table_with_links.findAll('a'))
                
                new_items = len(list_of_links)
                
                # iteration over links to the tariff tables per period
                for tl in list_of_links:
                    if last_iteration:
                        break
   
                    tariff_link = tl.get('href')[:]
                    
                    valor, soup = util.status_code_url(s, tariff_link, local)
                    
                    if not valor:
                        continue
                    
                    links_pdfs = []
                    url_pdf = soup.find_all('button', class_="btn btn-primary")
                    
                    if not url_pdf:
                        util.log(local,'Not logged on the right url = {}'.format(url_pdf))
                        return False
                    
                    links_pdfs.extend(url_pdf)
                    for pdf in links_pdfs:
                        pdf_link = pdf.get('onclick')[:].split("https://")[-1].split("'")[0].replace(" ", "%20")
                        pdf_link = "https://" + pdf_link
                        
                        # if the link was already processed skip
                        if pdf_link in past_links:
                            continue

                        valor, mes_year, extension, link_title, last_iteration = \
                            verifica_link(pdf_link, date_limit, local, last_iteration)
                        
                        if last_iteration:
                            break
                            
                        if not valor:
                            continue
                            
                        # if the number of links is less than 30 the pagination reach
                        # the last page
                        previous_items = previous_items + 1
                        if (previous_items - new_items) > 30:
                            last_iteration = True
                            break

                        
                        if pdf_link != None and ".pdf" in pdf_link.lower():
                            lista = [tariff_link, link_title, pdf_link, mes_year,
                                     0, "El Salvador", local, "", "", ""]
                            valor, path = util.dump_pdf(pdf_link, link_title, local)
                            if not valor:
                                continue
                                
                            data.append(lista)
                            date_ori = arma_csv(lista, path, date_ori, local)
                            
                        util.dump_files_s3(data, output_path, local)
                        util.log(local, "PDF gathered " + link_title)
                        print('Execution time Armado de CSV = ' + str(round(time.time() - beginning_time)), 'sec')
                        beginning_time = time.time()

                if last_iteration:
                    break
                    
            util.log(local, "All links gathered from " + base_link)

            # after exiting the loop check if it was last iteration and
            # return
            if last_iteration:
                util.log(local,'Ended with the time of scraping of El Salvador tariff tables')
                return True

        except Exception as e:
            raise e
        finally:
            # Move the save to this point to improve performance avoiding write to S3 per each item in the list
            # save the new processed link in data
            util.dump_files_s3(data, output_path, local)

    except Exception as e:
        raise e
