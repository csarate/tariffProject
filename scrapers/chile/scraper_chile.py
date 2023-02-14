import calendar
import datetime
import re
import time

import numpy as np
import pandas as pd
import requests

from ..utils_test import util


# ------------------------------------------------------------------
# Diccionario de Tarifas a convertir
# ------------------------------------------------------------------
def cargos_validos():
    dicc_c = \
    {
        'Administración del servicio (1)':                                              'Administración del servicio',
        'Cargos de Transporte':                                                         'Cargos de Transporte',
        'Cargo por energía (3)':                                                        'Cargo por energía',
        'Cargo por potencia (4)':                                                       'Cargo por potencia',
        'Cargo por potencia base (5)':                                                  'Cargo por potencia base',
        'TOTAL TARIFA BASE TRAT1':                                                      'Total Tarifa Base TRAT1',
        'Cargos por demanda máxima de potencia':                                        'Cargo por demanda potencia'
    }
    return dicc_c


# ------------------------------------------------------------------
# Diccionario de Composiciones a convertir
# ------------------------------------------------------------------
def composicion_validos():
    dicc_c = {
            'Cargo por energía':                                            'Cargo por Energia',
            'Cargo por servicio público (No incorpora recargo':             'Cargo por Servicio',
            'TOTAL TARIFA BASE BT1a':                                       'Total Tarifa Base BT1',
            'Cargos por demanda máxima de potencia (4)':                    'Cargos por demanda máxima de potencia',
            'Cargos por demanda máxima de potencia (5)':                    'Cargos por demanda máxima de potencia',
            'Cargos por potencia contratada (4)':                           'Cargos por potencia contratada',
            'Cargo fijo mensual':                                           'Cargo fijo mensual',
            'Cargo por servicio público':                                   'Cargo por servicio público',
            'Cargo por uso de sistema de transmisión':                      'Cargo por uso transmisión',
            'Cargo transmisión nacional interconexión':                     'Cargo transmisión interconexión',
            'Cargo transmisión zonal sistema C':                            'Cargo transmisión zonal C',
            'Cargo transmisión zonal sistema D':                            'Cargo transmisión zonal D',
            'Cargo transmisión dedicado':                                   'Cargo transmisión dedicado',
            'Transporte de electricidad (2)':                               'Transporte de electricidad',
            'Cargo por compras de potencia':                                'Cargo por compras de potencia',
            'Cargo por potencia base en su componente de distribución':     'Cargo por potencia de distribución',
            'Cargo por potencia base en su componente':                     'Cargo por potencia de distribución',
            'Electricidad consumida (6)':                                   'Electricidad consumida',
            'Cargo por demanda máxima de potencia leída en horas de punta, en su componente de distribución':
                'Cargo por demanda de potencia leída en punta distribución',
            'Cargo por demanda máxima de potencia suministrada, en su componente de distribución':
                'Cargo por demanda potencia suministrada distribución',
            'Cargo por potencia contratada presente en punta':              'Cargo por potencia en punta',
            'Cargo por potencia contratada parcialmente presente en punta': 'Cargo por potencia fuera de punta',
            'Electricidad consumida (6) (Se obtiene':                       'Cargo Total BT1',
            'Cargo por demanda máxima de potencia':                         'Cargo por demanda de potencia'
    }
    return dicc_c


def tarifas_validas():
    dicc_u = \
        {
            "BT1": True, "TRBT2": True, "TRBT3": True, "TRAT1": True, "TRAT2": True, "TRAT3": True, "BT2":   True,
            "BT3": True, "BT4.1": True, "BT4.2": True, "BT4.3": True, "BT5": True, "AT2": True, "AT3": True,
            "AT4.1": True, "AT4.2": True, "AT4.3": True, "AT5":   True
        }
    return dicc_u


def valores_busqueda():
    listas_valores = ["BT1", "TRBT2", "TRBT3", "TRAT1", "TRAT2", "TRAT3", "BT2", "BT3", "BT4.1", "BT4.2", "BT4.3",
                      "BT5", "AT2", "AT3", "AT4.1", "AT4.2", "AT4.3", "AT5", "igual a 500", "igual a 1000",
                      "igual a 5000", "Superior a 5000"]

    return listas_valores


# ---------------------------------------------
# Diccionario de Rangos de Consumo a convertir
# ---------------------------------------------
def rangos_validos():
    dicc = \
        {
            'T1': ['0-200 kWh'],
            'T2': ['201-210 kWh'],
            'T3': ['211-220 kWh'],
            'T4': ['221-230 kWh'],
            'T5': ['231-240 kWh'],
            'T6': ['241-350 kWh'],
            'FET': ['351-500 kWh', '501-1000 kWh', '1001-5000 kWh', '5001-999999 kWh']
        }
    return dicc


# ---------------------------------------------
# Diccionario de Rangos de Consumo a convertir
# ---------------------------------------------
def rangos_FET():
    dicc = \
        {

            'igual a 500': '351-500 kWh',
            'igual a 1000': '501-1000 kWh',
            'igual a 5000': '1001-5000 kWh',
            'Superior a 5000': '5001-999999 kWh'
        }
    return dicc


# -----------------------------
# Diccionario de Recargos FET
# -----------------------------
def recargos_FET():
    dicc = \
        {

            '351-500 kWh': 0.811,
            '501-1000 kWh': 1.825,
            '1001-5000 kWh': 2.535,
            '5001-999999 kWh': 2.839
        }
    return dicc

# ------------------------------------------------------------------
# Diccionario de Nombres de PDFs no validos
# ------------------------------------------------------------------
def nombres_no_validos():
    dicc_n = {
        "PEAJES": True,
        "FLEXIBLES": True,
        "SERVICIOS": True
    }
    return dicc_n


# ------------------------------------------------------------------
# Descarga y grabación de CSVs
# ------------------------------------------------------------------
def arma_csv(data, path, local, pais, tariff_link, link_title, pdf_link, anio_mes):
    pais = pais.capitalize()
    beginning_time = time.time()

    # ----------------------------------------------------------
    # Armado del dataframe con las tablas del PDF con PDFPlumber
    # ----------------------------------------------------------
    pdf = util.read_pdfplumber(path)
    pages = pdf.pages
    lista_tar = list()
    lista_valores = valores_busqueda()
    lista_rangos = rangos_validos()
    rangos_fet = rangos_FET()
    recarg_fet = recargos_FET()
    lista_csv = list()

    # -------------------------------------
    # Barrido de Datos Iniciales del PDF
    # -------------------------------------

    for p, page in enumerate(pages):
        texto = page.extract_text(y_tolerance=0)
        lista_lit = []
        lista_lit = texto.split("\n")
        lista_code = []
        lista_tar =[]
        page_number = page.page_number
        for linea in lista_lit:
            for c, code in enumerate(lista_valores):
                if code in linea:
                    if 'BT1a' in linea or 'BASE TRAT1' in linea:
                        break
                    if rangos_fet.get(code):
                        valor_fet = rangos_fet.get(code)
                        numero = [s for s in re.findall(r'-?\d+\,?\d*', linea)]
                        valor = float(numero[-1].replace(',','.'))
                        recarg_fet[valor_fet] = valor
                    else:
                        lista_code.append(code)
                        break

        # ----------------------------------------------------------
        # Armado del dataframe con las tablas del PDF con Tabula
        # ----------------------------------------------------------
        try:
            dfs = util.read_pdftabula(path, **{"lattice": True, "multiple_tables": False, "guess": True,
                                                "pages": page_number, "encoding": 'utf-8', "relative_area": True,
                                                'pandas_options': {'skiprows': 1, 'header': None}})

            if len(dfs.columns) > 39:
                dfs = dfs.drop([0], axis=1)

            dfs = dfs.rename({0: 'Cargo_boleta', 1: 'RED', 2: 'ETR', 3: 'Cargo_tarifario', 4: 'Unidad',
                        5: 'Neto_1', 6: 'IVA_1', 7: 'Neto_2', 8: 'IVA_2', 9: 'Neto_3', 10: 'IVA_3', 11: 'Neto_4',
                        12: 'IVA_4', 13: 'Neto_5', 14: 'IVA_5', 15: 'Neto_6', 16: 'IVA_6',
                        17: 'Neto_7', 18: 'IVA_7', 19: 'Neto_8', 20: 'IVA_8', 21: 'Neto_9', 22: 'IVA_9',
                        23: 'Neto_10', 24: 'IVA_10', 25: 'Neto_11', 26: 'IVA_11', 27: 'Neto_12', 28: 'IVA_12',
                        29: 'Neto_13', 30: 'IVA_13', 31: 'Neto_14', 32: 'IVA_14', 33: 'Neto_15', 34: 'IVA_15',
                        35: 'Neto_16', 36: 'IVA_16', 37: 'Neto_17', 38: 'IVA_17'},
                        axis=1).sort_index(axis=1).astype('string')

            columnas = ['Cargo_boleta', 'RED', 'ETR', 'Cargo_tarifario', 'Unidad',
                        'Neto_1', 'IVA_1', 'Neto_2', 'IVA_2', 'Neto_3', 'IVA_3', 'Neto_4', 'IVA_4', 'Neto_5', 'IVA_5',
                        'Neto_6', 'IVA_6', 'Neto_7', 'IVA_7', 'Neto_8', 'IVA_8', 'Neto_9', 'IVA_9', 'Neto_10', 'IVA_10',
                        'Neto_11', 'IVA_11', 'Neto_12', 'IVA_12', 'Neto_13', 'IVA_13', 'Neto_14', 'IVA_14',
                        'Neto_15', 'IVA_15', 'Neto_16', 'IVA_16', 'Neto_17', 'IVA_17']

            dfs = dfs.reindex(columns=columnas)
            dfs = dfs.replace({None: np.nan})
            dfs = dfs.replace({'': np.nan})
            dfs = dfs.dropna(how='all')
            dfs = dfs.rename_axis(index=None)
            dfs.reset_index(drop=True, inplace=True)

            columnas = ['Cargo_boleta', 'RED', 'ETR', 'Cargo_tarifario', 'Unidad',
                        'Neto_1', 'Neto_2', 'Neto_3', 'Neto_4', 'Neto_5', 'Neto_6', 'Neto_7', 'Neto_8', 'Neto_9',
                        'Neto_10', 'Neto_11', 'Neto_12', 'Neto_13', 'Neto_14', 'Neto_15', 'Neto_16', 'Neto_17',
                        'IVA_1', 'IVA_2', 'IVA_3', 'IVA_4', 'IVA_5', 'IVA_6', 'IVA_7', 'IVA_8', 'IVA_9',
                        'IVA_10', 'IVA_11', 'IVA_12', 'IVA_13', 'IVA_14', 'IVA_15', 'IVA_16', 'IVA_17']
            dfs = dfs.reindex(columns=columnas)

            columnas = ['Cargo_boleta', 'RED', 'ETR', 'Cargo_tarifario', 'Unidad',
                        'Neto_1', 'Neto_2', 'Neto_3', 'Neto_4', 'Neto_5', 'Neto_6', 'Neto_7', 'Neto_8', 'Neto_9',
                        'Neto_10', 'Neto_11', 'Neto_12', 'Neto_13', 'Neto_14', 'Neto_15', 'Neto_16', 'Neto_17']

            df_tabla = pd.DataFrame(columns=columnas)

            ind = 0
            first = True
            for t1 in range(len(dfs)):
                if dfs.loc[t1, 'Cargo_boleta'] is np.nan and dfs.loc[t1, 'Cargo_tarifario'] is np.nan:
                    loclist = dfs.to_numpy().tolist()
                    loclist = loclist[t1]
                    loclist = [str(x) for x in loclist]
                    loclist = [x for x in loclist if x != 'nan']
                    lista_tar = [lista_code[ind], loclist]
                    # lista_tar.append(lista)
                    ind += 1

                if dfs.loc[t1, 'Cargo_tarifario'] is np.nan:
                    if first:
                        first = False
                        continue
                    else:
                        break

                if dfs.loc[t1, 'Cargo_boleta'] is not np.nan:
                    if 'Cargo en Boleta/Factura' in dfs.loc[t1, 'Cargo_boleta']:
                        continue
                    if 'Cargo por potencia adicional' in dfs.loc[t1, 'Cargo_boleta']:
                        break

                if dfs.loc[t1, 'Cargo_boleta'] is not np.nan:
                    boleta = dfs.loc[t1, 'Cargo_boleta']
                    boleta = boleta.split('\r')[0]

                dfs.loc[t1, 'Cargo_boleta'] = boleta
                cargo = dfs.loc[t1, 'Cargo_tarifario']
                cargo = cargo.split('\r')[0]
                dfs.loc[t1, 'Cargo_tarifario'] = cargo
                etr = dfs.loc[t1, 'ETR']
                etr = "".join(etr.split())
                dfs.loc[t1, 'ETR'] = etr

                if dfs.loc[t1, 'IVA_16'] is np.nan and dfs.loc[t1, 'RED'] is not np.nan:
                    for r in range(17, 1, -1):
                        campo1 = '"Neto_' + str(r - 1) + '"'
                        campo2 = '"Neto_' + str(r) + '"'
                        expresion = 'dfs.loc[t1, ' + campo2 + '] = dfs.loc[t1, ' + campo1 + ']'
                        exec(expresion)

                    dfs.loc[t1, 'Neto_1'] = dfs.loc[t1, 'Cargo_tarifario']
                    if 'BT_' in dfs.loc[t1, 'RED']:
                        dfs.loc[t1, 'Cargo_tarifario'] = dfs.loc[t1 - 1, 'Cargo_tarifario']
                        dfs.loc[t1, 'Unidad'] = dfs.loc[t1 - 1, 'Unidad']
                    else:
                        dfs.loc[t1, 'Cargo_tarifario'] = dfs.loc[t1, 'RED']
                        dfs.loc[t1, 'Unidad'] = dfs.loc[t1, 'ETR']
                        dfs.loc[t1, 'RED'] = dfs.loc[t1 - 1, 'RED']
                        dfs.loc[t1, 'ETR'] = dfs.loc[t1 - 1, 'ETR']

                if dfs.loc[t1, 'Neto_17'] is np.nan and dfs.loc[t1, 'RED'] is not np.nan:
                    for r in range(17, 1, -1):
                        campo1 = '"Neto_' + str(r - 1) + '"'
                        campo2 = '"Neto_' + str(r) + '"'
                        expresion = 'dfs.loc[t1, ' + campo2 + '] = dfs.loc[t1, ' + campo1 + ']'
                        exec(expresion)

                    dfs.loc[t1, 'Neto_1'] = dfs.loc[t1, 'Cargo_tarifario']
                    if 'BT_' in dfs.loc[t1, 'RED']:
                        dfs.loc[t1, 'Cargo_tarifario'] = dfs.loc[t1 - 1, 'Cargo_tarifario']
                        dfs.loc[t1, 'Unidad'] = dfs.loc[t1 - 1, 'Unidad']
                    else:
                        dfs.loc[t1, 'Cargo_tarifario'] = dfs.loc[t1, 'RED']
                        dfs.loc[t1, 'Unidad'] = dfs.loc[t1, 'ETR']
                        dfs.loc[t1, 'RED'] = dfs.loc[t1 - 1, 'RED']
                        dfs.loc[t1, 'ETR'] = dfs.loc[t1 - 1, 'ETR']

                if dfs.loc[t1, 'Cargo_boleta'] is np.nan and dfs.loc[t1, 'Cargo_tarifario'] is not np.nan:
                    dfs.loc[t1, 'Cargo_boleta'] = dfs.loc[t1 - 1, 'Cargo_boleta']
                    dfs.loc[t1, 'RED'] = dfs.loc[t1 - 1, 'RED']
                    dfs.loc[t1, 'ETR'] = dfs.loc[t1 - 1, 'ETR']
                    cargo = dfs.loc[t1, 'Cargo_tarifario']
                    cargo = cargo.split('\r')[0]
                    dfs.loc[t1, 'Cargo_tarifario'] = cargo
                    etr = dfs.loc[t1, 'ETR']
                    etr = "".join(etr.split())
                    dfs.loc[t1, 'ETR'] = etr

                df_tabla = df_tabla.append(dfs.iloc[t1, 0:22])

            df_tabla = df_tabla.rename_axis(index=None)
            df_tabla.reset_index(drop=True, inplace=True)
            lista_csv = pagina_parser(df_tabla, pais, tariff_link, pdf_link, link_title,
                                        anio_mes, lista_tar, local, lista_csv)
            if len(lista_code) == ind:
                break

        except:
            util.log(local, 'Error de formato del PDF {}'.format(link_title))
            print('Error de formato del PDF {}'.format(link_title))
            return False

    valor, last_iteration, lis_data = grabacion_archivos(data, lista_csv, pais, tariff_link, link_title,
                                                        pdf_link, anio_mes, beginning_time, local)
    return valor, last_iteration, lis_data


# ------------------------------------
# Parseado de datos para grabación
# ------------------------------------
def pagina_parser(df_tabla, pais, tariff_link, pdf_link, link_title, anio_mes, lista_tar,local, lista_csv):
    pd.set_option('float_format', '{:.2f}'.format)
    tariff_base = tarifas_validas()
    cargo_base = cargos_validos()
    recarg_fet = recargos_FET()
    rang_base = rangos_validos()
    compo_base = composicion_validos()
    tariff_code = ''
    utility = "ENEL"

    columnas = ['Cargo_boleta', 'RED', 'ETR', 'Cargo_tarifario', 'Unidad',
                'Neto_1', 'Neto_2', 'Neto_3', 'Neto_4', 'Neto_5', 'Neto_6', 'Neto_7', 'Neto_8', 'Neto_9',
                'Neto_10', 'Neto_11', 'Neto_12', 'Neto_13', 'Neto_14', 'Neto_15', 'Neto_16', 'Neto_17']

    try:
        tariff_ori = lista_tar[0][:]
        loclist = lista_tar[1][:]
        for l2 in range(len(loclist)):
            region = loclist[l2]
            first = True
            for t1 in range(len(df_tabla)):
                if 'Cargo en Boleta' in df_tabla.loc[t1, 'Cargo_boleta'] and first:
                    first = False
                    continue

                tariff_type = df_tabla.loc[t1, 'Cargo_boleta']
                red = df_tabla.loc[t1, 'RED']
                etr = df_tabla.loc[t1, 'ETR']
                cargo = df_tabla.loc[t1, 'Cargo_tarifario']
                unit = df_tabla.loc[t1, 'Unidad']
                neto = 'Neto_' + str(l2 + 1)
                charge = df_tabla.loc[t1, neto]
                charge = float(charge.replace(',','.'))

                if 'T1-T6' in etr:
                    lis_red = red.split('\r')
                    for r, red in enumerate(lis_red):
                        if compo_base.get(cargo):
                            charge_type = compo_base.get(cargo)
                        else:
                            print('Not Found Component ' + cargo)

                        for e in range(1, 7):
                            tx = 'T' + str(e)
                            etr = tx

                            if tariff_base.get(tariff_ori):
                                tariff_code = tariff_ori + ' - ' + red
                            else:
                                print('Not Found Tariff Code ' + tariff_ori)

                            component = rang_base.get(etr)
                            for c1, com in enumerate(component):
                                compo = etr + ' - ' + com
                                lista = [pais, pdf_link, link_title, tariff_code, compo, unit, utility,
                                        charge, anio_mes, "CLP", tariff_type, charge_type, region]
                                lista_csv.append(lista)

                        if 'SERVICIO' in cargo.upper():
                            component = rang_base.get('FET')
                            if tariff_base.get(tariff_ori):
                                tariff_code = tariff_ori + ' - ' + red

                            for c1, com in enumerate(component):
                                if recarg_fet.get(com):
                                    charge = charge + recarg_fet.get(com)
                                compo = 'FET' + ' - ' + com
                                lista = [pais, pdf_link, link_title, tariff_code, compo, unit, utility,
                                        charge, anio_mes, "CLP", tariff_type, charge_type, region]
                                lista_csv.append(lista)
                else:
                    if tariff_base.get(tariff_ori):
                        tariff_code = tariff_ori + ' - ' + red
                    if compo_base.get(cargo):
                        charge_type = compo_base.get(cargo)
                    else:
                        print('Not Found Component ' + cargo)

                    component = rang_base.get(etr)
                    for c1, com in enumerate(component):
                        compo = etr + ' - ' + com
                        lista = [pais, pdf_link, link_title, tariff_code, compo, unit, utility,
                                charge, anio_mes, "CLP", tariff_type, charge_type, region]
                        lista_csv.append(lista)
    except:
        util.log(local, 'Error de parseo {}'.format(pdf_link))
        print('Error de parseo {}'.format(pdf_link))
        return False

    return lista_csv


# ************************
# Grabación de archivos
# ***********************
def grabacion_archivos(data, lista_csv, pais, tariff_link, link_title, pdf_link,
                        anio_mes, beginning_time, local):
    df_csv = pd.DataFrame(lista_csv, columns=['Country', 'Document_link', 'PDF_Name', 'Tariff_code', 'Component',
                                                'Unit', 'Utility', 'Charge', 'Date_start', 'Date_end', '$local',
                                                'Tariff_type', 'Charge_type'])

    nombre = "chile_tariff_asep_" + anio_mes
    nombre = f"{nombre}.csv"

    # -----------------------------------------------------------------
    #  Lectura de CSVs para creacion o inserción y posterior grabación
    # -----------------------------------------------------------------

    data_csv = pd.DataFrame(columns=['Country', 'Document_link', 'PDF_Name', 'Tariff_code', 'Component',
                                        'Unit', 'Utility', 'Charge', 'Date_start', 'Date_end', '$local', 'Tariff_type'])
    data_csv = util.read_csv_s3(data_csv, local, nombre,
                                **({"sep": ',', "decimal": ".", "encoding": 'utf-8', "index": False}))
    data_csv = data_csv.append(df_csv)
    # data_csv = data_csv.sort_values(by=['Tariff_code'])

    util.dump_files_s3_v2(data_csv.to_csv(index=False), nombre, local)

    util.log(local, "CSV {} generated".format(nombre))
    print(local, "CSV {} generated".format(nombre))
    print('Execution time Armado de CSV = ' + str(round(time.time() - beginning_time)), 'sec')

    lis_data = [tariff_link, link_title, pdf_link, anio_mes, 0, pais, local, "", "", ""]
    data.append(lis_data)

    valor = True
    last_iteration = False
    return valor, last_iteration, data


# -------------------------------------------------------------------------------
# Clase para iterar recursivamente en los diferentes niveles de opciones del html
# -------------------------------------------------------------------------------
class lista_recursiva:

    def __init__(self, s, soup, base_link, last_iteration, lista_urls, urls_pdfs, inicio, local, valor):
        self.s = s
        self.soup = soup
        self.base_link = base_link
        self.last_iteration = last_iteration
        self.lista_urls = lista_urls
        self.urls_pdfs = urls_pdfs
        self.inicio = inicio
        self.local = local
        self.valor = valor

    # -------------------------------------------------------------------
    # Recursividad y control en links encontrados
    # -------------------------------------------------------------------
    def loop_list_links(self):
        for tl in range(self.inicio, len(self.lista_urls)):
            self.urls_pdfs = []
            no_val = nombres_no_validos()
            for t, tar in enumerate(self.lista_urls):
                esta = False
                for word in no_val:
                    if word in tar.upper():
                        esta = True
                        break
                if esta:
                    continue

                if ".pdf" in tar:
                    if 'https:' not in tar:
                        tar = self.base_link + tar

                    tar = tar.replace('%20', ' ')
                    self.urls_pdfs.append(tar)

                    # self.valor, self.soup, response, status_code = util.status_code_url(self.s, tar, self.local)
                    #
                    # if status_code != 200:
                    #     util.log(self.local, 'Not logged on the right url = {}'.format(self.lista_urls[tl]))
                    #     self.valor = False
                    #     return self.valor, self.urls_pdfs, self.last_iteration

        return self.valor, self.urls_pdfs


# -------------------------------------------
# Main de el Scraper de chile
# -------------------------------------------
def chile_asep_tariff(output_path, event: dict, local):
    pais = local.split("_")[-1]
    date_limit = "2012"

    util.set_event(event)

    try:
        # create dir to save results if it is not created
        util.create_s3dirs()

        data, past_links = util.lectura_links_procesados(output_path, local)
        util.log(local, 'Started the scraping of Chile tariffs')

        try:
            # urls
            base_link = 'https://www.enel.cl'
            next_link = 'https://www.enel.cl/es/clientes/tarifas-y-regulacion/tarifas.html'
            # counter for the already visited links
            previous_items = len(past_links)

            # list of the past_links
            list_of_links = []

            beginning_time = time.time()
            last_iteration = False

            while not util.execution_time_out() or last_iteration:
                s = util.open_session(requests)

                valor, soup, response, status_code = util.status_code_url(s, next_link, local)

                if not valor:
                    return False

                table_with_links = soup.find_all('a', attrs={'href': True})
                # table_with_links = soup.find_all("div", {"class": "fusion-text"})

                # check if the table has the links
                if not table_with_links:
                    util.log(local, 'Not logged on the right url = {}'.format(next_link))
                    return False

                links = []
                for tag in table_with_links:
                    link = tag.get("href").strip("=")
                    links.append(link)

                lista_urls = []
                url_ant = None
                for tar in links:
                    if '.pdf' in tar and url_ant is None:
                        url_ant = base_link + tar
                        lista_urls.append(url_ant)
                    else:
                        url_ant = None


                list_of_links.extend(lista_urls)

                new_items = len(list_of_links)

                urls_pdfs = []

                # iteration over links to the tariff tables per period
                inicio = 0

                deep_links = lista_recursiva(
                    s,
                    soup,
                    base_link,
                    last_iteration,
                    list_of_links,
                    urls_pdfs,
                    inicio,
                    local,
                    valor
                )

                valor, urls_pdfs = deep_links.loop_list_links()

                if not valor:
                    return False

                last_iteration = False
                pdf_ant = np.nan
                mes_dict = util.MonthDict()
                for pdf in urls_pdfs:
                    pdf_link = pdf

                    if pdf_link in past_links:
                        continue

                    # -----------------
                    # Grabación PDFs
                    # -----------------
                    link_title = pdf_link.split('/')[-1]
                    link_title = link_title.strip()
                    tariff_link = pdf_link[0:(len(pdf_link)-len(link_title))]
                    year = link_title.strip().split(" ")[-1].split(".")[0]

                    if len(year) == 2:
                        year = "20" + year

                    if date_limit < year:
                        valor, path, status_code = util.dump_pdf(s, pdf_link, link_title, local)
                    else:
                        last_iteration = True
                        break

                    if status_code != 200:
                        continue

                    util.log(local, "PDF {} gathered".format(link_title))
                    print(local, "PDF {} gathered".format(link_title))

                    # ---------------
                    # Armado de CSVs
                    # ---------------
                    try:
                        day = '01'
                        words_list = link_title.strip().split(" ")
                        for word in words_list:
                            if word.isdigit():
                                if len(word) == 2:
                                    year = "20" + word
                                    continue
                                if len(word) == 4:
                                    year = word
                                    continue
                            if mes_dict.get(word):
                                month = mes_dict.get(word)
                                continue

                        year_month = year + "-" + month
                        year_month_day = year_month + "-" + day
                        fecha = datetime.datetime.strptime(year_month_day, "%Y-%B-%d")
                        mes = fecha.strftime('%m')
                        anio = fecha.strftime('%Y')
                        anio_mes = anio + "-" + mes
                        ult_mes = anio + "-" + mes + "-" + str(calendar.monthrange(int(year), int(mes))[1])
                        date_ori = fecha.strftime('%Y-%m-%d')
                        date_end = datetime.datetime.strptime(ult_mes, '%Y-%m-%d').strftime('%Y-%m-%d')
                    except:
                        util.log(local, 'Did not found a date format on the link = {}'.format(tariff_link))
                        print('Did not found a date format on the link = {}'.format(tariff_link))
                        valor = False
                        return valor,

                    if year <= date_limit:
                        valor = False
                        last_iteration = True
                        return valor, last_iteration, data

                    valor, last_iteration, data = arma_csv(data, path, local, pais, tariff_link,
                                                            link_title, pdf_link, anio_mes)

                    if not valor:
                        last_iteration = True
                        break

                    if last_iteration:
                        break

                    util.dump_files_s3(data, output_path, local)

                last_iteration = True
                break

            util.dump_files_s3(data, output_path, local)
            util.log(local, "All links gathered from " + base_link)

            # after exiting the loop check if it was last iteration and
            # return
            if last_iteration:
                util.log(local, 'Ended with the time of scraping of Chile tariff tables')
                return True

        except:
            return False
        finally:
            # Move the save to this point to improve performance avoiding write to S3 per each item in the list
            # save the new processed link in data
            util.dump_files_s3(data, output_path, local)

    except:
        return False
