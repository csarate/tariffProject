import datetime
import time

import numpy as np
import pandas as pd
import requests

from ..utils_test import util


# ------------------------------------------------------------------
# Diccionario de Tarifas a convertir
# ------------------------------------------------------------------
def cargos_validos():
    dict_u = {
        'Cargo Fijo': ['Cargo Fijo'],
        'Cargo Fijo (en BTS - primeros 10 kWh)': ['Cargo Fijo'],

        'Cargo por energía': ['Cargo por energía'],
        'Cargo por Sistema': ['Cargo por energía'],
        'Sistema': ['Cargo por energía'],
        'Cargo por energía (en BTS siguientes kWh)': ['Cargo por energía'],
        'Cargo por energía, excluyendo pérdidas': ['Cargo por energía'],
        'Cargo por energía por los siguientes 490 kWh': ['Cargo por energía'],
        'Cargo por energía por los siguientes kWh': ['Cargo por energía'],
        'Cargo por energía (en BTS siguientes kWh y en BTD hasta 10,000 kWh)': ['Cargo por energía'],
        'Cargo por energía (en BTD siguientes kWh de 10,001 a 30,000)': ['Cargo por energía'],
        'Cargo por energía (en BTD siguientes kWh de 30,001 a 50,000)': ['Cargo por energía'],
        'Cargo por energía (en BTD siguientes kWh desde 50,001)': ['Cargo por energía'],
        'Cargo por energía por los kWh adicionales': ['Cargo por energía'],
        'Cargo por Energía por las primeras 255 horas': ['Cargo por energía'],
        'Cargo por Energía por las siguientes horas uso': ['Cargo por energía'],
        'Cargo por Energía por las primeras 255 horas uso': ['Cargo por energía'],
        'Cargo de energía por los kWh adicionales': ['Cargo por energía'],
        'Cargo de energía': ['Cargo por energía'],
        'Cargo por Consumo de energía': ['Cargo por energía'],
        'Consumo de energía': ['Cargo por energía'],
        'Cargo por Potencia Energizada': ['Cargo por energía'],
        'Cargo por Potencia Energizada (en BTD hasta 10,000 kWh)': ['Cargo por energía'],
        'Cargo por Potencia Energizada (en BTD siguientes kWh de 10,001 a 30,000)': ['Cargo por energía'],
        'Cargo por Potencia Energizada (en BTD siguientes kWh de 30,001 a 50,000)': ['Cargo por energía'],
        'Cargo por Potencia Energizada (en BTD siguientes kWh desde 50,001)': ['Cargo por energía'],
        'Potencia Energizada': ['Cargo por energía'],

        'Cargo por energía en Punta': ['Cargo por energía en Punta'],
        'Cargo por Energía en Punta': ['Cargo por energía en Punta'],
        'Energía en Punta': ['Cargo por energía en Punta'],
        'Cargo por potencia energizada en Punta': ['Cargo por energía en Punta'],
        'Cargo po potencia energizada en Punta': ['Cargo por energía en Punta'],
        'Cargo por Potencia energizada en Punta': ['Cargo por energía en Punta'],
        'Cargo por energía Fuera de Punta': ['Cargo por energía Fuera de Punta'],
        'Cargo por Energía Fuera de Punta': ['Cargo por energía Fuera de Punta'],
        'Energía Fuera de Punta': ['Cargo por energía Fuera de Punta'],
        'Cargo por Potencia energizada Fuera de Punta': ['Cargo por energía Fuera de Punta'],
        'Cargo por potencia energizada Fuera de Punta': ['Cargo por energía Fuera de Punta'],

        'Cargo pérdidas de energía': ['Cargo pérdidas de energía'],
        'Cargo por pérdidas de energía': ['Cargo pérdidas de energía'],
        'Cargo por pérdidas de energía estándar': ['Cargo pérdidas de energía'],
        'Cargo por pérdida estándar de energía distribución': ['Cargo pérdidas de energía'],
        'Cargo por pérdida estándar de energía en distribución': ['Cargo pérdidas de energía'],
        'Cargo por pérdidas de energía por los siguientes 490 kWh': ['Cargo pérdidas de energía'],
        'Cargo por pérdidas de energía por los kWh adicionales': ['Cargo pérdidas de energía'],
        'Cargo por pérdidas en transmisión': ['Cargo pérdidas de energía'],
        'Cargo por pérdidas estándar en distribución': ['Cargo pérdidas de energía'],
        'Cargo pérdida energía en Punta': ['Cargo pérdida energía en Punta'],
        'Cargo por pérdida de energía en Punta': ['Cargo pérdida energía en Punta'],
        'Cargo por pérdida estándar de energía en Punta': ['Cargo pérdida energía en Punta'],
        'Pérdida Energía en Distribución en Punta': ['Cargo pérdida energía en Punta'],
        'Cargo por pérdida estándar de Energía en Distribución en Punta': ['Cargo pérdida energía en Punta'],
        'Cargo por pérdida estándar de energía en Distribución en Punta': ['Cargo pérdida energía en Punta'],
        'Cargo pérdida energía Fuera de Punta': ['Cargo pérdida energía Fuera de Punta'],
        'Cargo por pérdida de energía Fuera de Punta': ['Cargo pérdida energía Fuera de Punta'],
        'Cargo por pérdida estándar de energía Fuera de Punta': ['Cargo pérdida energía Fuera de Punta'],
        'Cargo por pérdida estándar de Energía en Distribución Fuera Punta': ['Cargo pérdida energía Fuera de Punta'],
        'Cargo por pérdida estándar de energía en Distribución Fuera de Punta': [
            'Cargo pérdida energía Fuera de Punta'],
        'Pérdidas Estándar en Distribución Fuera Punta': ['Cargo pérdida energía Fuera de Punta'],

        'Cargo por pérdida de demanda': ['Cargo por pérdida de demanda'],
        'Cargo por pérdida estándar de demanda': ['Cargo por pérdida de demanda'],
        'Cargo por pérdida estándar de Demanda en Distribución': ['Cargo por pérdida de demanda'],
        'Pérdida Potencia en Distribución': ['Cargo por pérdida de demanda'],
        'Cargo por pérdida de demanda máxima en Punta': ['Cargo por pérdida de demanda máxima en Punta'],
        'Cargo por pérdida estándar de Demanda Máxima en Punta': ['Cargo por pérdida de demanda máxima en Punta'],
        'Cargo por pérdida de Demanda Máxima en Distribución en Punta': [
            'Cargo por pérdida de demanda máxima en Punta'],

        'Cargo por demanda máxima': ['Cargo por demanda máxima'],
        'Cargo por demanda máxima en Punta': ['Cargo por demanda máxima en Punta'],
        'Cargo por demanda máxima Fuera de Punta': ['Cargo por demanda máxima Fuera de Punta'],
        'Cargo por demanda máxima en Fuera de Punta': ['Cargo por demanda máxima Fuera de Punta'],

        'Cargo de abastecimiento de alumbrado público': ['Cargo de abastecimiento de alumbrado público']
    }

    return dict_u


# ------------------------------------------------------------------
# Diccionario de Cargos de Punt y Fuera de Punta
# ------------------------------------------------------------------
def cargos_TH():
    dict_c = {
        'Cargo Fijo': ['Cargo Fijo en Punta', 'Cargo Fijo Fuera de Punta'],
        'Cargo Fijo (en BTS - primeros 10 kWh)': ['Cargo Fijo en Punta', 'Cargo Fijo Fuera de Punta'],
        'Cargo por energía': ['Cargo por energía en Punta', 'Cargo por energía Fuera de Punta'],
        'Cargo de energía': ['Cargo por energía en Punta', 'Cargo por energía Fuera de Punta'],
        'Cargo por Sistema': ['Cargo por energía en Punta', 'Cargo por energía Fuera de Punta'],
        'Sistema': ['Cargo por energía en Punta', 'Cargo por energía Fuera de Punta'],
        'Cargo de abastecimiento de alumbrado público': ['Cargo por energía en Punta', 'Cargo por energía Fuera de Punta'],
        'Cargo por Consumo de energía': ['Cargo por energía en Punta', 'Cargo por energía Fuera de Punta'],
        'Cargo por pérdidas en transmisión': ['Cargo pérdida energía en Punta', 'Cargo pérdida energía Fuera de Punta'],
        'Cargo por demanda máxima': ['Cargo por demanda máxima en Punta',
                                        'Cargo por demanda máxima Fuera de Punta'],
        'Cargo por Potencia Energizada (en BTD hasta 10,000 kWh)':
            ['Cargo por energía en Punta', 'Cargo por energía Fuera de Punta'],
        'Cargo por Potencia Energizada': ['Cargo por energía en Punta', 'Cargo por energía Fuera de Punta']
    }

    return dict_c


# -----------------------------------
# Diccionario de Tarifas a convertir
# -----------------------------------
def rangos_BTD():
    dict_u = {
        'Cargo por energía (en BTS siguientes kWh y en BTD hasta 10,000 kWh)': ['0-10000 kWh'],
        'Cargo por energía (en BTD siguientes kWh de 10,001 a 30,000)': ['10001-30000 kWh'],
        'Cargo por energía (en BTD siguientes kWh de 30,001 a 50,000)': ['30001-50000 kWh'],
        'Cargo por energía (en BTD siguientes kWh desde 50,001)': ['50001-999999 kWh'],

        'Cargo por Potencia Energizada (en BTD hasta 10,000 kWh)': ['0-10000 kWh'],
        'Cargo por Potencia Energizada (en BTD siguientes kWh de 10,001 a 30,000)': ['10001-30000 kWh'],
        'Cargo por Potencia Energizada (en BTD siguientes kWh de 30,001 a 50,000)': ['30001-50000 kWh'],
        'Cargo por Potencia Energizada (en BTD siguientes kWh desde 50,001)': ['50001-999999 kWh']
    }

    return dict_u


# ------------------------------------------------------------------
# Diccionario de Unities a convertir
# ------------------------------------------------------------------
def unities_validos():
    dict_u = {'Unities':
        {
            'Cargo por energía (en BTS siguientes kWh)': 'B/.kWh',
            'Cargo Fijo': 'B/.cliente mes',
            'Cargo por energía, excluyendo pérdidas': 'B/.kWh',
            'Consumo de energía': 'B/.kWh',
            'Energía en Punta': 'B/.kWh',
            'Energía Fuera de Punta': 'B/.kWh',
            'Potencia Energizada': 'B/.kWh',
            'Cargo por energía por los siguientes 490 kWh': 'B/.kWh',
            'Cargo de energía por los kWh adicionales': 'B/.kWh',
            'Cargo por energía por los siguientes kWh': 'B/.kWh',
            'Cargo por Energía por las primeras 255 horas': 'B/.kWh',
            'Cargo por Energía por las siguientes horas uso': 'B/.kWh',
            'Cargo por pérdidas de energía estándar': 'B/.kWh',
            'Cargo por pérdida estándar de energía en Punta': 'B/.kWh',
            'Cargo por pérdida estándar de energía Fuera de Punta': 'B/.kWh',
            'Cargo por pérdida estándar de demanda': 'B/.kW/mes',
            'Cargo por pérdida estándar de Demanda Máxima en Punta': 'B/.kW/mes',
            'Cargo por energía en Punta': 'B/.kW/mes',
            'Cargo por energía Fuera de Punta': 'B/.kW/mes',
            'Cargo por Energía por las primeras 255 horas uso': 'B/.kW/mes',
            'Pérdida Potencia en Distribución': 'B/.kW/mes',
            'Pérdida Energía en Distribución en Punta': 'B/.kWh',
            'Pérdidas Estándar en Distribución Fuera Punta': 'B/.kWh',
            'Cargo por pérdidas de energía por los siguientes 490 kWh': 'B/.kWh',
            'Cargo Fijo (en BTS - primeros 10 kWh)': 'B/.cliente mes',
            'Cargo por energía (en BTS siguientes kWh y en BTD hasta 10,000 kWh)': 'B/.kWh',
            'Cargo por energía (en BTD siguientes kWh de 10,001 a 30,000)': 'B/.kWh',
            'Cargo por energía (en BTD siguientes kWh de 30,001 a 50,000)': 'B/.kWh',
            'Cargo por energía (en BTD siguientes kWh desde 50,001)': 'B/.kWh',
            'Cargo por Energía en Punta': 'B/.kWh',
            'Cargo por Energía Fuera de Punta': 'B/.kWh',
            'Cargo por demanda máxima': 'B/.kW/mes',
            'Cargo por demanda máxima en Punta': 'B/.kW/mes',
            'Cargo por demanda máxima Fuera de Punta': 'B/.kW/mes',
            'Cargo por energía': 'B/.kWh',
            'Cargo por pérdida estándar de energía en distribución': 'B/.kWh',
            'Cargo por pérdida estándar de Energía en Distribución en Punta': 'B/.kWh',
            'Cargo por pérdida estándar de Energía en Distribución Fuera Punta': 'B/.kWh',
            'Cargo por Sistema': 'B/.kWh',
            'Cargo por Consumo de energía': 'B/.kWh',
            'Cargo por demanda máxima en Fuera de Punta': 'B/.kW/mes',
            'Cargo por pérdidas en transmisión': 'B/.kWh',
            'Cargo por Potencia Energizada (en BTD hasta 10,000 kWh)': 'B/.kWh',
            'Cargo por Potencia Energizada (en BTD siguientes kWh de 10,001 a 30,000)': 'B/.kWh',
            'Cargo por Potencia Energizada (en BTD siguientes kWh de 30,001 a 50,000)': 'B/.kWh',
            'Cargo por Potencia Energizada (en BTD siguientes kWh desde 50,001)': 'B/.kWh',
            'Cargo por Potencia energizada en Punta': 'B/.kWh',
            'Cargo por Potencia energizada Fuera de Punta': 'B/.kWh',
            'Cargo por pérdida estándar de energía distribución': 'B/.kWh',
            'Cargo por pérdida estándar de energía en Distribución en Punta': 'B/.kWh',
            'Cargo por pérdida estándar de energía en Distribución Fuera de Punta': 'B/.kWh',
            'Cargo por pérdida estándar de Demanda en Distribución': 'B/.kW/mes',
            'Cargo por pérdida de Demanda Máxima en Distribución en Punta': 'B/.kW/mes',
            'Cargo de abastecimiento de alumbrado público': 'B/.kWh',
            'Cargo de energía': 'B/.kWh',
            'Cargo por Potencia Energizada': 'B/.kWh',
            'Cargo por pérdidas estándar en distribución': 'B/.kWh',
            'Cargo por potencia energizada en Punta': 'B/.kWh',
            'Cargo po potencia energizada en Punta': 'B/.kWh',
            'Cargo por pérdidas de energía por los kWh adicionales': 'B/.kWh',
            'Cargo por energía por los kWh adicionales': 'B/.kWh',
            'Sistema': 'B/.kWh',
            'Cargo por potencia energizada Fuera de Punta': 'B/.kWh'}
    }
    return dict_u


# ------------------------------------------------------------------
# Diccionario de Composiciones a convertir
# ------------------------------------------------------------------
def composicion_validos():
    dict_c = {'Composicion':
        {
            'CARGOS EN LA FACTURA': 'CARGOS EN LA FACTURA',
            'DETALLE DE LOS CARGOS': 'DETALLE DE LOS CARGOS',
            'Comercialización': 'Comercialización',
            'Distribución': 'Distribución',
            'Alumbrado Público': 'Alumbrado Público',
            'Transmisión': 'Transmisión',
            'Generación': 'Generación',
            'Cargo Fijo (en BTS - primeros 10 kWh) B/.cliente mes': 'Cargo Fijo (en BTS - primeros 10 kWh)',
            'Cargo Fijo(en BTS - primeros 10 kWh) B /.cliente mes': 'Cargo Fijo (en BTS - primeros 10 kWh)',
            'Cargo por energía (en BTS siguientes kWh y en BTD hasta 10,B00/.0k WkWhh)':
                'Cargo por energía (en BTS siguientes kWh y en BTD hasta 10,000 kWh)',
            'Cargo por energía (en BTS siguientes kWh y en BTD hasta 10,000 BkW/.khW) h':
                'Cargo por energía (en BTS siguientes kWh y en BTD hasta 10,000 kWh)',
            'Cargo por energía (en BTS siguientes kWh y en BTD hasta 10,0B00/. kkWhh)':
                'Cargo por energía (en BTS siguientes kWh y en BTD hasta 10,000 kWh)',
            'Cargo por energía (en BTD siguientes kWh de 10,001 a 30,000)':
                'Cargo por energía (en BTD siguientes kWh de 10,001 a 30,000)',
            'Cargo por energía (en BTD siguientes kWh de 10,001 a 30,000)B/.kWh':
                'Cargo por energía (en BTD siguientes kWh de 10,001 a 30,000)',
            'Cargo por energía (en BTD siguientes kWh de 10,001 a 30,000) B/.kWh':
                'Cargo por energía (en BTD siguientes kWh de 10,001 a 30,000)',
            'Cargo por energía(en BTD siguientes kWh de 10, 001 a 30, 000) B /.kWh':
                'Cargo por energía (en BTD siguientes kWh de 10,001 a 30,000)',
            'Cargo por energía (en BTD siguientes kWh de 30,001 a 50,000)':
                'Cargo por energía (en BTD siguientes kWh de 30,001 a 50,000)',
            'Cargo por energía (en BTD siguientes kWh de 30,001 a 50,000)B/.kWh':
                'Cargo por energía (en BTD siguientes kWh de 30,001 a 50,000)',
            'Cargo por energía (en BTD siguientes kWh de 30,001 a 50,000) B/.kWh':
                'Cargo por energía (en BTD siguientes kWh de 30,001 a 50,000)',
            'Cargo por energía (en BTD siguientes kWh desde 50,001)':
                'Cargo por energía (en BTD siguientes kWh desde 50,001)',
            'Cargo por energía (en BTD siguientes kWh desde 50,001) B/.kWh':
                'Cargo por energía (en BTD siguientes kWh desde 50,001)',
            'Cargo por Energía en Punta B/.kWh': 'Cargo por Energía en Punta',
            'Cargo por energía en Punta B/.kWh': 'Cargo por Energía en Punta',
            'Cargo por Energía Fuera de Punta B/.kWh': 'Cargo por Energía Fuera de Punta',
            'Cargo por energía Fuera de Punta B/.kWh': 'Cargo por Energía Fuera de Punta',
            'Cargo de energía B /.kWh': 'Cargo de energía',
            'Cargo de energía B/.kWh': 'Cargo de energía',
            'Cargo por energía B/.kWh': 'Cargo por energía',
            'Cargo por demanda máxima B/.kW/mes': 'Cargo por demanda máxima',
            'Cargo por demanda máxima B /.kW / mes': 'Cargo por demanda máxima',
            'Cargo por demanda máxima en Punta B/.kW/mes': 'Cargo por demanda máxima en Punta',
            'Cargo por demanda máxima Fuera de Punta B/.kW/mes': 'Cargo por demanda máxima Fuera de Punta',
            'Cargo por pérdida estándar de energía distribución B/.kWh':
                'Cargo por pérdida estándar de energía en distribución',
            'Cargo por Sistema B/.kWh': 'Cargo por Sistema',
            'Cargo por pérdida estándar de energía en Distribución en Punta B/.kWh':
                'Cargo por pérdida estándar de energía en Distribución en Punta',
            'Cargo por pérdida estándar de energía en Distribución Fuera de PunBta/.kWh':
                'Cargo por pérdida estándar de energía en Distribución Fuera de Punta',
            'Cargo por pérdida estándar de Demanda en Distribución B/.kW/mes':
                'Cargo por pérdida estándar de Demanda en Distribución',
            'Cargo por pérdida de Demanda Máxima en Distribución en Punta B/.kW/mes':
                'Cargo por pérdida de Demanda Máxima en Distribución en Punta',
            'Cargo de abastecimiento de alumbrado público B/.kWh': 'Cargo de abastecimiento de alumbrado público',
            'Cargo por Potencia Energizada B/.kWh': 'Cargo por Potencia Energizada',
            'Cargo por pérdidas en transmisión B/.kWh': 'Cargo por pérdidas en transmisión',
            'Cargo por demanda máxima Fuera de Punt': 'Cargo por demanda máxima Fuera de Punta',
            'Cargo por pérdida estándar de energía Fuera de P':
                'Cargo por pérdida estándar de energía Fuera de Punta',
            'Cargo por pérdida estándar de energía Fuera de Punt':
                'Cargo por pérdida estándar de energía Fuera de Punta',
            'Cargo por pérdidas de energía por los siguientes 4':
                'Cargo por pérdidas de energía por los siguientes 490 kWh',
            'Cargo por pérdidas de energía por los siguientes 490':
                'Cargo por pérdidas de energía por los siguientes 490 kWh',
            'Cargo por pérdidas de energía por los kWh adicional':
                'Cargo por pérdidas de energía por los kWh adicionales',
            'Cargo por pérdidas de energía por los kWh adicio':
                'Cargo por pérdidas de energía por los kWh adicionales',
            'Cargo por pérdida estándar de Demanda Máxima': 'Cargo por pérdida estándar de Demanda Máxima en Punta',
            'Cargo por pérdida estándar de Demanda Máxima en': 'Cargo por pérdida estándar de Demanda Máxima en Punta',
            'Cargo por Potencia Energizada B /.kWh': 'Cargo por Potencia Energizada'}
    }
    return dict_c


# -------------------------------------
# Diccionario de Unities a convertir
# -------------------------------------
def rangos_validos():
    dict = \
        {
            'BTS 1': ['0-300 kWh'],
            'BTS 2': ['301-750 kWh'],
            'BTS 3': ['751-999999 kWh'],
            'Prepago': ['0-300 kWh'],
            'BTD': ['0-10000 kWh', '10001-30000 kWh', '30001-50000 kWh', '50001-999999 kWh'],
            'BTH': ['0-999999 kWh'],
            'MTD': ['0-999999 kWh'],
            'MTH': ['0-999999 kWh'],
            'ATD': ['0-999999 kWh'],
            'ATH': ['0-999999 kWh']
        }
    return dict


# ------------------------------------------------------------------
# Areas y columnas de pdfs con problemas de scrapeo para el tabula
# ------------------------------------------------------------------
def areas_pdf():
    dict = {
        'comp_tarifa_I_2013.pdf': {
            '1': {'Area': [15, 2, 484, 792], 'Columnas': [240, 315, 370, 425, 496, 562, 628, 694]},
            '2': {'Area': [15, 2, 484, 792], 'Columnas': [280, 363, 414, 462, 525, 588, 643, 709]},
            '3': {'Area': [15, 2, 484, 792], 'Columnas': [245, 363, 414, 462, 525, 588, 643, 709]},
            '4': {'Area': [14, 2, 484, 792], 'Columnas': [220, 300, 370, 425, 496, 562, 628, 694]}},

        'comp_tarifa_II_2013.pdf': {
            '1': {'Area': [15, 2, 484, 792], 'Columnas': [240, 315, 370, 425, 496, 562, 628, 694]},
            '2': {'Area': [15, 2, 484, 792], 'Columnas': [280, 363, 414, 462, 525, 588, 643, 709]},
            '3': {'Area': [15, 2, 484, 792], 'Columnas': [245, 363, 414, 462, 525, 588, 643, 709]},
            '4': {'Area': [14, 2, 484, 792], 'Columnas': [220, 300, 370, 425, 496, 562, 628, 694]}},

        'comp_tarifa_I_2014.pdf': {
            '1': {'Area': [15, 2, 484, 792], 'Columnas': [240, 315, 370, 425, 496, 562, 628, 694]},
            '2': {'Area': [15, 2, 484, 792], 'Columnas': [280, 363, 414, 462, 525, 588, 643, 709]},
            '3': {'Area': [15, 2, 484, 792], 'Columnas': [245, 363, 414, 462, 525, 588, 643, 709]},
            '4': {'Area': [14, 2, 484, 792], 'Columnas': [220, 300, 370, 425, 496, 562, 628, 694]}},

        'comp_tarifa_II_2014.pdf': {
            '1': {'Area': [15, 2, 484, 792], 'Columnas': [230, 300, 360, 430, 500, 570, 640, 710]},
            '2': {'Area': [15, 2, 484, 792], 'Columnas': [270, 360, 420, 480, 535, 590, 650, 709]},
            '3': {'Area': [15, 2, 484, 792], 'Columnas': [245, 350, 414, 462, 525, 588, 643, 709]},
            '4': {'Area': [14, 2, 484, 792], 'Columnas': [220, 300, 370, 425, 496, 562, 628, 694]}},

        'comp_tarifa_I_2015.pdf': {
            '1': {'Area': [14, 2, 744, 1052], 'Columnas': [342, 410, 470, 530, 580, 630, 680, 740, 800, 860, 950]},
            '2': {'Area': [14, 1, 744, 1052], 'Columnas': []},
            '3': {'Area': [13, 2, 744, 1052], 'Columnas': [337, 410, 465, 510, 565, 620, 665, 745, 785, 840, 910]}},

        'comp_tarifa_II_2015.pdf': {
            '1': {'Area': [14, 2, 744, 1052], 'Columnas': [342, 410, 470, 530, 580, 630, 680, 740, 800, 860, 950]},
            '2': {'Area': [14, 1, 744, 1052], 'Columnas': []},
            '3': {'Area': [13, 2, 744, 1052], 'Columnas': [337, 410, 465, 510, 565, 620, 665, 745, 785, 840, 910]}},

        'comp_tarifa_II_2016.pdf': {
            '1': {'Area': [14, 2, 744, 1052], 'Columnas': [342, 410, 470, 530, 580, 630, 680, 740, 800, 860, 950]},
            '2': {'Area': [14, 1, 744, 1052], 'Columnas': []},
            '3': {'Area': [13, 2, 744, 1052], 'Columnas': [337, 410, 465, 510, 565, 620, 665, 745, 785, 840, 910]}},

        'comp_tarifa_I_2016.pdf': {
            '1': {'Area': [14, 2, 744, 1052], 'Columnas': [342, 410, 470, 530, 580, 630, 680, 740, 800, 860, 950]},
            '2': {'Area': [14, 1, 744, 1052], 'Columnas': []},
            '3': {'Area': [13, 2, 744, 1052], 'Columnas': [337, 410, 465, 510, 565, 620, 665, 745, 785, 840, 910]}},

        'comp_tarifa_II_2017.pdf': {
            '1': {'Area': [15, 2, 744, 1052], 'Columnas': [342, 410, 470, 530, 580, 630, 680, 740, 800, 860, 950]},
            '2': {'Area': [14, 1, 744, 1052], 'Columnas': [342, 410, 470, 530, 580, 630, 680, 740, 800, 860, 950]},
            '3': {'Area': [15, 2, 744, 1052], 'Columnas': [337, 410, 465, 510, 565, 620, 665, 745, 785, 840, 910]}},

        'comp_tarifa_I_2017.pdf': {
            '1': {'Area': [15, 2, 744, 1052], 'Columnas': [342, 410, 470, 530, 580, 630, 680, 740, 800, 860, 950]},
            '2': {'Area': [14, 1, 744, 1052], 'Columnas': [342, 410, 470, 530, 580, 630, 680, 740, 800, 860, 950]},
            '3': {'Area': [15, 2, 744, 1052], 'Columnas': [337, 410, 465, 510, 565, 620, 665, 745, 785, 840, 910]}},

        'comp_tarifa_I_2018.pdf': {
            '1': {'Area': [15, 2, 744, 1052], 'Columnas': [342, 410, 470, 530, 580, 630, 680, 740, 800, 860, 950]},
            '2': {'Area': [14, 1, 744, 1052], 'Columnas': [342, 410, 470, 530, 580, 630, 680, 740, 800, 860, 950]},
            '3': {'Area': [15, 2, 744, 1052], 'Columnas': [337, 410, 465, 510, 565, 620, 665, 745, 785, 840, 910]}},

        'comp_tarifa_II_2018.pdf': {
            '1': {'Area': [15, 2, 744, 1052], 'Columnas': [342, 410, 470, 530, 580, 630, 680, 740, 800, 860, 950]},
            '2': {'Area': [14, 1, 744, 1052], 'Columnas': [342, 410, 470, 530, 580, 630, 680, 740, 800, 860, 950]},
            '3': {'Area': [15, 2, 744, 1052], 'Columnas': [337, 410, 465, 510, 565, 620, 665, 745, 785, 840, 910]}},

        'comp_tarifa_I_2019.pdf': {
            '1': {'Area': [15, 2, 744, 1052], 'Columnas': []},
            '2': {'Area': [15, 2, 744, 1052], 'Columnas': []},
            '3': {'Area': [15, 2, 744, 1052], 'Columnas': [360, 410, 465, 510, 565, 620, 675, 730, 785, 840, 910]}},

        'comp_tarifa_II_2019.pdf': {
            '1': {'Area': [15, 2, 744, 1052], 'Columnas': []},
            '2': {'Area': [15, 2, 744, 1052], 'Columnas': []},
            '3': {'Area': [15, 2, 744, 1052], 'Columnas': [360, 410, 465, 510, 565, 620, 675, 730, 785, 840, 910]}},

        'comp_tarifa_I_2020.pdf': {
            '1': {'Area': [15, 2, 744, 1052], 'Columnas': []},
            '2': {'Area': [15, 2, 744, 1052], 'Columnas': []},
            '3': {'Area': [15, 2, 744, 1052], 'Columnas': [360, 410, 465, 510, 565, 620, 675, 730, 785, 840, 910]}},

        'comp_tarifa_II_2020.pdf': {
            '1': {'Area': [15, 2, 744, 1052], 'Columnas': []},
            '2': {'Area': [15, 2, 744, 1052], 'Columnas': []},
            '3': {'Area': [15, 2, 744, 1052], 'Columnas': [300, 345, 390, 430, 470, 510, 555, 600, 650, 700, 770]}}
    }

    return dict


# ------------------------------------------------------------------
# Diccionario de Nombres de PDFs no validos
# ------------------------------------------------------------------
def nombres_no_validos():
    dict_n = {
        "Pliego Tarifario": True,
        "EDEMET": True, "EDECHI": True, "ELEKTRA": True,
        "I_vc": True, "II_vc": True, "_vc_": True, "variacion": True,
        "I_tarifas": True, "tarifas_1": True, "tarifas_julio": True, "tarifas_agosto": True,
        "I_semestre": True, "II_semestre": True,
        "ASEP": True, "2007": True, "2008": True, "2010": True, "2011": True, "2012": True
    }
    return dict_n


# ----------------------------
# Arma el df desde los PDFs
# ----------------------------
def arma_desde_pagina(path, link_title, page_number, local):
    unit_base = unities_validos()
    dict_c = composicion_validos()
    compo_base = dict_c["Composicion"]
    areas_base = areas_pdf()

    # ----------------------------------------------------------
    # Armado del dataframe con las tablas del PDF con Tabula.py
    # ----------------------------------------------------------
    df_tabla = pd.DataFrame()

    try:
        for pag in range(1, (page_number + 1)):

            if link_title in areas_base:
                areas = areas_base[link_title][str(pag)]['Area']
                columnas = areas_base[link_title][str(pag)]['Columnas']
            else:
                columnas = []
                if 'comp_tarifa_I_2016' in link_title:
                    areas = [13.5, 2, 744, 952]
                else:
                    areas = [15, 2, 650, 1052]

            dfs = util.read_pdftabula(path, **{"stream": True, "pages": pag, "guess": False, "area": areas,
                                                "columns": columnas, "relative_area": True,
                                                "pandas_options": {'header': None}})

            table = dfs.copy()

            if len(table.columns.values) < 12:
                if len(table.columns.values) < 11:
                    table = table.rename({0: 'Composicion', 1: 'Unidades', 2: 'BTS 1', 3: 'BTD', 4: 'BTH',
                                            5: 'MTD', 6: 'MTH', 7: 'ATD', 8: 'ATH'},
                                            axis=1).sort_index(axis=1).astype('string')
                    table["BTS 2"] = np.nan
                    table["BTS 3"] = np.nan
                    table["Prepago"] = np.nan
                else:
                    if '2015' in link_title or '2016' in link_title:
                        compo = table.loc[7, 0]
                        if compo in compo_base or '':
                            table["Compo"] = table[0].map(dict_c['Composicion'])
                            table[0] = table["Compo"]
                            table = table.drop(columns=['Compo'])
                            table[1] = table[0].map(unit_base['Unities'])

                    if 'B/.kWh' not in table.values:
                        table = table.rename({0: 'Composicion', 1: 'BTS 1', 2: 'BTS 2', 3: 'BTS 3', 4: 'Prepago',
                                                5: 'BTD', 6: 'BTH', 7: 'MTD', 8: 'MTH', 9: 'ATD', 10: 'ATH'},
                                                axis=1).sort_index(axis=1).astype('string')
                        table["Unidades"] = table['Composicion'].map(unit_base['Unities'])
                    else:
                        table = table.rename({0: 'Composicion', 1: 'Unidades', 2: 'BTS 1', 3: 'BTS 2', 4: 'BTS 3',
                                                5: 'BTD', 6: 'BTH', 7: 'MTD', 8: 'MTH', 9: 'ATD', 10: 'ATH'},
                                                axis=1).sort_index(axis=1).astype('string')
                        table["Prepago"] = np.nan

            else:
                table = table.rename({0: 'Composicion', 1: 'Unidades', 2: 'BTS 1', 3: 'BTS 2', 4: 'BTS 3',
                                        5: 'Prepago', 6: 'BTD', 7: 'BTH', 8: 'MTD', 9: 'MTH', 10: 'ATD',
                                        11: 'ATH'}, axis=1).sort_index(axis=1).astype('string')

            if 'comp_tarifa_II_2019' in link_title:
                compo = table.loc[7, 'Composicion']
                if compo in compo_base:
                    table["Compo"] = table['Composicion'].map(dict_c['Composicion'])
                    table["Composicion"] = table["Compo"]
                    table = table.drop(columns=['Compo'])

            table["Unidades"] = table['Composicion'].map(unit_base['Unities'])

            columnas = ['Composicion', 'Unidades', 'BTS 1', 'BTS 2', 'BTS 3', 'Prepago',
                        'BTD', 'BTH', 'MTD', 'MTH', 'ATD', 'ATH']

            table = table[columnas]
            table = table.rename_axis(index=None)
            table.reset_index(drop=True, inplace=True)
            table = table.replace({None: np.nan})
            table = table.replace({'': np.nan})

            table = table.dropna(how='all')
            df_tabla = df_tabla.append(table)

        df_tabla = df_tabla.rename_axis(index=None)
        df_tabla.reset_index(drop=True, inplace=True)

    except:
        util.log(local, 'Error de formato del PDF {}'.format(link_title))
        print('Error de formato del PDF {}'.format(link_title))
        return False

    return df_tabla


# ------------------------------------
# Parseado de datos para grabación
# ------------------------------------
def pagina_parser(df_tabla, pais, tariff_link, pdf_link, link_title, date_ori, date_end, local):
    lista_csv = []
    tariff_base = cargos_validos()
    unit_base = unities_validos()
    rang_base = rangos_validos()
    dict_c = composicion_validos()
    rangos_btd = rangos_BTD()
    cargos_punta = cargos_TH()
    compo_base = dict_c["Composicion"]
    tariff_code = ''
    component = ''
    utility = ["EDEMET", "ELEKTRA", "EDECHI"]
    tariff_type = ''
    charge_type = ''
    tariff_ori = ''

    columnas = ['Composicion', 'Unidades', 'BTS 1', 'BTS 2', 'BTS 3', 'Prepago',
                'BTD', 'BTH', 'MTD', 'MTH', 'ATD', 'ATH']

    uti = -1
    try:
        for t1 in range(len(df_tabla)):
            compo = df_tabla.loc[t1, 'Composicion']
            if compo is np.nan or 'Composici' in compo or '*' in compo:
                continue

            unit = df_tabla.loc[t1, 'Unidades']
            if unit is not np.nan:
                if '*' in unit:
                    continue

            if compo is np.nan and unit is np.nan:
                continue

            if "FACTURA" in compo and unit is np.nan:
                tariff_type = "Factura"
                uti += 1
                continue

            if "DETALLE" in compo and unit is np.nan:
                continue

            if compo is not np.nan and unit is np.nan:
                if compo_base.get(compo):
                    compo = compo_base.get(compo)
                    unit = unit_base.get(compo)
                tariff_type = compo
                continue

            inicio = 2
            tariff_ori = df_tabla.loc[t1, 'Composicion']
            for col in range(inicio, len(columnas)):
                if type(df_tabla.loc[t1, columnas[col]]) == str:
                    tariff_code = columnas[col]
                    if 'TH' in tariff_code:
                        if cargos_punta.get(compo):
                            cargos = cargos_punta.get(compo)
                        elif tariff_base.get(compo):
                            cargos = tariff_base.get(compo)
                        else:
                            print("Error falta tariff_base")
                            continue
                    elif tariff_base.get(compo):
                        cargos = tariff_base.get(compo)
                    else:
                        print("Error falta tariff_base")
                        continue
                    for c, cargo in enumerate(cargos):
                        charge_type = cargo
                        charge = df_tabla.loc[t1, columnas[col]]
                        if charge == '.':
                            continue
                        charge = charge.replace(' ', '')
                        if 'meses' in charge or 'sto,seaplicóelp' in charge or 'semestre' in charge:
                            continue
                        if charge.isalpha():
                            continue
                        if 'BTD' in tariff_code:
                            if rangos_btd.get(tariff_ori):
                                component = rangos_btd.get(tariff_ori)
                            else:
                                component = rang_base.get(columnas[col])
                        else:
                            component = rang_base.get(columnas[col])
                        for c1, com in enumerate(component):
                            lista = [pais, pdf_link, link_title, tariff_code, com, unit, utility[uti],
                                    charge, date_ori, date_end, "USD", tariff_type, charge_type]
                            lista_csv.append(lista)

    except:
        util.log(local, 'Error de formato del PDF {}'.format(pdf_link))
        print('Error de formato del PDF {}'.format(pdf_link))
        return False

    return lista_csv


# ************************
# Grabación de archivos
# ***********************
def grabacion_archivos(data, lista_csv, pais, tariff_link, link_title, pdf_link,
                        anio_mes_ori, anio_mes_end, beginning_time, local):
    df_csv = pd.DataFrame(lista_csv, columns=['Country', 'Document_link', 'PDF_Name', 'Tariff_code', 'Component',
                                                'Unit', 'Utility', 'Charge', 'Date_start', 'Date_end', '$local',
                                                'Tariff_type', 'Charge_type'])

    nombre = "panama_tariff_asep_" + anio_mes_ori + "-" + anio_mes_end
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

    lis_data = [tariff_link, link_title, pdf_link, anio_mes_ori, 0, pais, local, "", "", ""]
    data.append(lis_data)

    valor = True
    last_iteration = False
    return valor, last_iteration, data


# ------------------------------------------------------------------
# Descarga y grabación de CSVs
# ------------------------------------------------------------------
def arma_csv(data, path, local, pais, tariff_link, link_title, pdf_link, date_limit, last_iteration):
    pais = pais.capitalize()
    mes_dict = util.MonthDict()
    beginning_time = time.time()
    day_begin = None
    day_end = None
    year = None
    month_begin = None
    month_end = None

    pdf = util.read_pdfplumber(path)

    # ----------------------------------------------------------
    # Armado del dataframe con las tablas del PDF con PDFPlumber
    # ----------------------------------------------------------

    pages = pdf.pages
    df_plumb = pd.DataFrame()
    lista_lit = []
    for p, page in enumerate(pages):
        texto = page.extract_text()
        lista_lit.append(texto.split("\n"))
    page_number = page.page_number

    # -------------------------------------
    # Barrido de Datos Iniciales del PDF
    # -------------------------------------

    for f, fila in enumerate(lista_lit[p]):
        if "EMPRESA" in fila:
            continue
        if "VIGEN" in fila.upper():
            periodo = fila
            periodo = " ".join(periodo.split())
            continue
        if f > 2:
            break

    try:
        words_list = periodo.strip().split(" ")
        for word in words_list:
            if word.isdigit():
                if len(word) == 1:
                    if day_begin is None:
                        day_begin = "0" + word
                    else:
                        day_end = "0" + word
                    continue
                if len(word) == 2:
                    if day_begin is None:
                        day_begin = word
                    else:
                        day_end = word
                    continue
                if len(word) == 4:
                    year = word
                    continue
            if mes_dict.get(word):
                month = mes_dict.get(word)
                if month_begin is None:
                    month_begin = month
                else:
                    month_end = month
                continue

        year_month = year + "-" + month_begin
        year_month_day = year_month + "-" + day_begin
        fecha = datetime.datetime.strptime(year_month_day, "%Y-%B-%d")
        mes = fecha.strftime('%m')
        anio = fecha.strftime('%Y')
        anio_mes_ori = anio + "-" + mes
        date_ori = fecha.strftime('%Y-%m-%d')
        year_month = year + "-" + month_end
        year_month_day = year_month + "-" + day_end
        fecha = datetime.datetime.strptime(year_month_day, "%Y-%B-%d")
        mes = fecha.strftime('%m')
        anio = fecha.strftime('%Y')
        anio_mes_end = anio + "-" + mes
        date_end = fecha.strftime('%Y-%m-%d')
    except:
        util.log(local, 'Did not found a date format on the link = {}'.format(tariff_link))
        print('Did not found a date format on the link = {}'.format(tariff_link))
        valor = False
        return valor, last_iteration, data

    if year <= date_limit:
        valor = False
        last_iteration = True
        return valor, last_iteration, data

    df_tabla = arma_desde_pagina(path, link_title, page_number, local)

    # Elimina filas con valores nulos en la tabla
    df_tabla = df_tabla.dropna(how='all')
    df_tabla.reset_index(drop=True, inplace=True)

    lista_csv = pagina_parser(df_tabla, pais, tariff_link, pdf_link, link_title, date_ori, date_end, local)

    valor, last_iteration, lis_data = grabacion_archivos(data, lista_csv, pais, tariff_link, link_title,
                                                        pdf_link, anio_mes_ori, anio_mes_end, beginning_time, local)

    return valor, last_iteration, lis_data


# -------------------------------------------------------------------------------
# Clase para iterar recursivamente en los diferentes niveles de opciones del html
# -------------------------------------------------------------------------------
class lista_recursiva:

    def __init__(self, s, soup, base_link, last_iteration, lista_urls, urls_pdfs, inicio, local):
        self.s = s
        self.soup = soup
        self.base_link = base_link
        self.last_iteration = last_iteration
        self.lista_urls = lista_urls
        self.urls_pdfs = urls_pdfs
        self.inicio = inicio
        self.local = local

    # -------------------------------------------------------------------
    # Busqueda de links por nivel de profundidad
    # -------------------------------------------------------------------
    def _build_deep_links(self):
        nombre = self.next_link

        table_with_links = self.soup.find_all('a', attrs={'href': True})

        # check if the table has the links
        if not table_with_links:
            valor = False
            return valor, self.urls_pdfs, self.last_iteration

        links = []
        for tag in table_with_links:
            try:
                link = tag.get("href").strip("=")
                if "#" in link:
                    continue
                links.append(link)
            except:
                continue

        lista_urls = []
        no_val = nombres_no_validos()
        for t, tar in enumerate(links):
            esta = False
            for word in no_val:
                if word in tar:
                    esta = True
                    break
            if esta:
                continue

            if ".pdf" in tar:
                self.urls_pdfs.append(nombre)
                if 'https:' not in tar:
                    self.urls_pdfs.append(self.base_link + tar)
                else:
                    self.urls_pdfs.append(tar)

        if len(lista_urls) <= 3:
            self.last_iteration = True
            return self.urls_pdfs, self.last_iteration

        inicio = 3
        deep_links = lista_recursiva(
            self.s,
            self.soup,
            self.last_iteration,
            lista_urls,
            self.urls_pdfs,
            inicio,
            self.local
        )

        self.valor, self.urls_pdfs, self.last_iteration = deep_links.loop_list_links()

        if self.last_iteration:
            self.last_iteration = False
            return self.urls_pdfs, self.last_iteration

    # -------------------------------------------------------------------
    # Recursividad y control  en links encontrados
    # -------------------------------------------------------------------
    def loop_list_links(self):
        for tl in range(self.inicio, len(self.lista_urls)):
            self.valor, self.soup, response, status_code = util.status_code_url(self.s, self.lista_urls[tl], self.local)

            if status_code != 200:
                util.log(self.local, 'Not logged on the right url = {}'.format(self.lista_urls[tl]))
                self.valor = False
                return self.valor, self.urls_pdfs, self.last_iteration

            self.next_link = self.lista_urls[tl]

            self.urls_pdfs, self.last_iteration = self._build_deep_links()

        self.last_iteration = True
        return self.valor, self.urls_pdfs, self.last_iteration


# -------------------------------------------
# Main de el Scraper de Panama
# -------------------------------------------
def panama_asep_tariff(output_path, event: dict, local):
    pais = local.split("_")[-1]
    date_limit = "2012"

    util.set_event(event)

    try:
        # create dir to save results if it is not created
        util.create_s3dirs()

        data, past_links = util.lectura_links_procesados(output_path, local)
        util.log(local, 'Started the scraping of Panama tariffs')

        try:
            # urls
            base_link = 'https://www.asep.gob.pa'
            next_link = 'https://www.asep.gob.pa/?page_id=12686 Estructura Tarifaria por mes/año y distribuidora.Son pdfs'
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

                # table_with_links = soup.find_all('a', attrs={'href': True})
                table_with_links = soup.find_all("div", {"class": "fusion-text"})

                # check if the table has the links
                if not table_with_links:
                    util.log(local, 'Not logged on the right url = {}'.format(next_link))
                    return False

                links = []
                for tag in table_with_links:
                    links.extend(tag.findAll('a'))

                lista_urls = []
                for tar in links:
                    if 'Tarifas del Sector Eléctrico: Año 2023-2026' not in tar.contents[0]:
                        link = base_link + tar.get("href").strip("=")
                        lista_urls.append(link)
                    else:
                        link = tar.get("href").strip("=")
                        lista_urls.append(link)

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
                    local
                )

                valor, urls_pdfs, last_iteration = deep_links.loop_list_links()

                if not valor:
                    return False

                last_iteration = False
                pdf_ant = np.nan
                for pdf in urls_pdfs:
                    pdf_link = pdf

                    if ".pdf" not in pdf_link:
                        tariff_link = pdf_link
                        continue

                    if pdf_link in past_links:
                        continue

                    if pdf_ant is np.nan:
                        pdf_ant = pdf_link
                    elif pdf_ant == pdf_link:
                        continue
                    else:
                        pdf_ant = pdf_link

                    # -----------------
                    # Grabación PDFs
                    # -----------------
                    link_title = pdf_link.split("/")[-1]
                    link_title = link_title.strip()
                    year = link_title.strip().split("_")[-1].split(".")[0]
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
                    valor, last_iteration, data = arma_csv(data, path, local, pais, tariff_link,
                                                        link_title, pdf_link, date_limit, last_iteration)

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
                util.log(local, 'Ended with the time of scraping of Panama tariff tables')
                return True

        except:
            return False
        finally:
            # Move the save to this point to improve performance avoiding write to S3 per each item in the list
            # save the new processed link in data
            util.dump_files_s3(data, output_path, local)

    except:
        return False
