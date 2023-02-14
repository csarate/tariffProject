import datetime
import os
import sys
import time
import warnings

import numpy as np
import pandas as pd

from scrapers.utils_test import util

sys.path.append('./scrapers')
warnings.filterwarnings("ignore")


def tarifas_power():
    dict_t = \
        {
            'BTS 1': '0 - 600 0 - 15',
            'BTS 2': '0 - 600 0 - 15',
            'BTS 3': '0 - 600 0 - 15',
            'Prepago': '0 - 600 0 - 15',
            'BTD': '0 - 600 15 - 999999',
            'BTH': '0 - 600 15 - 999999',
            'MTD': '601 - 115000 0 - 999999',
            'MTH': '601 - 115000 0 - 999999',
            'ATD': '115000 - 999999 0 - 999999',
            'ATH': '115000 - 999999 0 - 999999',
        }
    return dict_t


# #### Funtion definition
# ### Voltage ranges
# #### Funtion definition
# ### Voltage ranges
def dicTarVol(df):
    #### Power ranges
    power_range = tarifas_power()
    pd.set_option('float_format', '{:.2f}'.format)
    power = pd.Series(power_range)

    ds = power.astype('str').str.extractall(pat='([-+]?\d*\.\d+|\d+)').unstack().fillna(999999).astype('f')

    ds.columns = ["emin", "emax", "pmin", "pmax"]

    ds.round(decimals=2)

    return ds.to_dict()


# #### Funtion definition
# ### Energy ranges
def dicTarEn(df):
    lstcomponent = df.drop_duplicates().sort_values().reset_index(drop=True)
    lstcomponent.index = lstcomponent

    ds = lstcomponent.astype('str').str.extractall('(\d+)').astype('int').unstack().fillna(999999).astype('int')
    ds.columns = ["min", "max"]

    return ds.to_dict()


def ini_consolidate(status_file, local):
    # read already processed and saved links so as not to repeat
    data = util.read_files_s3(status_file, local)
    if not data:
        data = []

    # get the already prcessed links in second position
    past_links = []
    for line in data:
        try:
            past_links.append(line[1])
        except IndexError as e:
            util.log('badly formed link on saved links = {}'.format(line))
    return data, past_links


def consolidate_tariff(status_file, event: dict, local):
    util.set_event(event)

    msg = '####### Cosolidate Tariff #######'
    util.log(local, msg)

    data, past_links = ini_consolidate(status_file, local)

    beginning_time = time.time()

    # ## Importing and consolidation data
    # ### get the columns dic of the result table
    col_dict = util.tariff_columns_dict()
    AllCol = list(col_dict.keys())
    DS_IDB_Result = pd.DataFrame()

    # ### Create a DF from all csv files in the paste
    # Create a dataframe from multiple files
    arq_all = util.list_files('csv*', local)
    arq_tobeprocess = []

    for file in arq_all:
        if file in past_links:
            msg = file + ' table already processed'
            util.log(local, msg)
        else:
            msg = file + ' table to be processed'
            util.log(local, msg)
            arq_tobeprocess.append(file)

    if len(arq_tobeprocess) == 0:
        msg = 'Consolidation of Panamá tariff tables does not add files '
        util.log(local, msg)
        return True

    arq_chunk = list(util.split_list(arq_tobeprocess, 110))

    dicDayTime = {'start': {'Peak': '5:00', 'OffPeak': '21:00'},
                  'end': {'Peak': '20:59', 'OffPeak': '4:59'}}

    chunk = 0
    while not util.execution_time_out():

        # ### Tariff table download
        arq = arq_chunk[chunk]

        # Create a dataframe from multiple files

        try:
            TariffCode = ['BTS 1', 'BTS 2', 'BTS 3', 'Prepago']
            DS = pd.DataFrame()
            for f in arq:
                nombre = os.path.basename(f)
                df = util.get_df_from_csv(**{"str_csv_file": f, "sep": ',', "decimal": ".", "encoding": 'utf-8 ',
                                             "parse_dates": ['Date_start', 'Date_end'], "dayfirst": False})
                df['Document_name'] = nombre
                DS = DS.append(df, ignore_index=False)

            DS = DS.rename(columns={'$local': 'CURRENCY', 'Utility': 'UTILITY', 'Country': 'COUNTRY'})
            DS[DS.Tariff_code.str.contains('|'.join(TariffCode)).fillna(False)]

            # Active Tariff
            DS_T1 = DS[(DS.Tariff_code.str.contains('|'.join(TariffCode)))]
            DS_T1 = DS_T1.reset_index(drop=True)

            # ### Creating columns IDB table format
            # df = DS_T1.Tariff_code.str.cat(DS_T1.Charge_type, sep='-')

            df = DS_T1['Tariff_code']
            DS_T1['Voltage_trench_start'] = df.map(dicTarVol(df)['emin']).fillna(0).astype('float')
            DS_T1['Voltage_trench_end'] = df.map(dicTarVol(df)['emax']).fillna(9999.99).astype('float')

            DS_T1['Power_trench_start'] = df.map(dicTarVol(df)['pmin']).fillna(0).astype('float')
            DS_T1['Power_trench_end'] = df.map(dicTarVol(df)['pmax']).fillna(9999.99).astype('float')

            df = DS_T1['Component']
            DS_T1['Volumetric_trench_start'] = df.map(dicTarEn(df)['min']).fillna(0).astype('int')
            DS_T1['Volumetric_trench_end'] = df.map(dicTarEn(df)['max']).fillna(9999.99).astype('int')

            DS_T1['Componente_IDB'] = \
                np.where((DS_T1.Tariff_type.str.upper().str.contains('FACTURA') &
                          DS_T1.Charge_type.str.upper().str.contains('FIJO')),
                         'Charges_component_fixed',
                         np.where((DS_T1.Tariff_type.str.upper().str.contains('FACTURA') &
                                   DS_T1.Charge_type.str.upper().str.contains('ENERG')),
                                  'Volumetric_total',
                                  np.where((DS_T1.Tariff_type.str.upper().str.contains('FACTURA') &
                                            DS_T1.Charge_type.str.upper().str.contains('DEMANDA')),
                                           'Realized_power_rate',
                                           np.where((DS_T1.Tariff_type.str.upper().str.contains('COMERCIALIZA') &
                                                     DS_T1.Charge_type.str.upper().str.contains('FIJO')),
                                                    'Charges_component_commercialization',
                                                    np.where(
                                                        (DS_T1.Tariff_type.str.upper().str.contains('COMERCIALIZA') &
                                                         DS_T1.Charge_type.str.upper().str.contains('ENERG')),
                                                        'Volumetric_energy_commercialization',
                                                        np.where(
                                                            (DS_T1.Tariff_type.str.upper().str.contains('DISTRIBUC') &
                                                             DS_T1.Charge_type.str.upper().str.contains('POR ENERG')),
                                                            'Volumetric_distribution_component',
                                                            np.where((DS_T1.Tariff_type.str.upper().str.contains(
                                                                'DISTRIBUC') &
                                                                      DS_T1.Charge_type.str.upper().str.contains(
                                                                          'POR DEMANDA')),
                                                                     'Power_distribution_component',
                                                                     np.where((
                                                                                          DS_T1.Tariff_type.str.upper().str.contains(
                                                                                              'DISTRIBUC') &
                                                                                          DS_T1.Charge_type.str.upper().str.contains(
                                                                                              'POR PÉRDIDA')),
                                                                              'Volumetric_losses_component',
                                                                              np.where(
                                                                                  DS_T1.Tariff_type.str.upper().str.contains(
                                                                                      'ALUMBRADO'),
                                                                                  'Street_lighting_component_variable',
                                                                                  np.where((
                                                                                                       DS_T1.Tariff_type.str.upper().str.contains(
                                                                                                           'TRANSMIS') &
                                                                                                       DS_T1.Charge_type.str.upper().str.contains(
                                                                                                           'POR ENERG')),
                                                                                           'Volumetric_transmission_component',
                                                                                           np.where((
                                                                                                                DS_T1.Tariff_type.str.upper().str.contains(
                                                                                                                    'TRANSMIS') &
                                                                                                                ((
                                                                                                                            DS_T1.Charge_type.str.upper().str.contains(
                                                                                                                                'POR DEMANDA') |
                                                                                                                            DS_T1.Charge_type.str.upper().str.contains(
                                                                                                                                'POR PÉRDIDA')))),
                                                                                                    'Power_transmission_component',
                                                                                                    np.where((
                                                                                                                         DS_T1.Tariff_type.str.upper().str.contains(
                                                                                                                             'GENERAC') &
                                                                                                                         DS_T1.Charge_type.str.upper().str.contains(
                                                                                                                             'DEMANDA')),
                                                                                                             'Power_energy_component',
                                                                                                             'Volumetric_energy_component'
                                                                                                             ))))))))))))

            DS_T1['Billing_frequency'] = 'monthly'

            DS_T1['Voltage_group'] = np.where(DS_T1.Tariff_code.str.contains('MT'), 'Medium Voltage',
                                              np.where(DS_T1.Tariff_code.str.contains('AT'), 'High Voltage',
                                                       'Low Voltage'))

            try:
                DS_T1['Charge'] = pd.to_numeric(DS_T1['Charge'], errors='coerce')
            except Exception as e:
                print("Error de Conversion del Charge")
                print(DS_T1.iloc[DS_T1[DS_T1.isna()].index].Charge)
                return False

            # Group by duplicates variables
            col_id = ['COUNTRY', 'Document_link', 'PDF_Name', 'Tariff_code', 'UTILITY',
                      'Date_start', 'Date_end', 'CURRENCY', 'Document_name',
                      'Voltage_trench_start', 'Voltage_trench_end', 'Power_trench_start', 'Power_trench_end',
                      'Volumetric_trench_start', 'Volumetric_trench_end', 'Billing_frequency', 'Voltage_group',
                      'Componente_IDB']

            DS_T1 = DS_T1.groupby(col_id).agg(Charge=('Charge', 'sum')).reset_index()

            # ##### pivot table by IDB component (from file concept table to columns concept)
            DS_T1_PIV = DS_T1.pivot_table(
                values='Charge',
                index=['COUNTRY', 'Document_link', 'PDF_Name', 'Tariff_code', 'UTILITY',
                       'Date_start', 'Date_end', 'CURRENCY', 'Document_name',
                       'Voltage_trench_start', 'Voltage_trench_end', 'Power_trench_start', 'Power_trench_end',
                       'Volumetric_trench_start', 'Volumetric_trench_end',
                       'Billing_frequency', 'Voltage_group'],
                columns='Componente_IDB')

            DS_T1_PIV = DS_T1_PIV.reset_index()

            #### Final Group by necesary variables
            col_id = ['COUNTRY', 'Document_link', 'PDF_Name', 'Tariff_code', 'UTILITY',
                      'Date_start', 'Date_end', 'CURRENCY', 'Document_name',
                      'Voltage_trench_start', 'Voltage_trench_end', 'Power_trench_start', 'Power_trench_end',
                      'Volumetric_trench_start', 'Volumetric_trench_end',
                      'Billing_frequency', 'Voltage_group']

            DS_T1_IDB = DS_T1_PIV.groupby(col_id).agg(
                Charges_component_fixed=('Charges_component_fixed', 'sum'),
                Volumetric_total=('Volumetric_total', 'sum'),
                Charges_component_commercialization=('Charges_component_commercialization', 'sum'),
                Volumetric_energy_commercialization=('Volumetric_energy_commercialization', 'sum'),
                Volumetric_distribution_component=('Volumetric_distribution_component', 'sum'),
                Street_lighting_component_variable=('Street_lighting_component_variable', 'sum'),
                Volumetric_transmission_component=('Volumetric_transmission_component', 'sum'),
                Volumetric_energy_component=('Volumetric_energy_component', 'sum')).reset_index()

            DS_T1_IDB = DS_T1_IDB.reset_index()

            DS_T1_IDB['Structure_subtype'] = np.where((DS_T1_IDB['Volumetric_trench_start'] == 0) &
                                                      (DS_T1_IDB['Volumetric_trench_end'] == 999999), 'Flat rate',
                                                      'Block Increasing rate')
            DS_T1_IDB['Sector'] = 'General'

            #### Consolidation of the Dataset

            DS_IDB = pd.concat([DS_T1_IDB]).reset_index(drop=True)

            ##### Aditional info columns

            DS_IDB['Taxes_link'] = 'https://ciatorg.sharepoint.com/:x:/r/sites/cds/_layouts/15/guestaccess.aspx?docid' \
                                   '=08814180f3de44115bee96760999fb11f&authkey=Ac9cc1KJHzZyZOTz1SCEE-w&e' \
                                   '=e2fb160be70847908c740d7f25d702c7 '
            DS_IDB['Taxes_component_percentual'] = 0.07
            DS_IDB['Existence_volumetric_rate_w_minimum'] = 0
            DS_IDB['Existence_fixed_charge'] = 1
            DS_IDB['Tax_included'] = 0

            # #### Adding columns with nan to complete the dataset
            lst = [x.upper() for x in AllCol]
            DS_IDB.rename(columns={'Tariff_code': 'Tariff_Name_in_document'}, inplace=True)
            columnadd = list(set(AllCol).difference(DS_IDB.columns))

            DS_IDB[columnadd] = np.nan
            DS_IDB_Result = pd.concat([DS_IDB_Result, DS_IDB])

            # #######################################################
            # ### Working by tariff T2
            # #######################################################

            TariffCode = ['BTD', 'MTD', 'ATD']

            DS_T2 = DS[(DS.Tariff_code.str.contains('|'.join(TariffCode)))]

            df = DS_T2['Tariff_code']
            DS_T2['Voltage_trench_start'] = df.map(dicTarVol(df)['emin']).fillna(0).astype('float')
            DS_T2['Voltage_trench_end'] = df.map(dicTarVol(df)['emax']).fillna(9999.99).astype('float')

            DS_T2['Power_trench_start'] = df.map(dicTarVol(df)['pmin']).fillna(0).astype('float')
            DS_T2['Power_trench_end'] = df.map(dicTarVol(df)['pmax']).fillna(9999.99).astype('float')

            df = DS_T2['Component']
            DS_T2['Volumetric_trench_start'] = df.map(dicTarEn(df)['min']).fillna(0).astype('int')
            DS_T2['Volumetric_trench_end'] = df.map(dicTarEn(df)['max']).fillna(9999.99).astype('int')

            DS_T2['Componente_IDB'] = \
                np.where((DS_T2.Tariff_type.str.upper().str.contains('FACTURA') &
                          DS_T2.Charge_type.str.upper().str.contains('FIJO')),
                         'Charges_component_fixed',
                         np.where((DS_T2.Tariff_type.str.upper().str.contains('FACTURA') &
                                   DS_T2.Charge_type.str.upper().str.contains('ENERG')),
                                  'Volumetric_total',
                                  np.where((DS_T2.Tariff_type.str.upper().str.contains('FACTURA') &
                                            DS_T2.Charge_type.str.upper().str.contains('DEMANDA')),
                                           'Realized_power_rate',
                                           np.where((DS_T2.Tariff_type.str.upper().str.contains('COMERCIALIZA') &
                                                     DS_T2.Charge_type.str.upper().str.contains('FIJO')),
                                                    'Charges_component_commercialization',
                                                    np.where(
                                                        (DS_T2.Tariff_type.str.upper().str.contains('COMERCIALIZA') &
                                                         DS_T2.Charge_type.str.upper().str.contains('ENERG')),
                                                        'Volumetric_energy_commercialization',
                                                        np.where(
                                                            (DS_T2.Tariff_type.str.upper().str.contains('DISTRIBUC') &
                                                             DS_T2.Charge_type.str.upper().str.contains('POR ENERG')),
                                                            'Volumetric_distribution_component',
                                                            np.where((DS_T2.Tariff_type.str.upper().str.contains(
                                                                'DISTRIBUC') &
                                                                      DS_T2.Charge_type.str.upper().str.contains(
                                                                          'POR DEMANDA')),
                                                                     'Power_distribution_component',
                                                                     np.where((
                                                                                          DS_T2.Tariff_type.str.upper().str.contains(
                                                                                              'DISTRIBUC') &
                                                                                          DS_T2.Charge_type.str.upper().str.contains(
                                                                                              'POR PÉRDIDA')),
                                                                              'Volumetric_losses_component',
                                                                              np.where(
                                                                                  DS_T2.Tariff_type.str.upper().str.contains(
                                                                                      'ALUMBRADO'),
                                                                                  'Street_lighting_component_variable',
                                                                                  np.where((
                                                                                                       DS_T2.Tariff_type.str.upper().str.contains(
                                                                                                           'TRANSMIS') &
                                                                                                       DS_T2.Charge_type.str.upper().str.contains(
                                                                                                           'POR ENERG')),
                                                                                           'Volumetric_transmission_component',
                                                                                           np.where((
                                                                                                                DS_T2.Tariff_type.str.upper().str.contains(
                                                                                                                    'TRANSMIS') &
                                                                                                                ((
                                                                                                                            DS_T2.Charge_type.str.upper().str.contains(
                                                                                                                                'POR DEMANDA') |
                                                                                                                            DS_T2.Charge_type.str.upper().str.contains(
                                                                                                                                'POR PÉRDIDA')))),
                                                                                                    'Power_transmission_component',
                                                                                                    np.where((
                                                                                                                         DS_T2.Tariff_type.str.upper().str.contains(
                                                                                                                             'GENERAC') &
                                                                                                                         DS_T2.Charge_type.str.upper().str.contains(
                                                                                                                             'DEMANDA')),
                                                                                                             'Power_energy_component',
                                                                                                             'Volumetric_energy_component'
                                                                                                             ))))))))))))

            DS_T2['Billing_frequency'] = 'monthly'

            DS_T2['Voltage_group'] = np.where(DS_T2.Tariff_code.str.contains('MT'), 'Medium Voltage',
                                              np.where(DS_T2.Tariff_code.str.contains('AT'), 'High Voltage',
                                                       'Low Voltage'))

            try:
                DS_T2['Charge'] = pd.to_numeric(DS_T2['Charge'], errors='coerce')
            except Exception as e:
                print("Error de Conversion del Charge")
                print(DS_T2.iloc[DS_T2[DS_T2.isna()].index].Charge)
                return False

            # Group by duplicates variables
            col_id = ['COUNTRY', 'Document_link', 'PDF_Name', 'Tariff_code', 'UTILITY',
                      'Date_start', 'Date_end', 'CURRENCY', 'Document_name',
                      'Voltage_trench_start', 'Voltage_trench_end', 'Power_trench_start', 'Power_trench_end',
                      'Volumetric_trench_start', 'Volumetric_trench_end', 'Billing_frequency', 'Voltage_group',
                      'Componente_IDB']

            DS_T2 = DS_T2.groupby(col_id).agg(Charge=('Charge', 'sum')).reset_index()

            # ##### pivot table by IDB component (from file concept table to columns concept)
            DS_T2_PIV = DS_T2.pivot_table(
                values='Charge',
                index=['COUNTRY', 'Document_link', 'PDF_Name', 'Tariff_code', 'UTILITY',
                       'Date_start', 'Date_end', 'CURRENCY', 'Document_name',
                       'Voltage_trench_start', 'Voltage_trench_end', 'Power_trench_start', 'Power_trench_end',
                       'Volumetric_trench_start', 'Volumetric_trench_end',
                       'Billing_frequency', 'Voltage_group'],
                columns='Componente_IDB')

            #### Final Group by necesary variables
            col_id = ['COUNTRY', 'Document_link', 'PDF_Name', 'Tariff_code', 'UTILITY',
                      'Date_start', 'Date_end', 'CURRENCY', 'Document_name',
                      'Voltage_trench_start', 'Voltage_trench_end', 'Power_trench_start', 'Power_trench_end',
                      'Volumetric_trench_start', 'Volumetric_trench_end',
                      'Billing_frequency', 'Voltage_group']
            DS_T2_IDB = DS_T2_PIV.groupby(col_id).agg(
                Charges_component_fixed=('Charges_component_fixed', 'sum'),
                Volumetric_total=('Volumetric_total', 'sum'),
                Realized_power_rate=('Realized_power_rate', 'sum'),
                Charges_component_commercialization=('Charges_component_commercialization', 'sum'),
                Volumetric_energy_commercialization=('Volumetric_energy_commercialization', 'sum'),
                Volumetric_distribution_component=('Volumetric_distribution_component', 'sum'),
                Power_distribution_component=('Power_distribution_component', 'sum'),
                Volumetric_losses_component=('Volumetric_losses_component', 'sum'),
                Street_lighting_component_variable=('Street_lighting_component_variable', 'sum'),
                Volumetric_transmission_component=('Volumetric_transmission_component', 'sum'),
                Power_transmission_component=('Power_transmission_component', 'sum'),
                Power_energy_component=('Power_energy_component', 'sum'),
                Volumetric_energy_component=('Volumetric_energy_component', 'sum')).reset_index()

            DS_T2_IDB = DS_T2_IDB.reset_index()

            DS_T2_IDB['Structure_subtype'] = np.where((DS_T2_IDB['Volumetric_trench_start'] == 0) &
                                                      (DS_T2_IDB['Volumetric_trench_end'] == 999999), 'Flat rate',
                                                      'Block Increasing rate')
            DS_T2_IDB['Sector'] = 'General'

            #### Consolidation of the Dataset

            DS_IDB = pd.concat([DS_T2_IDB]).reset_index(drop=True)

            ##### Aditional info columns

            DS_IDB['Taxes_link'] = 'https://ciatorg.sharepoint.com/:x:/r/sites/cds/_layouts/15/guestaccess.aspx?docid' \
                                   '=08814180f3de44115bee96760999fb11f&authkey=Ac9cc1KJHzZyZOTz1SCEE-w&e' \
                                   '=e2fb160be70847908c740d7f25d702c7 '
            DS_IDB['Taxes_component_percentual'] = 0.07
            DS_IDB['Existence_volumetric_rate_w_minimum'] = 0
            DS_IDB['Existence_fixed_charge'] = 1
            DS_IDB['Tax_included'] = 0

            # #### Adding columns with nan to complete the dataset
            lst = [x.upper() for x in AllCol]
            DS_IDB.rename(columns={'Tariff_code': 'Tariff_Name_in_document'}, inplace=True)
            columnadd = list(set(AllCol).difference(DS_IDB.columns))

            DS_IDB[columnadd] = np.nan
            DS_IDB_Result = pd.concat([DS_IDB_Result, DS_IDB])

            # #######################################################
            # ### Working by tariff T3
            # #######################################################

            TariffCode = ['BTH', 'MTH', 'ATH']
            DS_T3 = DS[(DS.Tariff_code.str.contains('|'.join(TariffCode)))]

            df = DS_T3['Tariff_code']
            DS_T3['Voltage_trench_start'] = df.map(dicTarVol(df)['emin']).fillna(0).astype('float')
            DS_T3['Voltage_trench_end'] = df.map(dicTarVol(df)['emax']).fillna(9999.99).astype('float')

            DS_T3['Power_trench_start'] = df.map(dicTarVol(df)['pmin']).fillna(0).astype('float')
            DS_T3['Power_trench_end'] = df.map(dicTarVol(df)['pmax']).fillna(9999.99).astype('float')

            df = DS_T3['Component']
            DS_T3['Volumetric_trench_start'] = df.map(dicTarEn(df)['min']).fillna(0).astype('int')
            DS_T3['Volumetric_trench_end'] = df.map(dicTarEn(df)['max']).fillna(9999.99).astype('int')

            DS_T3['Componente_IDB'] = \
                np.where((DS_T3.Tariff_type.str.upper().str.contains('FACTURA') &
                          DS_T3.Charge_type.str.upper().str.contains('FIJO') &
                          DS_T3.Charge_type.str.upper().str.contains('PUNTA')),
                         'Charges_component_fixed',
                         np.where((DS_T3.Tariff_type.str.upper().str.contains('FACTURA') &
                                   DS_T3.Charge_type.str.upper().str.contains('ENERG') &
                                   DS_T3.Charge_type.str.upper().str.contains('PUNTA')),
                                  'Volumetric_total',
                                  np.where((DS_T3.Tariff_type.str.upper().str.contains('FACTURA') &
                                            DS_T3.Charge_type.str.upper().str.contains('DEMANDA') &
                                            DS_T3.Charge_type.str.upper().str.contains('PUNTA')),
                                           'Realized_power_rate',
                                           np.where((DS_T3.Tariff_type.str.upper().str.contains('COMERCIALIZA') &
                                                     DS_T3.Charge_type.str.upper().str.contains('FIJO') &
                                                     DS_T3.Charge_type.str.upper().str.contains('PUNTA')),
                                                    'Charges_component_commercialization',
                                                    np.where(
                                                        (DS_T3.Tariff_type.str.upper().str.contains('COMERCIALIZA') &
                                                         DS_T3.Charge_type.str.upper().str.contains('ENERG') &
                                                         DS_T3.Charge_type.str.upper().str.contains('PUNTA')),
                                                        'Volumetric_energy_commercialization',
                                                        np.where(
                                                            (DS_T3.Tariff_type.str.upper().str.contains('DISTRIBUC') &
                                                             DS_T3.Charge_type.str.upper().str.contains('DEMANDA') &
                                                             DS_T3.Charge_type.str.upper().str.contains('PUNTA')),
                                                            'Power_distribution_component',
                                                            np.where((DS_T3.Tariff_type.str.upper().str.contains(
                                                                'DISTRIBUC') &
                                                                      DS_T3.Charge_type.str.upper().str.contains(
                                                                          'PÉRDIDA') &
                                                                      DS_T3.Charge_type.str.upper().str.contains(
                                                                          'PUNTA')),
                                                                     'Volumetric_losses_component',
                                                                     np.where((
                                                                                          DS_T3.Tariff_type.str.upper().str.contains(
                                                                                              'ALUMBRADO') &
                                                                                          DS_T3.Charge_type.str.upper().str.contains(
                                                                                              'PUNTA')),
                                                                              'Street_lighting_component_variable',
                                                                              np.where((
                                                                                                   DS_T3.Tariff_type.str.upper().str.contains(
                                                                                                       'TRANSMIS') &
                                                                                                   DS_T3.Charge_type.str.upper().str.contains(
                                                                                                       'DEMANDA') &
                                                                                                   DS_T3.Charge_type.str.upper().str.contains(
                                                                                                       'PUNTA')),
                                                                                       'Power_transmission_component',
                                                                                       np.where((
                                                                                                            DS_T3.Tariff_type.str.upper().str.contains(
                                                                                                                'TRANSMIS') &
                                                                                                            DS_T3.Charge_type.str.upper().str.contains(
                                                                                                                'POR PÉRDIDA') &
                                                                                                            DS_T3.Charge_type.str.upper().str.contains(
                                                                                                                'PUNTA')),
                                                                                                'Volumetric_transmission_component',
                                                                                                np.where((
                                                                                                                     DS_T3.Tariff_type.str.upper().str.contains(
                                                                                                                         'GENERAC') &
                                                                                                                     DS_T3.Charge_type.str.upper().str.contains(
                                                                                                                         'ENERG') &
                                                                                                                     DS_T3.Charge_type.str.upper().str.contains(
                                                                                                                         'PUNTA')),
                                                                                                         'Volumetric_energy_component',
                                                                                                         np.where((
                                                                                                                              DS_T3.Tariff_type.str.upper().str.contains(
                                                                                                                                  'GENERAC') &
                                                                                                                              DS_T3.Charge_type.str.upper().str.contains(
                                                                                                                                  'DEMANDA') &
                                                                                                                              DS_T3.Charge_type.str.upper().str.contains(
                                                                                                                                  'PUNTA')),
                                                                                                                  'Power_energy_component',
                                                                                                                  'Volumetric_energy_component'))))))))))))

            DS_T3['Billing_frequency'] = 'monthly'

            DS_T3['Voltage_group'] = np.where(DS_T3.Tariff_code.str.contains('MT'), 'Medium Voltage',
                                              np.where(DS_T3.Tariff_code.str.contains('AT'), 'High Voltage',
                                                       'Low Voltage'))

            DS_T3['Daytime_group'] = np.where(DS_T3.Charge_type.str.upper().str.contains('EN PUNTA'), 'Peak',
                                              np.where(DS_T3.Charge_type.str.upper().str.contains('DE PUNTA'),
                                                       'OffPeak', ''))

            try:
                DS_T3['Charge'] = pd.to_numeric(DS_T3['Charge'], errors='coerce')
            except Exception as e:
                print("Error de Conversion del Charge")
                print(DS_T3.iloc[DS_T3[DS_T3.isna()].index].Charge)
                return False

            # Group by duplicates variables
            col_id = ['COUNTRY', 'Document_link', 'PDF_Name', 'Tariff_code', 'UTILITY',
                      'Date_start', 'Date_end', 'CURRENCY', 'Document_name',
                      'Voltage_trench_start', 'Voltage_trench_end', 'Power_trench_start', 'Power_trench_end',
                      'Volumetric_trench_start', 'Volumetric_trench_end', 'Billing_frequency',
                      'Voltage_group', 'Charge_type', 'Daytime_group', 'Componente_IDB']

            DS_T3 = DS_T3.groupby(col_id).agg(Charge=('Charge', 'sum')).reset_index()

            # ##### pivot table by IDB component (from file concept table to columns concept)
            DS_T3_PIV = DS_T3.pivot_table(
                values='Charge',
                index=['COUNTRY', 'Document_link', 'PDF_Name', 'Tariff_code', 'UTILITY',
                       'Date_start', 'Date_end', 'CURRENCY', 'Document_name',
                       'Voltage_trench_start', 'Voltage_trench_end', 'Power_trench_start', 'Power_trench_end',
                       'Volumetric_trench_start', 'Volumetric_trench_end',
                       'Billing_frequency', 'Voltage_group', 'Daytime_group'],
                columns='Componente_IDB')

            #### Final Group by necesary variables
            col_id = ['COUNTRY', 'Document_link', 'PDF_Name', 'Tariff_code', 'UTILITY',
                      'Date_start', 'Date_end', 'CURRENCY', 'Document_name',
                      'Voltage_trench_start', 'Voltage_trench_end', 'Power_trench_start', 'Power_trench_end',
                      'Volumetric_trench_start', 'Volumetric_trench_end',
                      'Billing_frequency', 'Voltage_group', 'Daytime_group']

            DS_T3_IDB = DS_T3_PIV.groupby(col_id).agg(
                Charges_component_fixed=('Charges_component_fixed', 'sum'),
                Volumetric_total=('Volumetric_total', 'sum'),
                Realized_power_rate=('Realized_power_rate', 'sum'),
                Charges_component_commercialization=('Charges_component_commercialization', 'sum'),
                Volumetric_energy_commercialization=('Volumetric_energy_commercialization', 'sum'),
                Power_distribution_component=('Power_distribution_component', 'sum'),
                Volumetric_losses_component=('Volumetric_losses_component', 'sum'),
                Street_lighting_component_variable=('Street_lighting_component_variable', 'sum'),
                Power_transmission_component=('Power_transmission_component', 'sum'),
                Volumetric_transmission_component=('Volumetric_transmission_component', 'sum'),
                Power_energy_component=('Power_energy_component', 'sum'),
                Volumetric_energy_component=('Volumetric_energy_component', 'sum')).reset_index()

            DS_T3_IDB = DS_T3_IDB.reset_index()

            DS_T3_IDB['Structure_subtype'] = np.where((DS_T3_IDB['Volumetric_trench_start'] == 0) &
                                                      (DS_T3_IDB['Volumetric_trench_end'] == 999999), 'Flat rate',
                                                      'Block Increasing rate')
            DS_T3_IDB['Sector'] = 'General'

            #### Consolidation of the Dataset

            DS_IDB = pd.concat([DS_T3_IDB]).reset_index(drop=True)

            ##### Aditional info columns

            DS_IDB['Daytime_group_time_differentiation_start'] = DS_IDB['Daytime_group'].map(dicDayTime['start'])
            DS_IDB['Daytime_group_time_differentiation_end'] = DS_IDB['Daytime_group'].map(dicDayTime['end'])
            DS_IDB['Taxes_link'] = 'https://ciatorg.sharepoint.com/:x:/r/sites/cds/_layouts/15/guestaccess.aspx?docid' \
                                   '=08814180f3de44115bee96760999fb11f&authkey=Ac9cc1KJHzZyZOTz1SCEE-w&e' \
                                   '=e2fb160be70847908c740d7f25d702c7 '
            DS_IDB['Taxes_component_percentual'] = 0.07
            DS_IDB['Existence_volumetric_rate_w_minimum'] = 0
            DS_IDB['Tax_included'] = 0
            DS_IDB['Existence_power_rate'] = np.where(DS_IDB['Realized_power_rate'].notna(), 1, 0)

            # dicVoltage = {'start':
            #                   {'Low Voltage': 0,
            #                    'Medium Voltage': 601,
            #                    'High Voltage': 115001},
            #               'end':
            #                   {'Low Voltage': 600,
            #                    'Medium Voltage': 115000,
            #                    'High Voltage': 150000}}
            #

            # #### Adding columns with nan to complete the dataset
            lst = [x.upper() for x in AllCol]
            DS_IDB.rename(columns={'Tariff_code': 'Tariff_Name_in_document'}, inplace=True)
            columnadd = list(set(AllCol).difference(DS_IDB.columns))

            DS_IDB[columnadd] = np.nan
            DS_IDB_Result = pd.concat([DS_IDB_Result, DS_IDB])

            # #### Export to Excel Partial
            now = datetime.datetime.now()

            date = now.strftime('%Y%m%d_%H%M%S')

            util.tariff_to_xlsx(date + "chunk " + str(chunk) + "_chile_tariff_db", DS_IDB, local)

            # save the new processed link in data
            for file in arq:
                data.append([date, file])

            util.dump_files_s3(data, status_file, local)

            msg = "Ended with the consolidate of the chunk " + str(
                chunk) + " Chile tariff tables. Execution time = " + str(
                round(time.time() - beginning_time)) + "sec"
            util.log(local, msg)

        except Exception as e:
            msg = 'Consolidating erro = {} '.format(e)
            util.log(local, msg)
            return False

        if chunk == len(arq_chunk) - 1:

            # #### Export to Excel Partial
            now = datetime.datetime.now()

            date = now.strftime('%Y%m%d_%H%M%S_')

            util.tariff_to_xlsx(date + "chile_tariff_db", DS_IDB_Result, local)

            # save the new processed link in data
            for file in arq:
                data.append([date, file])

            util.dump_files_s3(data, status_file, local)

            msg = 'Ended with the consolidation of Chile tariff tables'
            util.log(local, msg)

            return True

        chunk = chunk + 1
