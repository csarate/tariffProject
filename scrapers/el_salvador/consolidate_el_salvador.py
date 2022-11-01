import pandas as pd
import datetime
# import  glob
import os
import time
import sys
import numpy as np
import xlsxwriter
from scrapers.utils_test import util
import warnings

sys.path.append('./scrapers')
warnings.filterwarnings("ignore")

# #### Funtion definition
# ### Energy ranges
def dicTarEn(df):
    #### Power ranges
    lstcomponent = df.drop_duplicates().sort_values().reset_index(drop=True)
    lstcomponent.index = lstcomponent

    ds = lstcomponent.astype('str').str.extractall('(\d+)').unstack().fillna(999999).astype('int')
    ds.columns = ["pmin", "pmax", "aux", "emin", "emax"]
    ds['emax'] = np.where((lstcomponent.str.contains("Bloque 1")), ds['emin'], ds['emax'])
    ds['emin'] = np.where((lstcomponent.str.contains("Bloque 1")), 0, ds['emin'])
    ds['emin'] = np.where((lstcomponent.str.contains("Bloque")), ds['emin'], 0)
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


def consolidate_tariff(time_to_search, status_file, event: dict, local):
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
        msg = 'Consolidation of argentina tariff tables does not add files '
        util.log(local, msg)
        return True

    arq_chunk = list(util.split_list(arq_tobeprocess, 36))

    chunk = 0
    while time.time() - beginning_time < time_to_search:

        # ### Tariff table download
        arq = arq_chunk[chunk]

        # Create a dataframe from multiple files

        try:
            DS = pd.DataFrame()
            for f in arq:
                nombre = os.path.basename(f)
                df = util.get_df_from_csv(**{"str_csv_file": f, "sep": ',', "decimal": ".", "encoding": 'utf-8 ',
                                             "parse_dates": ['Date_start', 'Date_end'], "dayfirst": False})
                df['Document_name'] = nombre
                DS = DS.append(df, ignore_index=True)

            DS = DS.rename(columns={'$local': 'CURRENCY', 'Utility': 'UTILITY', 'Country': 'COUNTRY'})

            # ### Creating columns IDB table format
            df = DS['Tariff_code']
            DS['Volumetric_trench_start'] = df.map(dicTarEn(df)['emin']).fillna(0).astype('int')
            DS['Volumetric_trench_end'] = df.map(dicTarEn(df)['emax']).fillna(999999).astype('int')
            
            DS['Power_trench_start'] = df.map(dicTarEn(df)['pmin']).fillna(0).astype('int')
            DS['Power_trench_end'] = df.map(dicTarEn(df)['pmax']).fillna(999999).astype('int')

            # ## Table formating acording to de IDB table
            # ### Working by tariff
            # #### T1

            DS_T1 = DS[(DS.Tariff_code.str.contains('PEQUE'))]
            DS_T1 = DS_T1.reset_index(drop=True)
            DS_T1['Componente_IDB'] = np.where(DS_T1.Component.str.contains('Energ'), 'Volumetric_energy_component',
                                               np.where(DS_T1.Component.str.contains('Cargo Fijo'),
                                                       'Charges_component_fixed', 'Volumetric_distribution_component'))
            DS_T1['Billing_frequency'] = 'monthly'       # ##### pivot table by IDB component (from file concept table to columns concept)

            DS_T1_IDB = DS_T1.pivot_table(
                values='Charge',
                index=['COUNTRY', 'UTILITY', 'Document_link', 'Document_name', 'Date_start', 'Date_end', 'CURRENCY',
                       'Tariff_code', 'Billing_frequency', 'Volumetric_trench_start', 'Volumetric_trench_end',
                       'Power_trench_start', 'Power_trench_end'],
                columns='Componente_IDB'
            )
            DS_T1_IDB = DS_T1_IDB.reset_index()
            
            DS_T1_IDB['Volumetric_total'] = DS_T1_IDB['Volumetric_energy_component'] + DS_T1_IDB['Volumetric_distribution_component']

            # ##### Aditional info columns
            DS_T1_IDB['Voltage_group'] = 'Low Voltage'
            DS_T1_IDB['Structure_subtype'] = np.where((DS_T1_IDB['Volumetric_trench_start'] == 0) &
                                                      (DS_T1_IDB['Volumetric_trench_end'] == 999999), 'Flat rate',
                                                      'Block Increasing rate')
            DS_T1_IDB['Special_category'] = np.where(DS_T1_IDB.Tariff_code.str.upper().str.contains('ALUMBRADO P'), 'Street Lighting', '')
            DS_T1_IDB['Sector'] = np.where(DS_T1_IDB.Tariff_code.str.upper().str.contains('ALUMBRADO P'), 'Street Lighting',
                                           np.where(DS_T1_IDB.Tariff_code.str.contains('Bloque'), 'Residential',
                                                    np.where(DS_T1_IDB.Tariff_code.str.contains('GENERAL'),'General', '')))

            # ### Working by tariff
            # #### T2

            DS_T2 = DS[(DS.Tariff_code.str.contains('POTENCIA'))]
            DS_T2 = DS_T2.reset_index(drop=True)
            DS_T2['Billing_frequency'] = 'monthly'
            
            
            DS_T2['Componente_IDB'] =  np.where(DS_T2.Component.str.contains('Cargo Fijo'), 'Charges_component_fixed',
                                                         np.where(DS_T2.Component.str.contains('Potencia'),
                                                            'Contracted_Power_Rate', 'Volumetric_energy_component'))

            # ##### pivot table by IDB component (from file concept table to columns concept)

            DS_T2_IDB = DS_T2.pivot_table(
                values='Charge',
                index=['COUNTRY', 'UTILITY', 'Document_link', 'Document_name', 'Date_start', 'Date_end', 'CURRENCY',
                       'Tariff_code', 'Billing_frequency', 'Volumetric_trench_start', 'Volumetric_trench_end',
                       'Power_trench_start', 'Power_trench_end'],
                columns='Componente_IDB'
            )
            DS_T2_IDB = DS_T2_IDB.reset_index()
            DS_T2_IDB['Volumetric_total'] = DS_T2_IDB['Volumetric_energy_component']


            # ##### Aditional info columns

            DS_T2_IDB['Structure_subtype'] = 'Flat rate'
            DS_T2_IDB['Voltage_group'] = np.where(DS_T2_IDB.Tariff_code.str.contains('MEDIA '),
                                                'Medium Voltage',
                                                'Low Voltage')
            #DS_T2_IDB['Differentiation_type'] = 'Time-differentiated rate'

            # ### Working by tariff
            # #### T3

            DS_T3 = DS[(DS.Tariff_code.str.contains('HORARIO'))]
            DS_T3 = DS_T3.reset_index(drop=True)
            DS_T3['Billing_frequency'] = 'monthly'

            DS_T3['Daytime_group'] = np.where(DS_T3.Component.str.contains('Resto'),
                                        'Intermediate',
                                            np.where(DS_T3.Component.str.contains('Valle'),
                                                'OffPeak',
                                                    np.where(DS_T3.Component.str.contains('Punta'),
                                                        'Peak',
                                                        'OffPeak')))

            DS_T3['Componente_IDB'] = np.where(DS_T3.Component.str.contains('Energ'),
                                               'Volumetric_energy_component',
                                               np.where((DS_T3.Component.str.contains(
                                                   'Cargo Fijo') | DS_T3.Component.str.contains('Cliente')),
                                                        'Charges_component_fixed',
                                                        np.where(DS_T3.Component.str.contains('Potencia'),
                                                                 'Contracted_Power_Rate', np.nan)))

            # #### copy extra file to have value in all daytime options for each component.
            

            DS_Add_1 = DS_T3[DS_T3.Component.str.contains("|".join(['Cargo Fijo', 'Potencia', 'Cliente']))]\
                                                                    .reset_index(drop=True).copy(deep=True)
            DS_Add_2 = DS_Add_1.copy(deep=True)
            DS_Add_1['Daytime_group'] = 'Peak'
            DS_Add_2['Daytime_group'] = 'Intermediate'

            # DS_CPFP_1 = T3[DS_T3.Component == 'Cargo Pot. F. Pico'].reset_index(drop=True).copy(deep=True)
            #
            # DS_CPFP_1['Daytime_group'] = 'Intermediate'

            # DS_T3 = pd.concat([DS_T3, DS_Add_1, DS_Add_2, DS_CPFP_1])
            DS_T3 = pd.concat([DS_T3, DS_Add_1, DS_Add_2])

            # ##### pivot table by IDB component (from file concept table to columns concept)

            pvtcol = ['COUNTRY', 'UTILITY', 'Document_link', 'Document_name', 'Date_start', 'Date_end', 'CURRENCY',
                      'Tariff_code', 'Billing_frequency', 'Volumetric_trench_start', 'Volumetric_trench_end',
                      'Power_trench_start', 'Power_trench_end', 'Daytime_group']
            DS_T3_IDB = DS_T3.pivot_table(
                values='Charge',
                index=pvtcol,
                columns='Componente_IDB'
            )
            DS_T3_IDB = DS_T3_IDB.reset_index()

            # ##### Aditional info columns
            DS_T3_IDB['Volumetric_total'] = DS_T3_IDB['Volumetric_energy_component']
            DS_T3_IDB['Voltage_group'] = np.where(DS_T3_IDB.Tariff_code.str.contains('MEDIA TENSION'),
                                                           'Medium Voltage',
                                                           'Low Voltage')
            DS_T3_IDB['Structure_subtype'] = 'Flat rate'
            DS_T3_IDB['Differentiation_type'] = 'Time-differentiated rate'

            #### Consolidation of the Dataset

            DS_IDB = pd.concat([DS_T1_IDB, DS_T2_IDB, DS_T3_IDB]).reset_index(drop=True)

            ##### Aditional info columns

            DS_IDB['Taxes_link'] = 'https://ciatorg.sharepoint.com/:x:/r/sites/cds/_layouts/15/guestaccess.aspx?docid' \
                                   '=08814180f3de44115bee96760999fb11f&authkey=Ac9cc1KJHzZyZOTz1SCEE-w&e' \
                                   '=e2fb160be70847908c740d7f25d702c7 '
            DS_IDB['Taxes_component_percentual'] = 0.13
            DS_IDB['Existence_volumetric_rate_w_minimum'] = 0
            DS_IDB['Existence_fixed_charge'] = 1
            DS_IDB['Tax_included'] = 0
            DS_IDB['Existence_power_rate'] = np.where(DS_IDB['Contracted_Power_Rate'].notna(), 1, 0)
  
            dicVoltage = {'start':
                              {'Low Voltage': 0,
                               'Medium Voltage': 601,
                               'High Voltage': 115001},
                          'end':
                              {'Low Voltage': 600,
                               'Medium Voltage': 115000,
                               'High Voltage': 150000}}

            DS_IDB['Voltage_trench_start'] = DS_IDB['Voltage_group'].map(dicVoltage['start'])
            DS_IDB['Voltage_trench_end'] = DS_IDB['Voltage_group'].map(dicVoltage['end'])

            dicDayTime = {'start':
                              {'Peak': '18:00',
                               'Intermediate': '5:00',
                               'OffPeak': '23:00'},
                          'end':
                              {'Peak': '22:59',
                               'Intermediate': '17:59',
                               'OffPeak': '4:59'}}

            DS_IDB['Daytime_group_time_differentiation_start'] = DS_IDB['Daytime_group'].map(dicDayTime['start'])
            DS_IDB['Daytime_group_time_differentiation_end'] = DS_IDB['Daytime_group'].map(dicDayTime['end'])

            # #### Adding columns with nan to complete the dataset

            lst = [x.upper() for x in AllCol]
            DS_IDB.rename(columns={'Tariff_code': 'Tariff_Name_in_document'
                                   }, inplace=True)
            columnadd = list(set(AllCol).difference(DS_IDB.columns))

            DS_IDB[columnadd] = np.nan

            DS_IDB_Result = pd.concat([DS_IDB_Result, DS_IDB])

            # #### Export to Excel Partial
            now = datetime.datetime.now()

            date = now.strftime('%Y%m%d_%H%M%S')

            util.tariff_to_xlsx(date + "chunk " + str(chunk) + "_elsalvador_tariff_db", DS_IDB, local)

            # save the new processed link in data
            for file in arq:
                data.append([date, file])

            util.dump_files_s3(data, status_file, local)

            msg = "Ended with the consolidate of the chunk " + str(
                chunk) + " El Salvador tariff tables. Execution time = " + str(
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

            util.tariff_to_xlsx(date + "elsalvador_tariff_db", DS_IDB_Result, local)

            # save the new processed link in data
            for file in arq:
                data.append([date, file])

            util.dump_files_s3(data, status_file, local)

            msg = 'Ended with the consolidation of El Salvador tariff tables'
            util.log(local, msg)

            return True

        chunk = chunk + 1
