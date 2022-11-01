import pandas as pd
import datetime
#import  glob
import os
import time
import sys
import numpy as np

sys.path.append('./scrapers')

from  ..utils_test import util

##### Funtion definition

#### Energy ranges

def dicTarEn(df):
    lstcomponent=df.drop_duplicates().sort_values().reset_index(drop= True)
    lstcomponent.index = lstcomponent
    
    ds=lstcomponent.astype('str').str.extractall('(\d+)').unstack().fillna(999999).astype('int')
    ds.columns = ["min", "max"]
    return ds.to_dict()

#### Power ranges
def dicTarPt(df):

    # Power ranges
    lstcomponent=df.drop_duplicates().sort_values().reset_index(drop= True)
    lstcomponent.index = lstcomponent
    espRange = ((lstcomponent.str.upper().str.contains('KW')) & ~(lstcomponent.str.upper().str.contains('KWH')))|(lstcomponent.str.upper().str.contains('> 2800'))


    ## Read the limints in the tariff code
    cond=lstcomponent[espRange]
    df_txtlim=lstcomponent[cond].str[9:]

    ds=df_txtlim.astype('str').str.extractall('(\d+)').astype('int').unstack()
    ds.columns=ds.columns.droplevel(0)
    ds=ds.reset_index()
    ds['q']=ds.loc[:, ds.columns != 'Tariff_code'].count(axis=1)
    ds['min0'] = np.where((ds.Tariff_code.str.contains("> ")) &(ds['q'] == 1),ds[0]+1,
                       np.where((ds.Tariff_code.str.contains(">= ")) &(ds['q'] == 1),ds[0],
                       np.where((ds['q'] == 2),ds[0]+1,np.nan)))
    if 1 in ds.columns:
        ds['max0'] = np.where(ds['Tariff_code'].str.contains("> ")&(ds['q'] == 1),999999,
                           np.where(ds['Tariff_code'].str.contains(">=")&(ds['q'] == 1), 999999,
                           np.where(ds['Tariff_code'].str.contains("< ")&(ds['q'] == 1), ds[0]-1,
                           np.where(ds['Tariff_code'].str.contains("<=")&(ds['q'] == 1), ds[0],         
                           np.where((ds['q'] == 2), ds[1],np.nan)))))
    else:
        ds['max0'] = np.where(ds['Tariff_code'].str.contains("> ")&(ds['q'] == 1),999999,
                           np.where(ds['Tariff_code'].str.contains(">=")&(ds['q'] == 1), 999999,
                           np.where(ds['Tariff_code'].str.contains("< ")&(ds['q'] == 1), ds[0]-1,
                           np.where(ds['Tariff_code'].str.contains("<=")&(ds['q'] == 1), ds[0], np.nan))))


    # add tariff without ranges in its manes

    ds=ds[['Tariff_code','min0', 'max0' ]].append(lstcomponent[~espRange].to_frame().reset_index(drop = True)) 

    ## Fill na base on tariff
    # T1 = 0 to 10 kW
    # T2 = 10 to 50 kW
    # T3 >= 50 kW
    ds['min'] =np.where((ds['Tariff_code'].str.contains("Tarifa 1"))&(ds.min0.isna()),0,
                       np.where((ds['Tariff_code'].str.contains("Tarifa 2"))&(ds.min0.isna()),10,
                       np.where((ds['Tariff_code'].str.contains("Tarifa 3"))&(ds.min0.isna()),50,ds.min0))).astype('int')
    ds['max'] =np.where((ds['Tariff_code'].str.contains("Tarifa 1"))&(ds.max0.isna()),9,
                       np.where((ds['Tariff_code'].str.contains("Tarifa 2"))&(ds.max0.isna()),49,
                       np.where((ds['Tariff_code'].str.contains("Tarifa 3"))&(ds.max0.isna()),999999,ds.max0))).astype('int')
    return ds[['Tariff_code', 'min', 'max']].set_index('Tariff_code').to_dict()

def ini_consolidate(status_file):
    
    # read already processed and saved links so as not to repeat
    data = util.read_files_s3(status_file, 'local_argentina')
    if not data:
        data = []

    # get the already prcessed links in second position
    past_links = []
    for line in data:
        try:
            past_links.append(line[1])
        except IndexError as e:
            util.log(
                'badly formed link on saved links = {}'.format(
                line)
            )
    return data, past_links


def consolidate_tariff(time_to_search, status_file):
    
    msg = '####### Cosolidate Tariff #######'
    util.log('local_argentina', msg)
    print(msg)

    data, past_links = ini_consolidate(status_file)

    beginning_time = time.time()

    
    ### Importing and consolidation data
    #### get the columns dic of the result table
    col_dict=util.tariff_columns_dict()
    AllCol = list(col_dict.keys())
    DS_IDB_Result = pd.DataFrame()

        #### Create a DF from all csv files in the paste
    # Create a dataframe from multiple files
    arq_all= util.list_files('csv*', 'local_argentina')
    arq_tobeprocess = []

    for file in arq_all:
        if file in past_links:
            msg = file + ' table already processed'
            #arq.remove(file)
            util.log('local_argentina', msg)
            print(msg)
        else:
            msg = file + ' table to be processed'
            util.log('local_argentina', msg)
            arq_tobeprocess.append(file)
            print(msg)

    if len(arq_tobeprocess) == 0 :
        msg = 'Consolidation of argentina tariff tables does not add files '
        util.log('local_argentina',msg)
        print(msg)
        return True
        

    arq_chunk=list(util.split_list(arq_tobeprocess,36))

    chunk = 0
    while time.time() - beginning_time < time_to_search:

        #### Tariff table download
        arq=arq_chunk[chunk]
        
        # Create a dataframe from multiple files

        try:
            DS = pd.DataFrame()
            for f in arq:
                nombre = os.path.basename(f)
                df = pd.read_csv(f, sep = ',',decimal=".",encoding='cp1252', parse_dates=['Date_start','Date_end' ], dayfirst=False)
                df['Document_name'] = nombre
                DS = DS.append(df,ignore_index=True)
            
            DS=DS.rename(columns = {'$local' : 'CURRENCY', 'Utility': 'UTILITY', 'Country':'COUNTRY'}) 

            #### Creating columns IDB table format

            df=DS['Component']
            DS['Volumetric_trench_start'] =df.map(dicTarEn(df)['min']).fillna(0).astype('int')
            DS['Volumetric_trench_end'] =df.map(dicTarEn(df)['max']).fillna(999999).astype('int')

            df = DS['Tariff_code']
            DS['Power_trench_start'] =df.map(dicTarPt(df)['min'])
            DS['Power_trench_end'] =df.map(dicTarPt(df)['max'])

            ### Table formating acording to de IDB table
            #### Working by tariff
            ##### T1

            DS_T1 =DS[(DS.Tariff_code.str.contains('Tarifa 1'))]
            DS_T1=DS_T1.reset_index(drop = True)
            DS_T1['Componente_IDB'] =np.where(DS_T1.Component.str.contains('Cargo Variable'), 'Volumetric_total','Charges_component_fixed')
            DS_T1['Billing_frequency']=np.where(DS_T1.Date_start.isin(DS_T1[DS_T1.Unit == '$/bim'].Date_start.unique()), 'bi-monthly','monthly')

            ###### pivot table by IDB component (from file concept table to columns concept)

            DS_T1_IDB = DS_T1.pivot_table(
                values='Charge',
                index=[ 'COUNTRY','UTILITY', 'Document_link', 'Document_name', 'Date_start','Date_end', 'CURRENCY', 'Tariff_code','Billing_frequency','Volumetric_trench_start',\
                    'Volumetric_trench_end', 'Power_trench_start', 'Power_trench_end'],
                columns='Componente_IDB'
                )
            DS_T1_IDB=DS_T1_IDB.reset_index()

            ###### Aditional info columns

            DS_T1_IDB['Voltage_group'] = 'Low Voltage'
            DS_T1_IDB['Structure_subtype']= np.where((DS_T1_IDB['Volumetric_trench_start'] == 0) &(DS_T1_IDB['Volumetric_trench_end'] == 999999) , 'Flat rate','Block Increasing rate')
            DS_T1_IDB['Special_category'] = np.where(DS_T1_IDB.Tariff_code.str.contains('AP'), 'Street Lighting', '')
            DS_T1_IDB['Sector'] =np.where(DS_T1_IDB.Tariff_code.str.contains('AP'), 'Street Lighting',
                                        np.where(DS_T1_IDB.Tariff_code.str.contains('R\\d', case=False, regex=True), 'Residential',
                                        np.where(DS_T1_IDB.Tariff_code.str.contains('C\\d', case=False, regex=True), 'Commercial',
                                        np.where(DS_T1_IDB.Tariff_code.str.contains('G\\d', case=False, regex=True), 'General', ''))))

            #### Working by tariff
            ##### T2

            DS_T2 =DS[(DS.Tariff_code.str.contains('Tarifa 2'))]
            DS_T2=DS_T2.reset_index(drop = True)
            DS_T2['Billing_frequency']='monthly'
            DS_T2['Componente_IDB'] =np.where(DS_T2.Component.str.contains('Cargo Variable'), 'Volumetric_total',
                                            np.where(DS_T2.Component.str.contains('Cargo Fijo'), 'Charges_component_fixed',
                                            np.where(DS_T2.Component.str.contains('Cargo por Potencia Contratada'), 'Contracted_Power_Rate',
                                            np.where(DS_T2.Component.str.contains('Cargo por Potencia Adquirida'),'Realized_power_rate', np.nan))))

            ###### pivot table by IDB component (from file concept table to columns concept)

            DS_T2_IDB = DS_T2.pivot_table(
                values='Charge',
                index=['COUNTRY','UTILITY', 'Document_link', 'Document_name', 'Date_start','Date_end', 'CURRENCY', 'Tariff_code','Billing_frequency','Volumetric_trench_start', \
                    'Volumetric_trench_end',  'Power_trench_start', 'Power_trench_end'],
                columns='Componente_IDB'
                )
            DS_T2_IDB=DS_T2_IDB.reset_index()

            ###### Aditional info columns

            DS_T2_IDB['Structure_subtype']= 'Flat rate'
            DS_T2_IDB['Voltage_group'] = 'Medium Voltage'

            #### Working by tariff
            ##### T3

            DS_T3 =DS[(DS.Tariff_code.str.contains('Tarifa 3'))]
            DS_T3=DS_T3.reset_index(drop = True)
            DS_T3['Billing_frequency']='monthly'

            DS_T3['Daytime_group']=np.where(DS_T3.Component.str.contains('F. Pico'), 'OffPeak',
                                            np.where(DS_T3.Component.str.contains('Resto'), 'Intermediate',
                                            np.where(DS_T3.Component.str.contains('Valle'), 'OffPeak',
                                            np.where(DS_T3.Component.str.contains('Pico'), 'Peak','OffPeak'))))

            ##### copy extra file to have value in all daytime options for each component.

            DS_Add_1=DS_T3[DS_T3.Component.isin(['Cargo Fijo', 'Cargo por Potencia Contratada', 'Cargo por Potencia Adquirida'])].reset_index(drop = True).copy(deep=True)
            DS_Add_2 =DS_Add_1.copy(deep=True)
            DS_Add_1['Daytime_group']='Peak'
            DS_Add_2['Daytime_group']='Intermediate'

            DS_CPFP_1=DS_T3[DS_T3.Component == 'Cargo Pot. F. Pico'].reset_index(drop = True).copy(deep=True)

            DS_CPFP_1['Daytime_group']='Intermediate'

            DS_T3=pd.concat([DS_T3, DS_Add_1,DS_Add_2,DS_CPFP_1])

            DS_T3['Componente_IDB'] =np.where(DS_T3.Component.str.contains('Cargo Variable'), 'Volumetric_total',
                                            np.where(DS_T3.Component.str.contains('Cargo Fijo'), 'Charges_component_fixed',
                                            np.where(DS_T3.Component.str.contains('Cargo por Potencia Contratada'), 'Contracted_Power_Rate',
                                            np.where(DS_T3.Component.str.contains('Cargo por Potencia Adquirida'),'Realized_power_rate',
                                            np.where(DS_T3.Component.str.contains('Cargo Pot. Pico'), 'Contracted_Power_Rate',
                                            np.where(DS_T3.Component.str.contains('Cargo Pot. F. Pico'), 'Contracted_Power_Rate',np.nan))))))

            ###### pivot table by IDB component (from file concept table to columns concept)

            pvtcol = ['COUNTRY','UTILITY', 'Document_link', 'Document_name', 'Date_start','Date_end', 'CURRENCY', 'Tariff_code','Billing_frequency','Volumetric_trench_start'\
                    , 'Volumetric_trench_end', 'Power_trench_start', 'Power_trench_end', 'Daytime_group']
            DS_T3_IDB = DS_T3.pivot_table(
                values='Charge',
                index=pvtcol,
                columns='Componente_IDB'
                )
            DS_T3_IDB=DS_T3_IDB.reset_index()

            ###### Aditional info columns

            DS_T3_IDB['Voltage_group']=np.where(DS_T3_IDB.Tariff_code.str.contains('AT'), 'High Voltage',
                                                        np.where(DS_T3_IDB.Tariff_code.str.contains('MT'), 'Medium Voltage', 'Low Voltage'))
            DS_T3_IDB['Structure_subtype']= 'Flat rate'
            DS_T3_IDB['Differentiation_type'] = 'Time-differentiated rate'

            #### Consolidation of the Dataset

            DS_IDB = pd.concat([DS_T1_IDB,DS_T2_IDB,DS_T3_IDB]).reset_index(drop = True)

            ##### Aditional info columns

            DS_IDB['Taxes_link'] = 'https://ciatorg.sharepoint.com/:x:/r/sites/cds/_layouts/15/guestaccess.aspx?docid=08814180f3de44115bee96760999fb11f&authkey=Ac9cc1KJHzZyZOTz1SCEE-w&e=e2fb160be70847908c740d7f25d702c7'
            DS_IDB['Taxes_component_percentual'] = 0.21

            dicVoltage = {'start': 
                            {'Low Voltage' :0,
                            'Medium Voltage':1001,
                            'High Voltage': 66000},
                        'end':
                            {'Low Voltage' :1000,
                            'Medium Voltage':65999,
                            'High Voltage': 150000}}

            DS_IDB['Voltage_trench_start'] =DS_IDB['Voltage_group'].map(dicVoltage['start'])
            DS_IDB['Voltage_trench_end'] =DS_IDB['Voltage_group'].map(dicVoltage['end'])

            dicDayTime = {'start': 
                            {'Peak' :'18:15',
                            'Intermediate':'5:15',
                            'OffPeak': '23:15'},
                        'end':
                            {'Peak' :'23:00',
                            'Intermediate':'18:00',
                            'OffPeak': '5:00'}}


            DS_IDB['Daytime_group_time_differentiation_start'] =DS_IDB['Daytime_group'].map(dicDayTime['start'])
            DS_IDB['Daytime_group_time_differentiation_end'] =DS_IDB['Daytime_group'].map(dicDayTime['end'])

            ##### Adding columns with nan to complete the dataset 

            lst = [x.upper() for x in AllCol]
            DS_IDB.rename(columns={ 'Tariff_code':'Tariff_Name_in_document'
                                }, inplace = True)
            columnadd=list(set(AllCol).difference(DS_IDB.columns)) 

            DS_IDB[columnadd]=np.nan
            
            DS_IDB_Result=pd.concat([DS_IDB_Result,DS_IDB])

            ##### Export to Excel Partial
            now = datetime.datetime.now()

            date=now.strftime('%Y%m%d_%H%M%S')

            util.tariff_to_xlsx( date +"chunk "+ str(chunk) +"_argentina_tariff_db", DS_IDB, 'local_argentina')

            # save the new processed link in data
            for file in arq:
                data.append([date, file])

            util.dump_files_s3(data, status_file, 'local_argentina')


            msg = "Ended with the consolidate of the chunk " + str(chunk) + " Argentine tariff tables. Execution time = " + str(round(time.time() - beginning_time)) + "sec"  
            util.log( 'local_argentina', msg)
            print(msg)
            
        except Exception as e: 
            msg = 'Consolidating erro = {} '.format(e)
            util.log('local_argentina',msg)
            print(msg)
            return False
        
        if chunk == len(arq_chunk)-1:
            
            ##### Export to Excel Partial
            now = datetime.datetime.now()

            date=now.strftime('%Y%m%d_%H%M%S_')

            util.tariff_to_xlsx( date +"argentina_tariff_db", DS_IDB_Result, 'local_argentina')

            # save the new processed link in data
            for file in arq:
                data.append([date, file])

            util.dump_files_s3(data, status_file, 'local_argentina')

            msg = 'Ended with the consolidation of argentina tariff tables'
            util.log('local_argentina',msg)
            print(msg)

            return True

        chunk=chunk+1    



        

            
        
