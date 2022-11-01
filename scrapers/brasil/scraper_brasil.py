from ast import Return
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import numpy as np
from ..utils_test import util
import datetime

import time
#from io import  BytesIO, StringIO
#import numpy as np

#time_to_search=120

def ini_Brasil (status_file):
    '''
    time_to_search = float (seconds)
    status_file = check_social_tariff_pageo test the script status_file is the filename with
        the saved links in s3 dir
    '''
    # read already processed and saved links so as not to repeat
    data = util.read_files_s3(status_file, 'local_brasil')
    if not data:
        data = []

    # get the already prcessed links in second position
    past_links = []
    past_anomes = []
    for line in data:
        try:
            past_anomes.append(line[0])
            past_links.append(line[1])
        except IndexError as e:
            msg = "Badly formed link on saved links " + str(line)  
            util.log(msg)
            print(msg)
    return data, past_anomes, past_links

def brasil_Tariff(time_to_search, status_file):

    beginning_time = time.time()
    data, past_anomes, past_links = ini_Brasil(status_file)

    now = datetime.datetime.now()
    
    date = now.strftime('%Y%m')

    URL = 'https://dadosabertos.aneel.gov.br/dataset/5a583f3e-1646-4f67-bf0f-69db4203e89e/resource/fcf2906c-7c32-4b9b-a637-054e7a5234f4/download/tarifas-homologadas-distribuidoras-energia-eletrica.csv'

    try:
        #### Tariff table download
        
        msg = 'Starting reading the Brasil tariff tables'
        util.log('local_brasil', msg)
        print(msg)

        start = time.time()
        tarifas = pd.read_csv(URL,sep = ";",decimal = ",", encoding='utf_16', parse_dates=['DatInicioVigencia', 'DatFimVigencia'], dayfirst=False)
        end = time.time()
        msg = "Reading Brasil tariff table execution time  :"  + str(round(end - start)) +  "sec"
        util.log('local_brasil',msg)
        print(msg)

        tarifas['Document_link'] =URL
        tarifas['Document_name'] =os.path.basename(URL)
        tarifas['COUNTRY'] ='Brasil'
        tarifas['ano_mes'] =tarifas['DatInicioVigencia'].dt.strftime('%Y%m')

        # The tariff objetive is only the final user tariff
        tarifas_obj =tarifas[(tarifas.SigAgenteAcessante == 'Não se aplica') & (tarifas.DscBaseTarifaria == 'Tarifa de Aplicação')]
        tarifas_obj=tarifas_obj.replace('Não se aplica', np.nan)

        ## Lst of month and year available 
        lstanomes=tarifas.ano_mes.drop_duplicates().reset_index(drop = True).sort_values()

        lstanomesnew = []
        for anomes in lstanomes:
            if anomes in past_anomes:
                msg = 'Brasil tariff tables for year month ' + anomes + ' is already downloaded'
                util.log('local_brasil', msg)
                print(msg)
            else:
                lstanomesnew.append(anomes)
                msg = 'Brasil tariff tables for year month ' + anomes + ' to be downloaded'
                util.log('local_brasil', msg)
                print(msg)

        ## Concatenate relevant data of tariff name

        tarifas_obj['Tariff_Name_in_document'] =tarifas_obj[['DscSubGrupo','DscModalidadeTarifaria', 'DscClasse', 'DscSubClasse', 'NomPostoTarifario']]\
                                        .apply(lambda x: '_'.join(x.dropna().astype(str)),axis=1)

        try:
            while time.time() - beginning_time < time_to_search:
                ## Export tariff split files by month and year
                for anomes in lstanomesnew:
                    nombre =  'brasil_tariff_aneel_'+ anomes +'.csv'   
                    tarifas_obj[tarifas_obj.ano_mes == anomes].to_csv(os.path.join(util.setup_dic['local_brasil'], nombre  )\
                                                                    ,encoding='cp1252',index=False)
            
                    data.append([anomes, nombre])

                util.dump_files_s3(data, status_file, 'local_brasil')

                end = time.time()

                msg = "Brasil tariff table execution time  :"  + str(round(end - start)) +  "sec"
                util.log('local_brasil',msg)
                print(msg)
                return True

        except Exception as e: 
                    msg = 'Exporting data erro = {} '.format(e)
                    util.log('local_brasil',msg)
                    print(msg)
                
    except Exception as e: 
            msg = 'Downloading erro = {} trying to connect to {}'.format(e, URL)
            util.log('local_brasil',msg)
            print(msg)


def brasil_SAMP(time_to_search, status_file):
    beginning_time = time.time()

    data, past_anomes, past_links = ini_Brasil( status_file)
    '''
    time_to_search = float (seconds)
    status_file = check_social_tariff_pageo test the script status_file is the filename with
        the saved links in s3 dir
    '''
    # urls
    samp_link = 'https://dadosabertos.aneel.gov.br/dataset/3e153db4-a503-4093-88be-75d31b002dcf/resource/'

    # SAMP year ID
    dicid={ 2010 : '475a41c8-905d-46c6-8fc9-fe9a8a9c6868',
     2011 : '9c25ad95-a433-482c-9847-4f7c5c2b3df0',
     2012 : 'b2f4411d-90db-4455-91c7-ebe399ba93c5',
     2013 : '53fa913e-32bc-4765-b7c4-2c0544e33eb3',
     2014 : 'b53ee5a1-26f0-47f3-8dea-ae6894d714ac',
     2015 : '1a35925f-8f7d-4074-973e-42dfbaa99636',
     2016 : 'f5421c87-81c5-42e4-bbf0-33af4cc98800',
     2017 : '4a84f3c8-9dc8-4448-bba8-cc2dfbc26ca2',
     2018 : '6ab6596e-170d-42e8-af93-9de024ebe36b',
     2019 : '641003d6-4e87-4095-9416-ae5fbb5c94d3',
     2020 : '29f9fec9-34dd-454b-8f3c-6b4ca5b22f2c',
     2021 : '84906f77-0bb4-4527-b9a0-cb6c2b525661',
     2022 : '7e097631-46ad-4051-8954-9ef8fb594fdc'
}
    #### SAMP Consumer and Market table download
    try:

        for ano in dicid:
            cicle_time =time.time()
            if time.time() - beginning_time > time_to_search:
                return False
            else:     
                id = dicid[ano]
                samp_url = samp_link +id+ '/download/samp-' + str(ano) + '.csv' 
                date = str(ano) +'12'    
                            
                if samp_url in past_links:
                    msg = 'Brasil SAMP ' + str(ano) +' table already downloaded'
                    util.log('local_brasil', msg)
                    print(msg)
                else:
                    msg = 'Started with the download of Brasil SAMP ' +date
                    util.log('local_brasil', msg)
                    print(msg)

                    util.save_URL_files_s3(samp_url, 'local_brasil', 'brasil_SAMP_' +date + '_' )

                    data.append([date, samp_url])
                    
                    # save the new processed link in data
                    util.dump_files_s3(data, status_file, 'local_brasil')

                    msg = 'Ended with the download of Brasil SAMP ' +date

                    util.log( 'local_brasil', msg)
                    print(msg)
    
            print('Execution time = ' + str(round(time.time() - cicle_time)) , 'sec')

        return True

    except Exception as e: 
        msg = 'Downloading erro = {} trying to connect to {}'.format(e, samp_url)
        util.log('local_brasil',msg)
        print(msg)
    




def brasil_PCAT_SPARTA(time_to_search, status_file):
    '''
    time_to_search = float (seconds)
    status_file = check_social_tariff_pageo test the script status_file is the filename with
        the saved links in s3 dir
    '''
    beginning_time = time.time()

    
    now = datetime.datetime.now()
    
    date_now = now.strftime('%Y%m')

    data, past_anomes, past_links = ini_Brasil( status_file)

    # urls
    pcat_sparta__dist = "https://www2.aneel.gov.br/aplicacoes_liferay/tarifa/TipoProcesso.cfm?CategoriaAgente=d"  # List avalable companies
    pcat_sparta_link = "https://www2.aneel.gov.br/aplicacoes_liferay/tarifa/resultado.cfm" # PCAT & SPARTA the tariff structure and cost excel models 
    
    #### List od Utility
    
    try:
        response = requests.post(pcat_sparta__dist)
        response.raise_for_status() # check if site is on
        soup = BeautifulSoup(response.text, 'html5lib')# read html
        list_names = []
        dict_names_codes = {}
        for distribuidora in soup.find_all('option'):# find option with companies names and codes and stores them
            if distribuidora["value"] not in ["-", '0']:# just the legend of the dropdown menu
                dict_names_codes[distribuidora["value"].strip()]=distribuidora.text.strip()
                list_names.append(distribuidora.text.strip())
    
    except Exception as e: 
        msg = 'Downloading erro = {} trying to connect to {}'.format(e, pcat_sparta__dist)
        util.log('local_brasil',msg)
        print(msg)

    #### List and link of files
    lstSparta=[]
    lstPCAT= []
   
    try:
        for agente in dict_names_codes:
            cicle_time = time.time()
            if time.time() - beginning_time > time_to_search:
                return False
            else: 
                DictInfo = {'CategoriaAgente':'d',
                            'Agentes':agente,
                            'TipoProcesso': '0',
                            'Ano': 0,
                            } # form information

                msg = "Downloading SPARTA and PCAT "+ dict_names_codes[agente] # register that new info was gathered
                util.log('local_brasil',msg)
                print(msg)

                response = requests.post(pcat_sparta_link,data= DictInfo)# get SPARTA and PCAT info
                response.raise_for_status() # check if site is on
                soup = BeautifulSoup(response.text, 'lxml')# read html

                table = soup.table
                table_rows = table.find_all('tr')
                
                for tr in table_rows[3:]:    ## reading the table without the header of the table
                    td = tr.find_all('td')
                    row = [agente]
                    j=0
                    eSparta = False 
                    ePCAT = False
                    td_fecha=td[3].text.strip().split("/")
                    date =td_fecha[2]+td_fecha[1]
                    for registro in td:
                        j = j +1
                        valor= registro.text.strip()
                #        print(valor)
                        if valor != '' :
                #            print(row)
                            row.append(valor)
                        else:
                            try:
                                for link in registro.findAll('a', href=True):
                                    rowSparta= row[:]
                                    rowPCAT = row[:]
                                    link_ref = link['href']
                                    #col.append(link_ref)
                                    if ('PCAT' in link_ref):
                                        rowPCAT.extend([link_ref])
                                        lstPCAT.append(rowPCAT)
                                        ePCAT = True
                                        if link_ref in past_links:
                                            msg = 'Brasil PCAT ' + date + ' '+ link_ref +' table already downloaded'
                                            util.log('local_brasil', msg)
                                            print(msg)
                                        else:
                                            util.save_URL_files_s3(link_ref, 'local_brasil', 'brasil_pcat_'+date + '_')
                                            data.append([date, link_ref])
                                            util.dump_files_s3(data, status_file, 'local_brasil') # save the new processed link in data

                                    elif ('SPART' in link_ref):
                                        rowSparta.extend([link_ref])
                                        lstSparta.append(rowSparta)
                                        eSparta = True
                                        if link_ref in past_links:
                                            msg = 'Brasil SPARTA ' + date + ' '+ link_ref +' table already downloaded'
                                            util.log('local_brasil', msg)
                                            print(msg)
                                        else: 
                                            util.save_URL_files_s3(link_ref, 'local_brasil', 'brasil_sparta_'+date + '_')
                                            data.append([date, link_ref])
                                            util.dump_files_s3(data, status_file, 'local_brasil') # save the new processed link in data                                    
                            except:
                                    msg = 'error utility ' + agente 
                                    util.log('local_brasil',msg)
                                    print(msg)
                                
                    if not ePCAT :
                        lstPCAT.append(row)
                        
                    if  not eSparta:
                        lstSparta.append(row)
            print('Execution time = ' + str(round(time.time() - cicle_time)) , 'sec')
    
        columns= ['COD','Agente', 'Categoria do Agente', 'Tipo de Processo',
            'Data de Aniversario', 'Status Resultado', 'link']

        TblSparta=pd.DataFrame(lstSparta, columns =columns)
        TblPCAT=pd.DataFrame(lstPCAT, columns =columns)
        
        # save the new processed link in data
        TblSparta.to_csv(os.path.join(util.setup_dic['local_brasil'], date_now +' TblSparta.csv' ),encoding='cp1252')
        TblPCAT.to_csv(os.path.join(util.setup_dic['local_brasil'], date_now +' TblPCAT.csv' ),encoding='cp1252')
        return True    
    
    except Exception as e: 
        msg = 'Downloading erro = {} trying to connect to {}'.format(e, pcat_sparta_link)
        util.log('local_brasil',msg)
        print(msg)

    
#if __name__ == '__main__':
