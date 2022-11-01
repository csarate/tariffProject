import time
import datetime
import os
import re

import requests_html

import pandas as pd

from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup

from . import aux_mexico 
from ..utils_test import util


dic_tariffs_old_new = {
    'CMAS' : ['Tarifa_5', 'Tarifa_5A', 'Tarifa_6'],
    'CMAA' : ['Tarifa_9', 'Tarifa_9M', 'Tarifa_9CU', 'Tarifa_9N'],
    'CMAT' : ['Tarifa_7'],
    'CMABT': ['Tarifa_2', 'Tarifa_3'],
    'CMAMT' : ['Tarifa_O-M','Tarifa_H-M', 'Tarifa_H-MC'],
    'CMAAT' : ['Tarifa_H-S', 'Tarifa_H-SL', 'Tarifa_H-T', 'Tarifa_H-TL'],
    'CMASR' : ['Tarifa_HM-R', 'Tarifa_HM-RF', 'Tarifa_HM-RM', 
        'Tarifa_HS-R', 'Tarifa_HS-RF', 'Tarifa_HS-RM', 
        'Tarifa_HT-R', 'Tarifa_HT-RF', 'Tarifa_HT-RM'],
    'CMASI' : ['Tarifa_I-15', 'Tarifa_I-30'],
    }
dic_tariff_domesticas = {
    'Domestica Bajo Consumo' : ['Tarifa_1', 'Tarifa_1A', 'Tarifa_1B',
        'Tarifa_1C', 'Tarifa_1D', 'Tarifa_1E', 'Tarifa_1F' ],
    'Domestica Alto Consumo' : ['Domestica_Alto_Consumo']
    }
def list_files_negocio(tariff_name, year):
    l_files = []
    for old_t in dic_tariffs_old_new[tariff_name]:
        file_name = '{}_old_tariff_{}_date_{}.csv'.format(
            tariff_name,
            old_t,
            year)
        l_files.append(file_name)
    return l_files

def list_files_domesticas(tariff_name, year):
    l_files = []
    for t in dic_tariff_domesticas[tariff_name]:
        file_name = '{}_date_{}.csv'.format(
            t,
            year)
        l_files.append(file_name)
    return l_files

def savedf_tariff_table_h_n(
        output_path, data, soup, tariff_service, 
        tariff_name, year, link):
    print('\n\n\nSaving Negocio \n\n\n', tariff_name)
    soup_tables = soup.find('table').find('table').find('table')
    #aux_mexico.process_annual_negocio(soup_tables, tariff_name, year, link)
    try:
        l_fn_df = aux_mexico.table_to_df(
            soup_tables, 
            tariff_service, 
            tariff_name,
            year,
            link)
        if tariff_name == 'CMAMT':
            fns = set([ x[0] for x in l_fn_df ])
            new_fn_df = []
            for fn in fns:
                df = pd.concat([x[1] for x in l_fn_df if x[0] == fn])
                df.reset_index(drop=True, inplace=True)
                new_fn_df.append((fn, df))
            l_fn_df = new_fn_df
        for file_name, df in l_fn_df:
            df.to_csv(
                os.path.join(
                    util.setup_dic[local], 
                    file_name
                )
            )
            util.log(
                local,
                'saved file {}'.format(file_name))
            data.append([
                link, 
                file_name,
                datetime.datetime.now().strftime(
                    '%d/%m/%Y %H:%M:%S'),
                'MEXICO',
                'CFE',
                '',
                '',
                ''])
            util.dump_files_s3(data, output_path, local)
    except:
        print('Not able to save tariff_service: {},\
                tariff_name: {}, year: {}'.format(
                tariff_service,
                tariff_name,
                year))
        util.log('Not able to save tariff_service: {},\
                tariff_name: {}, year: {}'.format(
                tariff_service,
                tariff_name,
                year))


def savedf_tariff_table_h_d(output_path, data, soup, 
        tariff_service, tariff_name,year, link):

    # reach the third nested table
    soup_tables = soup.find('table').find('table').find('table')
    if tariff_name == 'Domestica Bajo Consumo':
        print('\n\n\nSaving Domestica Bajo Consumo \n\n\n')
        try:
            l_fn_df = aux_mexico.table_to_df(
                soup_tables, 
                tariff_service, 
                tariff_name,
                year,
                link)
            fns = set([ x[0] for x in l_fn_df ])
            new_fn_df = []
            for fn in fns:
                df = pd.concat([x[1] for x in l_fn_df if x[0] == fn])
                df.reset_index(drop=True, inplace=True)
                new_fn_df.append((fn, df))
            l_fn_df = new_fn_df
            for file_name, df in l_fn_df:
                df.to_csv(
                    os.path.join(
                        util.setup_dic[local], 
                        file_name
                    )
                )
                util.log(
                    local,
                    'saved file {}'.format(file_name))
                data.append([
                    link, 
                    file_name,
                    datetime.datetime.now().strftime(
                        '%d/%m/%Y %H:%M:%S'),
                    'MEXICO',
                    'CFE',
                    '',
                    '',
                    ''])
                util.dump_files_s3(data, output_path, local)
        except:
            print('Not able to save tariff_service: {},\
                    tariff_name: {}, year: {}'.format(
                    tariff_service,
                    tariff_name,
                    year))
            util.log('Not able to save tariff_service: {},\
                    tariff_name: {}, year: {}'.format(
                    tariff_service,
                    tariff_name,
                    year))

    elif tariff_name == 'Domestica Alto Consumo':
        print('\n\n\nSaving Domestica Alto Consumo \n\n\n')
        # high power limits to tariffs
        hp_table = soup_tables.findAll('table')[1].decode()
        df_high_power = pd.read_html(hp_table)[0]
        print('df_high_power'.format(df_high_power))
        print('saving files')
        file_name_hptable = 'hp_table_'+year+'.csv'
        df_high_power.to_csv(
            os.path.join(
                util.setup_dic[local], 
                file_name_hptable
            )
        )
        try:
            l_fn_df = aux_mexico.table_to_df(
                soup_tables, 
                tariff_service, 
                tariff_name,
                year,
                link)
            fns = set([ x[0] for x in l_fn_df ])
            new_fn_df = []
            for fn in fns:
                df = pd.concat([x[1] for x in l_fn_df if x[0] == fn])
                df.reset_index(drop=True, inplace=True)
                new_fn_df.append((fn, df))
            l_fn_df = new_fn_df
            for file_name, df in l_fn_df:
                df.to_csv(
                    os.path.join(
                        util.setup_dic[local], 
                        file_name
                    )
                )
                util.log(
                    local,
                    'saved file {}'.format(file_name))
                data.append([
                    link, 
                    file_name,
                    datetime.datetime.now().strftime(
                        '%d/%m/%Y %H:%M:%S'),
                    'MEXICO',
                    'CFE',
                    '',
                    '',
                    ''])
                util.dump_files_s3(data, output_path, local)
        except:
            print('Not able to save tariff_service: {},\
                    tariff_name: {}, year: {}'.format(
                    tariff_service,
                    tariff_name,
                    year))
            util.log('Not able to save tariff_service: {},\
                    tariff_name: {}, year: {}'.format(
                    tariff_service,
                    tariff_name,
                    year))
        
def scraper_cfe_historical_tariffs(time_to_search , output_path, local):

    util.create_s3dirs()
    util.log(local,'Started the scraping')
    print('beggining with the scraping of mexico')
    beginning_time = time.time()

    # mount a session with 5 retries
    session = requests_html.HTMLSession()
    retries = Retry(
        total=5,
        backoff_factor=0.1,
        status_forcelist=[ 500, 502, 503, 504 ],
        raise_on_status=True
    )
    session.mount('http://', HTTPAdapter(max_retries=retries))

    # read already processed and saved links so as not to repeat
    data = util.read_files_s3(output_path,local)
    if not data:
        data = []

    # get the already prcessed links in second position
    past_links = []
    for line in data:
        try:
            past_links.append(line[1])
        except IndexError:
            util.log(local,
                'badly formed link on saved links = {}'.format(
                line)
            )

    util.log(local,'Started the scraping of historical\
        hogar tariffs')
    print('\nbeggining with the scraping of historical hogar\
        tariffs')

    tariff_service="Hogar_anual"
    link_domesticas_h = 'https://app.cfe.mx/Aplicaciones/CCFE/Tarifas/Tarifas/tarifas_casa.asp'
    r = util.connect_to_link(
        session, 
        link_domesticas_h, 
        local) 
    if not r:
        return False
    soup = BeautifulSoup(r.text, 'lxml')

    # get the links to domestic tariffs as lists
    base_link_h = 'https://app.cfe.mx/Aplicaciones/CCFE/Tarifas/Tarifas/'
    l_a = aux_mexico.href_tariffs_h(soup)
    for a_tag in l_a:
        suffix = a_tag['href']
        link_tariff = base_link_h + a_tag['href']
        if 'domesticas' in suffix:
            tariff_name = 'Domestica Bajo Consumo'
        elif 'DAC' in suffix:
            tariff_name = 'Domestica Alto Consumo'
        print('\n\ntariff_name: {}'.format(
            tariff_name))

        r = util.connect_to_link(
            session, 
            link_tariff, 
            local) 
        if not r:
            return False
        soup = BeautifulSoup(r.text, 'lxml')

        # loop over list of years
        year_selector = aux_mexico.Selector(soup, 'year_annual')
        for year in year_selector.list_property('value'):

            # process year between 2013 and 2017
            if (int(year) > 2017) or (int(year) <= 2012):
                continue
            print('year : {}'.format(year))

            l_files = list_files_domesticas(tariff_name, year)
            if all([ x in past_links for x in l_files]):
                continue
            if year != year_selector.selected:
                if tariff_name == 'Domestica Alto Consumo':
                    data_to_post = {
                        'Tarifa' : 'DACAnual2003',
                        'Anio' : year
                            }
                elif tariff_name == 'Domestica Bajo Consumo':
                    data_to_post = {
                        'Tarifa' : 'domesticas2003',
                        'Anio' : year
                            }
                r = util.post_to_link(
                    session, 
                    link_domesticas_h,
                    data_to_post,
                    local)
            soup = BeautifulSoup(r.text, 'lxml')
            savedf_tariff_table_h_d(
                output_path,
                data,
                soup,
                tariff_service,
                tariff_name,
                year,
                link_domesticas_h)
    
    util.log(local,'Started the scraping negocio,\
        industria, agricola historical tariffs')
    print('beggining with the scraping of negocio, industria,\
        agricola historical tariffs')

    tariff_service="Negocio_anual"
    link_negocio_a = 'https://app.cfe.mx/Aplicaciones/CCFE/Tarifas/Tarifas/tarifas_negocio.asp'
    r = util.connect_to_link(session, link_negocio_a, local) 
    if not r:
        return False
    soup = BeautifulSoup(r.text, 'lxml')

    base_link_h = 'https://app.cfe.mx/Aplicaciones/CCFE/Tarifas/Tarifas/'
    l_a = aux_mexico.href_tariffs_h(soup)
    for a_tag in l_a:
        suffix = a_tag['href']
        link_tariff = base_link_h + a_tag['href']

        prog = re.compile(r'Tarifa=[A-Z]*&amp')
        tariff_name = prog.search(a_tag.decode())
        if tariff_name:
            tariff_name = tariff_name.group()[7:-4]
        else:
            prog = re.compile(r'Tarifa=[A-Z]*"')
            tariff_name = prog.search(a_tag.decode())
            if tariff_name:
                tariff_name = tariff_name.group()[7:-1]

        # Skip Tariffs with only fixed charges
        if tariff_name in ['CMAMF', 'CMATF']:
            continue
        print('\n\ntariff_name: {}'.format(tariff_name))

        r = util.connect_to_link(
            session, 
            link_tariff, 
            local) 
        if not r:
            print('False')
            return False
        soup = BeautifulSoup(r.text, 'lxml')

        # loop over list of years
        year_selector = aux_mexico.Selector(soup, 'year_annual')
        for year in year_selector.list_property('value'):

            # process year between 2013 and 2017
            if (int(year) > 2017) or (int(year) <= 2012):
                continue
            print('year : {}'.format(year))

            # check past links
            l_files = list_files_negocio(tariff_name, year)
            if all([ x in past_links for x in l_files]):
                continue

            if year != year_selector.selected:
                if tariff_name == 'CMAMT':
                    data_to_post = {
                        'Tarifa' : tariff_name,
                        'Anio' : year,
                        'imprime' : ''
                            }
                else:
                    data_to_post = {
                        'Tarifa' : tariff_name,
                        'Anio' : year
                            }
                #print('data_to_post form = {}'.format(data_to_post))
                r = util.post_to_link(
                    session, 
                    link_negocio_a,
                    data_to_post,
                    local)
            soup = BeautifulSoup(r.text, 'lxml')
            savedf_tariff_table_h_n(
                output_path,
                data,
                soup,
                tariff_service,
                tariff_name,
                year,
                link_negocio_a
                )
    return True
