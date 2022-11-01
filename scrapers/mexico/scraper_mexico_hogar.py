import time
import datetime
import re
import os
import sys

import requests_html

import pandas as pd

from bs4 import BeautifulSoup

from ..utils_test import util


def domesticas_bc(x):
    if x:
        return 'Tarifa' in x
    else:
        return False

def href_tariffs(soup):
    l_DBC = []
    l_DAC = []
    for p in soup.findAll('p'):
        for a in p.findAll('a', title=domesticas_bc):
            l_DBC.append(a)
        if p.find(string=re.compile('alto consumo')):
            l_DAC.append(p.find('a'))
    return l_DBC, l_DAC

def last_focus():
    return ''

def view_state(soup):
    return soup.select('#__VIEWSTATE')[0]['value']

def event_argument():
    return ''

def view_state_generator(soup):
    return soup.select("#__VIEWSTATEGENERATOR")[0]['value']

def event_validation(soup):
    return soup.select("#__EVENTVALIDATION")[0]['value']

def q():
    return ''

def fill_form(soup):
    form_post = {}
    form_post['__LASTFOCUS'] = last_focus()
    form_post['__VIEWSTATE'] = view_state(soup)
    form_post['__EVENTARGUMENT'] = event_argument()
    form_post['__VIEWSTATEGENERATOR'] = view_state_generator(soup)
    form_post['__EVENTVALIDATION'] = event_validation(soup)
    form_post['q'] = q()
    return form_post

def placeholdermesverano1(x):
    if ('PlaceHolder' in x) and ('MesVerano1' in x):
        return True
    else:
        return False

def placeholdermesverano2(x):
    if ('PlaceHolder' in x) and ('MesVerano2' in x):
        return True
    else:
        return False

def placeholdermonth(x):
    if ('PlaceHolder' in x) and ('ddMes' in x):
        return True
    else:
        return False

def placeholderyear(x):
    if ('PlaceHolder' in x) and ('ddAnio' in x):
        return True
    else:
        return False

def month_key_val(soup):
    tag = soup.find('select', id=placeholdermonth)
    if tag:
        month = tag.find('option', selected='selected')['value']
        key_month = tag['name']
        return (month, key_month)
    else:
        return ''

def summer_month_key_val(soup):
    tag = soup.find('select', id=placeholdermesverano1)
    if tag:
        month1 = tag.find('option', selected='selected')['value']
        key_month_verano1 = tag['name']
        m_kv1 = (month1, key_month_verano1)
    tag = soup.find('select', id=placeholdermesverano2)
    if tag:
        month2 = tag.find('option', selected='selected')['value']
        key_month_verano2 = tag['name']
        m_kv2 = (month2, key_month_verano2)
        return [(month1, key_month_verano1), (month2, key_month_verano2)]
    else:
        return ''
    
def year_key_val(soup):
    tag = soup.find('select', id=placeholderyear)
    if tag:
        year = tag.find('option', selected='selected')['value']
        key_year = tag['name']
        return (year, key_year)
    else:
        return ''



def form_to_post(soup, tariff_name, target, year, month='0'):
    '''
    soup : beautifulsoup element
    target : year, month, month_v1, month_v2 
    tariff_name : 'Domestica Alto Consumo, 
        Tarifa 1, Tarifa 1A ... Tarifa 1F'
    year : year being looped
    month : month being looped
    '''
    data_to_post = fill_form(soup)

    # last year consulted
    key_hanio = 'ctl00$ContentPlaceHolder1$hdAnio'

    # last month consulted
    key_hmes = 'ctl00$ContentPlaceHolder1$hdMes'

    # year and month currently selected and 
    # id of month and year input button
    v_year = year_key_val(soup)
    input_year, key_input_year = v_year
    print('year={}, key_input_year={}'.format(input_year, key_input_year))

    # hanio
    data_to_post[key_hanio] = input_year
    
    if target == 'year':
        data_to_post['__EVENTTARGET'] = key_input_year
        data_to_post[key_input_year] = year
    else:
        data_to_post[key_input_year] = input_year


    if tariff_name in ['Domestica Alto Consumo', 'Tarifa 1']:
        v_month = month_key_val(soup)
        input_month, key_input_month = v_month
        print('month={}, key_input_month={}'.format(input_month, key_input_month))
        if target == 'month': 
            data_to_post['__EVENTTARGET'] = key_input_month
            data_to_post[key_input_month] = month
        elif target == 'year': 
            data_to_post[key_input_month] = input_month

        # hmes    
        if tariff_name == 'Tarifa 1':
            data_to_post[key_hmes] = ''
        elif tariff_name == 'Domestica Alto Consumo':
            data_to_post[key_hmes] = input_month
    else:
        (input_month1, key_input_v1), (input_month2, key_input_v2) = summer_month_key_val(soup)
        print('month1={}, kv1={}, month2={}, kv2={}'.format(
            input_month1, key_input_v1, input_month2, key_input_v2)
        )

        if target == 'month_v1': 
            data_to_post['__EVENTTARGET'] = key_input_v1
            data_to_post[key_input_v1] = month
            data_to_post[key_input_v2] = input_month2
        elif target == 'month_v2': 
            data_to_post['__EVENTTARGET'] = key_input_v2
            data_to_post[key_input_v2] = month
            data_to_post[key_input_v1] = input_month1

    return data_to_post

def savedf_power_upperlimit(soup, year):
    df_high_power = pd.read_html(soup.find('table').findAll('table')[1].decode())
    print('df_high_power'.format(df_high_power))
    print('saving files')
    file_name_hptable = 'hp_table_'+year+'.csv'
    df_high_power[0].to_csv(
        os.path.join(
            util.setup_dic['local_mexico'], 
            file_name_hptable
        )
    )

def savedf_tariff_table(soup, tariff_name, **kwargs):
    tariffname = '_'.join(tariff_name.split(' '))
    l_tables = soup.find('table').findAll('table')
    if tariff_name == 'Tarifa 1': 
        df_tariff_components = pd.read_html(l_tables[-1].decode())
        print('saving files')
        
        file_name = tariffname+'_mes'+kwargs['month']+'_'+'anio'+kwargs['year']+'.csv'
        df_tariff_components[0].to_csv(
            os.path.join(
                util.setup_dic['local_mexico'], 
                file_name
            )
        )
    elif tariff_name in [
            'Tarifa 1A', 
            'Tarifa 1B', 
            'Tarifa 1C', 
            'Tarifa 1D', 
            'Tarifa 1E', 
            'Tarifa 1F']: 
        df_tariff_components = pd.read_html(l_tables[-1].decode())
        print('saving files')
        
        file_name = tariffname+'_mes_verano1'+'_'+ kwargs['month_v1']+'_'+'mes_verano2'+\
            '_'+kwargs['month_v2'] +'_'+'anio'+kwargs['year']+'.csv'
        df_tariff_components[0].to_csv(
            os.path.join(
                util.setup_dic['local_mexico'], 
                file_name
            )
        )
    elif tariff_name == 'Domestica Alto Consumo':
        df_california = pd.read_html(l_tables[-2].decode())
        print('df_california'.format(df_california))
        df_otherregions = pd.read_html(l_tables[-1].decode())
        print('df_otherregions'.format(df_otherregions))
        print('saving files')
        file_name_california = tariffname+'_california'+'mes'+kwargs['month']+'_'+'anio'+kwargs['year']+'.csv'
        file_name_otras_reg = tariffname+'_otras_reg'+'mes'+kwargs['month']+'_'+'anio'+kwargs['year']+'.csv'
        df_california[0].to_csv(
            os.path.join(
                util.setup_dic['local_mexico'], 
                file_name_california
            )
        )
        df_otherregions[0].to_csv(
            os.path.join(
                util.setup_dic['local_mexico'], 
                file_name_otras_reg

            )
        )

def scraper_cfe_tariffs(time_to_search , output_path):

    util.create_s3dirs()
    print('beggining with the scraping of mexico')
    beginning_time = time.time()
    current_year = str(datetime.date.today().year)
    current_month = str(datetime.date.today().month)
    print('current year: {}, current_month : {}'.format (
        current_year, current_month
        ))
    link_domesticas = 'https://app.cfe.mx/Aplicaciones/CCFE/Tarifas/TarifasCRECasa/Casa.aspx'
    session = requests_html.HTMLSession()
    r = session.get(link_domesticas)
    print('session status_code : {}'.format(r.status_code))
    soup = BeautifulSoup(r.text, 'lxml')
    # get the links to domestic tariffs as lists
    l_DBC, l_DAC = href_tariffs(soup)
    print('links to domestic tariffs:\n\talta tension:\
        {}\n\tbaja tension: {}\n\n'.format(l_DBC, l_DAC))
    #check the lists
    base_link = 'https://app.cfe.mx/Aplicaciones/CCFE/Tarifas'
    #while time.time() - beginning_time < time_to_search:
    for a_tariff in l_DAC + l_DBC:
        #suffix_dac = l_DAC[0]['href']
        suffix = a_tariff['href']
        # link to dac tariffs
        link_tariff = base_link + suffix.strip('..')
        print('link to domestic tariffs: {}'.format(link_tariff))
        tariff_name = a_tariff['title'] 
        print('tariff name: {}'.format(tariff_name))
        r = session.get(link_tariff)
        soup = BeautifulSoup(r.text, 'lxml')

        # list of years
        options_year = [ x['value'] for x in soup.find(
            'select', id='ContentPlaceHolder1_Fecha_ddAnio'
        ).findAll('option')]
        print('list of years = {}'.format(' '.join(options_year)))

        for year in options_year:
            print('year : {}'.format(year))
        
            if year != current_year:
                # modificar para 1A etc
                data_to_post = form_to_post(soup, tariff_name, 'year', year)
                print('data_to_post form = {}'.format(data_to_post))
                r = session.post(link_tariff, data=data_to_post)
                soup = BeautifulSoup(r.text, 'lxml')
                

            if tariff_name == 'Domestica Alto Consumo':
                savedf_power_upperlimit(soup, year)

            if tariff_name in ['Domestica Alto Consumo', 'Tarifa 1']:
                options_months = [ x['value'] for x in soup.find(
                    'select', id=placeholdermonth
                    ).findAll('option')]
                for month in options_months:
                    print('month', month)
                    if month == '0':
                        continue

                    data_to_post = form_to_post(soup, tariff_name, 'month', year, month)
                    print('data_to_post form = {}'.format(data_to_post))
                    r = session.post(link_tariff, data=data_to_post)
                    soup = BeautifulSoup(r.text, 'lxml')
                    savedf_tariff_table(soup, tariff_name, year=year, month=month)
            else:
                options_start_summer_months = [ x['value'] for x in soup.find(
                    'select', id=placeholdermesverano1
                    ).findAll('option')]
                for month1 in options_start_summer_months:
                    if month1 == '0':
                        continue
                    data_to_post = form_to_post(soup, tariff_name, 'month_v1', year, month1)
                    print('data_to_post form = {}'.format(data_to_post))
                    r = session.post(link_tariff, data=data_to_post)
                    soup = BeautifulSoup(r.text, 'lxml')
                     
                    options_months = [ x['value'] for x in soup.find(
                        'select', id=placeholdermesverano2
                    ).findAll('option')]
                    print('list of months = {}'.format(' '.join(options_months)))
                    for month2 in options_months:
                        print('month', month2)
                        if month2 == '0':
                            continue

                        data_to_post = form_to_post(soup, tariff_name, 'month_v2', year, month2)
                        print('data_to_post form = {}'.format(data_to_post))
                        r = session.post(link_tariff, data=data_to_post)
                        soup = BeautifulSoup(r.text, 'lxml')
                        savedf_tariff_table(soup, tariff_name, year=year, month_v1=month1, month_v2=month2)

