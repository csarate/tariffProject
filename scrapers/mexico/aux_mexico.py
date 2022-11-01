import re
import datetime
import calendar
import time
import bs4
import sys
import pandas as pd
import numpy as np
from .. utils_test import util
from .. utils_test.util import concat_cols


dic_season = {
    ('2', '1') : 'out_of_summer',
    ('2', '2') : 'summer',
    ('2', '3') : 'summer',
    ('2', '4') : 'summer',
    ('2', '5') : 'summer',
    ('2', '6') : 'summer',
    ('2', '7') : 'summer',
    ('2', '8') : 'out_of_summer',
    ('2', '8') : 'out_of_summer',
    ('2', '9') : 'out_of_summer',
    ('2', '10') : 'out_of_summer',
    ('2', '11') : 'out_of_summer',
    ('2', '12') : 'out_of_summer',
    ('5', '2') : 'out_of_summer',
    ('5', '3') : 'out_of_summer',
    ('5', '4') : 'out_of_summer',
    ('5', '8') : 'summer',
    ('5', '9') : 'summer',
    ('5', '10') : 'summer'
    }
dic_months = {
        'Ene' : 'Jan',
        'Feb' : 'Feb',
        'Mar' : 'Mar',
        'Abr' : 'Apr',
        'May' : 'May',
        'Jun' : 'Jun',
        'Jul' : 'Jul',
        'Ago' : 'Aug',
        'Sep' : 'Sep',
        'Oct' : 'Oct',
        'Nov' : 'Nov',
        'Dic' : 'Dec'
        }
def minimomensual(x):
    if not x:
        return False
    if x.text:
        return 'Mínimo mensual' in x.text
    else:
        return False

def find_minimo_mensual(soup):
    pattern_nr = re.compile(r'[0-9]+')
    found=False
    tag = soup.find(
        'span', 
        {'class':'tituloContenidoRojo'}, 
        string=minimomensual)
    if not tag:
        return ''
    lines = [ x for x in soup.text.split('\n') if x.strip() ]
    for l in lines:
        if 'Mínimo mensual' in l:
            found = True
            continue
        if found and 'equivalente' in l:
            min_mensual = l.strip()
            if (pattern_nr.search(min_mensual)):
                return(min_mensual,float(
                    pattern_nr.search(min_mensual).group()))
            else:
                return (min_mensual, '')
    return ('', '')

def href_industria(x):
    if 'TarifasCREIndustria' in x:
        return True
    else:
        return False

def href_negocios(x):
    if 'TarifasCRENegocio' in x:
        return True
    else:
        return False

def a_tariffs(soup, service):
    if service == 'domesticas':
        return a_link_domesticas(soup)
    elif service == 'negocios':
        return soup.findAll('a', href=href_negocios)
    elif service == 'industria':
        return soup.findAll('a', href=href_industria)

# find links
def a_link_domesticas(soup):
    l_a = []
    for p in soup.findAll('p'):
        if 'Domésticas' in p.text:
            l_a.extend(p.findAll('a'))
    return l_a

def findCuotas_h(x):
    if x:
        if 'Cuotas' in x:
            return True
        else:
            return False
    else:
        return False

def href_tariffs_h(soup):
    l_links = soup.findAll('a', string=findCuotas_h) 
    return l_links

# fill form
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

def placeholdermunicipio(x):
    if ('PlaceHolder' in x) and ('ddMunicipio' in x):
        return True
    else:
        return False

def placeholderestado(x):
    if ('PlaceHolder' in x) and ('ddEstado' in x):
        return True
    else:
        return False

def placeholderdivision(x):
    if ('PlaceHolder' in x) and ('ddDivision' in x):
        return True
    else:
        return False


class Selector():
    '''
    object to handle selectors

    '''
    def __init__(self, soup, target):
        '''
        target = string (element selected : year, month ...)
        '''
        self.soup = soup
        self.target = target 
        self.selector = self.find()
        self.selected = self.selected()

    def find(self):
        if self.target == 'estado':
            return self.soup.find(
                'select',
                id=placeholderestado)
        elif self.target == 'division':
            return self.soup.find(
                'select',
                id=placeholderdivision)
        elif self.target == 'municipio':
            return self.soup.find(
                'select',
                id=placeholdermunicipio)
        elif self.target == 'year':
            return self.soup.find(
                'select',
                id=placeholderyear)
        elif self.target == 'month':
            return self.soup.find(
                'select',
                id=placeholdermonth)
        elif self.target == 'month_v1':
            return self.soup.find(
                'select',
                id=placeholdermesverano1)
        elif self.target == 'month_v2':
            return self.soup.find(
                'select',
                id=placeholdermesverano2)
        elif self.target == 'year_annual':
            return self.soup.find(
                'select',
                {'name':'Anio'})

    def list_property(self, property): 
        if str(property) == 'text':
            return [ 
                x.text for x in self.selector.findAll('option')]
        else:
            return [ 
                x[str(property)] for x in self.selector.findAll('option')]

    def selected(self):
        if self.selector.find(selected='selected'):
            return self.selector.find(selected='selected')['value']
        else:
            return ''

    def __str__(self):
        return ','.join(
            [ x.text for x in self.selector.findAll('option')])

class form_to_post():
    '''
    form to make the post depending on the tariff type 

    '''
    # last year consulted
    key_hanio = 'ctl00$ContentPlaceHolder1$hdAnio'

    # last month consulted
    key_hmes = 'ctl00$ContentPlaceHolder1$hdMes'

    def __init__(self, soup, tariff_service, tariff_name):
        '''
        soup : beautifulsoup element
        tariff_service: string -> 'Hogar_by_month', 'Hogar_by_season', 
            'Negocio', 'Industria', 'Agricola'
        tariff_name : string -> 'Domestica Alto Consumo, 
            Tarifa 1, Tarifa 1A ... Tarifa 1F'
        '''
        self.soup = soup
        self.tariff_service = tariff_service
        self.tariff_name = tariff_name
        self.form = {}
        self.fill_form()

    def last_focus(self):
        self.form['__LASTFOCUS'] = ''
    
    def view_state(self):
        v = self.soup.select('#__VIEWSTATE')[0]['value']
        self.form['__VIEWSTATE'] = v

    def event_argument(self):
        self.form['__EVENTARGUMENT'] = ''
    
    def view_state_generator(self):
        v = self.soup.select("#__VIEWSTATEGENERATOR")[0]['value']
        self.form['__VIEWSTATEGENERATOR'] = v
    
    def event_validation(self):
        v = self.soup.select("#__EVENTVALIDATION")[0]['value']
        self.form['__EVENTVALIDATION'] = v
    
    def q(self):
        self.form['q'] = ''

    def year_key_val(self):
        tag = self.soup.find('select', id=placeholderyear)
        if tag:
            year = tag.find('option', selected='selected')['value']
            key_year = tag['name']
            return (year, key_year)
        else:
            return ''

    def month_key_val(self):
        tag = self.soup.find('select', id=placeholdermonth)
        if tag:
            month = tag.find('option', selected='selected')['value']
            key_month = tag['name']
            return (month, key_month)
        else:
            return ''

    def summer_month_key_val(self):
        tag = self.soup.find(
            'select', 
            id=placeholdermesverano1)
        if tag:
            month1 = tag.find(
                'option', 
                selected='selected')['value']
            key_month_verano1 = tag['name']
        tag = self.soup.find(
            'select', 
            id=placeholdermesverano2)
        if tag:
            month2 = tag.find(
                'option', 
                selected='selected')['value']
            key_month_verano2 = tag['name']
            return [
                (month1, key_month_verano1), 
                (month2, key_month_verano2)]
        else:
            return ''

    def municipio_key_val(self):
        tag = self.soup.find(
            'select', 
            id=placeholdermunicipio)
        if tag:
            municipio = tag.find(
                'option', 
                selected='selected')['value']
            key_municipio = tag['name']
            return (municipio, key_municipio)
        else:
            return ''

    def division_key_val(self):
        tag = self.soup.find(
            'select', 
            id=placeholderdivision)
        if tag:
            division = tag.find(
                'option', 
                selected='selected')['value']
            key_division = tag['name']
            return (division, key_division)
        else:
            return ''
    
    def estado_key_val(self):
        tag = self.soup.find(
            'select', 
            id=placeholderestado)
        if tag:
            estado = tag.find(
                'option', 
                selected='selected')['value']
            key_estado = tag['name']
            return (estado, key_estado)
        else:
            return ''

    def fill_key_hmes(self):
        if (self.tariff_name == 'Tarifa 1') or (
                self.tariff_service == 'Agricola'):
            self.form[self.key_hmes] = ''
        elif (self.tariff_name == 'Domestica Alto Consumo') or (
                self.tariff_service in ['Negocio', 'Industria']):
            input_month, _ = self.month_key_val()
            self.form[self.key_hmes] = input_month

    def fill_form(self):
        self.last_focus()
        self.view_state()
        self.event_argument()
        self.view_state_generator()
        self.event_validation()
        self.q()
        input_year, _ = self.year_key_val()
        self.form[self.key_hanio] = input_year
        self.fill_key_hmes()

    def fill_target(self, target, **kwargs):
        '''
        target : string -> (item to post) 'year', 'month',
            'month_v1', 'month_v2', 'municipio', 'estado', 
            'distrito'
        '''
        if self.tariff_service in ['Negocio', 'Industria']:
            self.fill_by_region(target, **kwargs)
        elif self.tariff_service in ['Hogar_by_month', 'Agricola']:
            self.fill_by_month(target, **kwargs)
        elif self.tariff_service == 'Hogar_by_season':
            self.fill_by_season(target, **kwargs)

    def fill_by_month(self, target, **kwargs):
        input_year, key_input_year = self.year_key_val()
        input_month, key_input_month = self.month_key_val()
        if target == 'year':
            self.form['__EVENTTARGET'] = key_input_year
            self.form[key_input_year] = kwargs['year']
            self.form[key_input_month] = input_month
        elif target == 'month':
            self.form['__EVENTTARGET'] = key_input_month
            self.form[key_input_year] = input_year
            self.form[key_input_month] = kwargs['month']
            
    def fill_by_season(self, target, **kwargs):        
        input_year, key_input_year = self.year_key_val()
        (input_month1, key_input_v1), (input_month2, key_input_v2) =\
            self.summer_month_key_val()

        if target == 'year':
            self.form['__EVENTTARGET'] = key_input_year
            self.form[key_input_year] = kwargs['year']
            self.form[key_input_v1] = input_month1
            self.form[key_input_v2] = input_month2
        if target == 'month_v1': 
            self.form['__EVENTTARGET'] = key_input_v1
            self.form[key_input_v1] = kwargs['month1']
            self.form[key_input_year] = input_year
            self.form[key_input_v2] = input_month2
        elif target == 'month_v2': 
            self.form['__EVENTTARGET'] = key_input_v2
            self.form[key_input_v2] = kwargs['month2']
            self.form[key_input_year] = input_year
            self.form[key_input_v1] = input_month1

    def fill_by_region(self, target, **kwargs):
        input_year, key_input_year = self.year_key_val()
        input_month, key_input_month = self.month_key_val()
        input_estado, key_input_estado = self.estado_key_val()
        input_division, key_input_division = self.division_key_val()
        input_municipio, key_input_municipio = self.municipio_key_val()
        if target == 'year':
            self.form['__EVENTTARGET'] = key_input_year
            self.form[key_input_year] = kwargs['year']
            self.form[key_input_month] = input_month
            self.form[key_input_estado] = input_estado 
            self.form[key_input_division] = input_division
            self.form[key_input_municipio] = input_municipio
        elif target == 'month':
            self.form['__EVENTTARGET'] = key_input_month
            self.form[key_input_month] = kwargs['month']
            self.form[key_input_year] = input_year
            self.form[key_input_estado] = input_estado 
            self.form[key_input_division] = input_division
            self.form[key_input_municipio] = input_municipio
        elif target == 'municipio':
            self.form['__EVENTTARGET'] = key_input_municipio
            self.form[key_input_municipio] = kwargs['municipio']
            self.form[key_input_year] = input_year
            self.form[key_input_month] = input_month
            self.form[key_input_estado] = input_estado 
            self.form[key_input_division] = input_division
        elif target == 'division':
            self.form['__EVENTTARGET'] = key_input_division
            self.form[key_input_division] = kwargs['division']
            self.form[key_input_year] = input_year
            self.form[key_input_month] = input_month
            self.form[key_input_estado] = input_estado 
            self.form[key_input_municipio] = input_municipio
        elif target == 'estado':
            self.form['__EVENTTARGET'] = key_input_estado
            self.form[key_input_estado] = kwargs['estado']
            self.form[key_input_year] = input_year
            self.form[key_input_month] = input_month
            self.form[key_input_division] = input_division 
            self.form[key_input_municipio] = input_municipio

def voltage_group(r):
    if not 'Tariff_Voltage_line' in r:
        return r
    voltage = r['Tariff_Voltage_line']
    pattern_low = re.compile(r'[Bb][Aa][jJ][AaoO]')
    pattern_medium = re.compile(r'[Mm][eE][dD][Ii][AaoO]')
    pattern_high = re.compile(r'[Aa][Ll][Tt][AaOo]')
    if pattern_low.search(voltage):
        r['Tariff Voltage Group'] = 'Low Voltage'
    elif pattern_medium.search(voltage):
        r['Tariff Voltage Group'] = 'Medium Voltage'
    elif pattern_high.search(voltage):
        r['Tariff Voltage Group'] = 'High Voltage'
    return r

def comp_to_unit(r):
    if not 'Component_unit' in r:
        return r
    comp_unit = r['Component_unit']
    try:
        comp = comp_unit.split('(')[0].strip()
        if not comp:
            comp = 'energía'
        unit = comp_unit.split('(')[1].strip(')').strip()
        r['Component'] = comp
        r['Unit'] = unit
    except:
        r['Component'] = comp_unit
        r['Unit'] = ''
        
    return r

def convert_to_numeric(v):
    '''
    v = string 
    gets the substring that matches a number and
    converts it to float
    '''
    if not v:
        return v
    if not isinstance(v, str):
        v = str(v)
    pattern_dec = re.compile(r'[0-9]+.[0-9]+')
    pattern_int = re.compile(r'[0-9]+')
    if pattern_dec.search(v):
        return float(pattern_dec.search(v).group())
    elif pattern_int.search(v):
        return float(pattern_int.search(v).group())
    else:
        print('fail to convert to number: {}'.format(v))
        raise ValueError

def add_season(df, season):
    '''
    season = string ('summer', 'out_of_summer', 'annual')
    '''
    if season == 'summer':
        df['Seasontime_differentiation_group'] = [
            season for _ in range(df.shape[0])]
    elif season == 'out_of_summer':
        df_list = []
        for s in ['spring', 'winter', 'autumn']:
            df['Seasontime_differentiation_group'] = [
            s for _ in range(df.shape[0])]
            df_list.append(df.copy())
        df = pd.concat(df_list)
    else:
        df['Seasontime_differentiation_group'] = [
            '' for _ in range(df.shape[0])]
    df.reset_index(drop=True, inplace=True)
    return df

def extract_tag(df, pattern_not_tag, l_tags, ix_list, added_lines=0):
    # index nr of tag rows
    ix_of_df_tags = (
        df.loc[
            df.iloc[:,0].apply(
               lambda x : False if (
                   pattern_not_tag.search(str(x))) else True
            )
        ].iloc[:,0].index.values
    )
    l_tags.extend(df.iloc[ix_of_df_tags,0].values)

    # list of the indexes of tag rows, ending with 
    # total length of rows
    ix_list.extend(ix_of_df_tags)
    ix_list.append(df.shape[0])

    # nr of rows between tags
    ix_arr = np.array(ix_list)
    ix_diff = np.diff(ix_arr) - 1

    # creates the complete list of tags, repeating
    # the tag between indexes in ix_list
    tag_col = [ [x]*y for x, y in zip(l_tags, ix_diff+added_lines)]
    tag_col = [ x for y in tag_col for x in y]

    # df with only non tag rows
    df = df.loc[
            df.iloc[:,0].apply(
                lambda x : True if (
                    pattern_not_tag.search(x)) else False
            )
    ].copy()

    df.reset_index(drop=True, inplace=True)
    return df, tag_col, ix_diff

def df_concat(df, series_fijo, ix_diff):
    l_ix = list(np.cumsum(ix_diff))
    l_ix.insert(0,0)
    l_df = []
    for b,e in zip(l_ix[:-1], l_ix[1:]):
        new = pd.concat([series_fijo,df[b:e]])
        l_df.append(new)
    df = pd.concat(l_df)
    return df

def add_columns(
        df, tariff_name='', year_month='', link='', 
        date_start_end='', region=''):
    '''
    date_start_end = tuple with strings formatted %d-%m-%Y
    df = pandas.DataFrame
    year_month = time.datetime
    link = string (url)
    tariff_name = string

    '''
    nlines = df.shape[0]
    df['Country'] = ['Mexico' for _ in range(nlines)]
    df['Utility'] = ['CFE' for _ in range(nlines)]
    df['$local'] = ['MXN' for _ in range(nlines)]
    if year_month:
        date_start = time.strftime('%d-%m-%Y', year_month)
        df['Date_start'] = [ date_start for _ in range(nlines) ]
        ndays = calendar.monthrange(
            year_month.tm_year,
            year_month.tm_mon)[1]
        date_end = time.strptime(
            '-'.join(
                map(str,(
                    ndays, 
                    year_month.tm_mon, 
                    year_month.tm_year)
                )
            ), '%d-%m-%Y'
        )
        date_end = time.strftime('%d-%m-%Y', date_end)
        df['Date_end'] = [ date_end for _ in range(nlines) ]
    if link:
        df['Document_link'] = [ link for _ in range(nlines) ]
    if region:
        df['Region'] = [ region for _ in range(nlines) ]
    if tariff_name:
        df['Tariff_code'] = [ tariff_name for _ in range(nlines) ]
    if date_start_end:
        df['Date_start'] = [date_start_end[0] for _ in range(nlines)]
        df['Date_end'] = [date_start_end[1] for _ in range(nlines)]
    return df

######################################################################

def transform_tables_agricola(
        table, tariff_name, year_month, link):
    df = pd.read_html(table.decode())[0]
    col_names = df.columns.values
    df.dropna(axis=0, subset=col_names[1:], inplace=True)
    df = df.iloc[0,1:].reset_index().rename(
        columns={'index': 'Component', 0: 'Charge'}).copy()
    df = add_columns(df, tariff_name=tariff_name, 
        year_month=year_month, link=link)
    df['Charge'] = df['Charge'].apply(
        convert_to_numeric)
    return df

def transform_tables_negocio(
        table, tariff_name, year_month, region, link):
    df = pd.read_html(table.decode())[0]
    if tariff_name not in ['GDMTH', 'DIST', 'DIT']:
        col_names = [
            'Tariff_code', 
            'Description', 
            'Component',
            'Unit',
            'Charge']
    elif tariff_name in ['GDMTH', 'DIST', 'DIT']:
        col_names = [
            'Tariff_code', 
            'Description', 
            'Daytime_group',
            'Component',
            'Unit',
            'Charge']
    df.columns = col_names
    df.dropna(axis=0, subset=['Charge'], inplace=True)
    df = add_columns(df, tariff_name=tariff_name, 
        year_month=year_month, link=link, region=region)
    df['Charge'] = df['Charge'].apply(
        convert_to_numeric)
    return df

def transform_tables_hogar(
        l_tables, tariff_name, year_month, 
        link, min_mensual, season):
    if tariff_name == 'Domestica Alto Consumo':
       df = transform_dac(
            l_tables, tariff_name, year_month, link, min_mensual)
    elif tariff_name == 'Tarifa 1':
       df = transform_tarifa_1(
            l_tables, tariff_name, year_month, link, min_mensual)
    else:
       df = transform_tarifa_1letter(
            l_tables, tariff_name, year_month, link, min_mensual,
            season)
    df = df.reset_index(drop=True)
    return df


def transform_dac(
        l_tables, tariff_name, year_month, link, min_mensual):
    df_1 = pd.read_html(l_tables[-2].decode())[0]
    df_2 = pd.read_html(l_tables[-1].decode())[0]
    col_names = [
        'Region', 
        'Charges_component_fixed', 
        'Volumetric_total']
    df_1_summer = concat_cols(
        df_1,
        [0,2], [2,3], 
        first_row=0, 
        col_names = col_names)
    df_1_out_of_summer = concat_cols(
        df_1, 
        [0,2], [3,4], 
        first_row=0, 
        col_names = col_names)
    df_2 = concat_cols(
        df_2, 
        [0,2], [2,3], 
        first_row=0, 
        col_names = col_names)
    df_list = []
    df_1_summer = add_season(df_1_summer, 'summer')
    df_list.append(df_1_summer)
    df_1_out_of_summer = add_season(
        df_1_out_of_summer,
        'out_of_summer')
    df_list.append(df_1_out_of_summer)
    df_2 = add_season(df_2, 'annual')
    df_list.append(df_2)
    df_cat = pd.concat(df_list, axis=0)
    df_c1 = concat_cols(
        df_cat,
        [0,1], [1,2], 
        first_row=0, 
        col_names = ['Region', 'Charge'])
    df_c1['Component'] = ['Charges_component_fixed' 
            for _ in range(df_c1.shape[0])]
    df_c2 = concat_cols(
        df_cat,
        [0,1], [2,3], 
        first_row=0, 
        col_names = ['Region', 'Charge'])
    df_c2['Component'] = ['Volumetric_total' 
            for _ in range(df_c2.shape[0])]
    df = pd.concat([df_c1,df_c2])
    df['Charge'] = df['Charge'].apply(
        convert_to_numeric)
    df['Min_consumption_vol'] = min_mensual[1]
    df['Min_consumption_vol_line'] = min_mensual[0]
    df = add_columns(df, tariff_name=tariff_name, 
        year_month=year_month, link=link)
    return df

def transform_tarifa_1(
        l_tables, tariff_name, year_month, link, min_mensual):
    df = pd.read_html(l_tables[-1].decode())[0]
    col_names = [
        'Variable Component', 
        'Volumetric_total',
        'Volumetric_trenchs']
    df.columns = col_names
    df = add_season(df, 'annual')
    df['Volumetric_total'] = df['Volumetric_total'].apply(
        convert_to_numeric)
    df['Min_consumption_vol'] = [ 
        min_mensual[1] for _ in range(df.shape[0])]  
    df['Min_consumption_vol_line'] = [
        min_mensual[0] for _ in range(df.shape[0])]  
    df = add_columns(df, tariff_name=tariff_name, 
        year_month=year_month, link=link, region='')
    return df

def transform_tarifa_1letter(
        l_tables, tariff_name, year_month, link, min_mensual,
        season):
    df = transform_tarifa_1(l_tables, tariff_name, year_month,
        link, min_mensual)
    if season == 'summer':
        df = add_season(df, 'summer')
    else:
        df = add_season(df, 'out_of_summer')
    return df

def transform_upp_lmt(df):
    df = df.set_index(0).rename(
        columns={
            1:'limit', 
            2:'unit'}
    )
    df['limit'] = df['limit'].apply(
        lambda x : float(x.split()[0].replace(',', '')))
    dic_df = df.to_dict(orient='index')
    return dic_df

#####################################################################

def table_to_df(
        soup_tables, tariff_service, tariff_name, year, link):
    if (tariff_service == 'Hogar_anual') and (
            tariff_name == 'Domestica Bajo Consumo'
    ):
        return process_annual_dbc(
            soup_tables, year, link)
    elif (tariff_service == 'Hogar_anual') and (
            tariff_name == 'Domestica Alto Consumo'
    ):
        return process_annual_dac(
            soup_tables, tariff_name, year, link)

    elif (tariff_service == 'Negocio_anual') and not(
            tariff_name == 'CMAMT'
    ):
        return process_annual_negocio(
            soup_tables, tariff_name, year, link)
    elif (tariff_service == 'Negocio_anual') and ( 
            tariff_name == 'CMAMT'
    ):
        return process_annual_CMAMT(
            soup_tables, tariff_name, year, link)

def check_html(table):
    pattern = re.compile(r'tdalign')
    match = pattern.search(table)
    if match:
        new_table = re.sub(
                r'tdalign', 'td align', table)
        return new_table
    return table

def process_annual_dbc(
    soup_tables, year, link):
    # list all tags with information about the 
    # tariff
    tags = []
    found = False
    for x in soup_tables.findAll(
            'span', 
            {'class':'tituloContenidoRojo'}):
        if 'Tarifa 1' in x:
            found = True
        if found:
            tags.append(x.text.strip())

    # list all tables that do not contain other
    # nested tables 
    l_df = []
    for t in soup_tables.findAll('table'):
        if len(t.findAll('table'))!=0:
            continue
        elif len(t.findAll('td')) > 5:
            t = check_html(t.decode())
            df = pd.read_html(t)[0]
            df.dropna(how='all', inplace=True)
            df.reset_index(drop=True, inplace=True)
            l_df.append(df)

    # join tariff_name and temporada in a single
    # tag
    prefix = ''
    temporada = ''
    l_pt = []
    for t in tags:
        if 'Tarifa' in t:
            if not prefix:
                prefix = t
            elif prefix and not temporada:
                l_pt.append((prefix, temporada))
                prefix = t
            else:                    
                prefix = t
        elif 'TEMPORADA' in t:
            temporada = t
            l_pt.append((prefix, temporada))

    # check that both lists (tags and df) have the 
    # same length 
    if len(l_pt) != len(l_df):
        util.log('local_mexico',
            'year: {},\
            tariff_service: {}, tariff_name: {},\
            nr of tags differs from nr of dataframes')
        print('not the same length')
        raise Exception

    # add columns with the information of season and 
    # tariff name
    l_filename_df = []
    for t, df in zip(l_pt, l_df):
        df['Tariff_code'] = [ t[0] for _ in range(df.shape[0])]
        df['Season_line'] = [
            t[1] for _ in range(df.shape[0])]
        # format each table:
        l_filename_df.append(transform_tables_hogar_dbc_annual(
            df, year, link))
    return l_filename_df

def process_annual_dac(
    soup_tables, tariff_name, year, link):
    is_found = False
    region = ''
    cuota = ''
    # list tags with information of region and 
    # tables
    l_filename_df = []
    for child in soup_tables.children:
        if type(child) == bs4.element.NavigableString:
            continue
        
        # start after finding 'REGION BAJA CALIFORNIA'
        found = child.findAll(
            'span', 
            {'class':'tituloContenidoRojo'}, 
            string='REGION BAJA CALIFORNIA')
        if found:
            is_found = True
        if not is_found:
            continue

        # get the text in span about region or cuotas
        if len(child.findAll('span'))>0:
            text = child.find('span').text
            if 'REGION' in text:
                region = text
            elif 'CUOTAS' in text:
                cuota = text

        # get the tables 
        if len(child.findAll('table'))>0:
            df = pd.read_html(child.decode())[0]
            df.dropna(how='all', inplace=True)
            df['REGION'] = [
                region for _ in range(df.shape[0])]
            df['CUOTAS'] = [
                cuota for _ in range(df.shape[0])]
            condition = df[df.columns.values[0]].astype(str).apply(
                lambda x:'Cuotas por energía' in x)
            df.drop(df[condition].index, inplace=True)
            df.reset_index(drop=True, inplace=True)
            l_filename_df.append(transform_tables_hogar_dac_annual(
                df, year, tariff_name, link))
    return l_filename_df


def process_annual_negocio(soup_tables, tariff_name, year, link):
    '''
    Reads the html pages and saves the tables and other relevant 
    data (as region or tariff name) set as text or headers in the
    page. Then creates the DataFrames with a standard format, 
    with each month and the collected information in a different 
    column

    '''
    # set the string to start saving data of the page 
    if tariff_name == 'CMAS':
        start_string = 'Tarifa 5'
    elif tariff_name == 'CMAA':
        start_string = 'Tarifa 9'
    elif tariff_name == 'CMAT':
        start_string = 'Tarifa 7'
    elif tariff_name == 'CMABT':
        start_string = 'Tarifa 2'
    elif tariff_name == 'CMAAT':
        start_string = 'Tarifa H-S'
    elif tariff_name == 'CMASR':
        start_string = 'Tarifa HM-R'
    elif tariff_name == 'CMASI':
        start_string = 'Tarifa I-15'

    # list the tags about tariff name
    tags = []
    found = False
    for x in soup_tables.findAll(
            'span', {'class':'tituloContenidoRojo'}):
        if start_string in x.text:
            found = True
        if found:
            if (
                tariff_name in ['CMAAT','CMASR', 'CMASI']) and (
                    not x.text.strip().startswith('Tarifa')):
                continue
            tags.append(x.text.strip())
            
    # list the DataFrames 
    l_df = []
    for t in soup_tables.findAll('table'):
        if len(t.findAll('table'))!=0:
            continue
        elif len(t.findAll('td')) > 5:
            l_df.append(pd.read_html(t.decode())[0])

    # add the information about tariff on the df 
    l_df_by_year = []
    for t, df in zip(tags, l_df):
        df['Tariff_code_old_notation'] = [ t for _ in range(df.shape[0])]

        if tariff_name == 'CMAS':
            if t in ['Tarifa 5', 'Tarifa 5A']:
                header = 'Tariff_Voltage_line'
            elif t in ['Tarifa 6']:
                header = 'Component_unit'
            df.columns = df.iloc[0,:]
            df = df.drop(0)
            df.dropna(axis=1, how='all', inplace=True)
            original_col_names = df.columns.values
            new_col_names = np.concatenate((
                np.array([header]),
                original_col_names[1:-1], 
                np.array(['Tariff_old_notation'])
            ))
            df.columns = new_col_names
            df = df[np.concatenate((new_col_names[1:],
                new_col_names[0:1]))]

            l_df_by_year.append(
                transform_tables_negocio_annual(
                   df, t, tariff_name, year, link, 2))

        elif tariff_name == 'CMAA':

            if t in ['Tarifa 9', 'Tarifa 9M']:
                header = 'Voltage_Trench_line'
            elif t in ['Tarifa 9CU', 'Tarifa 9N']:
                header = 'Component'
            df.columns = df.iloc[0,:]
            df = df.drop(0)
            df.dropna(axis=1, how='all', inplace=True)

            # shift columns to begin with months
            original_col_names = df.columns.values
            new_col_names = np.concatenate((
                np.array([header]),
                original_col_names[1:-1], 
                np.array(['Tariff_old_notation'])
            ))
            df.columns = new_col_names
            df = df[np.concatenate((new_col_names[1:],
                new_col_names[0:1]))]

            l_df_by_year.append(transform_tables_negocio_annual(
                   df, t, tariff_name, year, link, 2))

        elif tariff_name == 'CMAT':
            df.dropna(axis=1, how='all', inplace=True)
            component_series = df.loc[df.iloc[:,0].apply(
                lambda x : 'Cargo' in x)].iloc[:,0].copy()
            component_series.reset_index(drop=True, inplace=True)
            df = df.loc[df.iloc[:,0].apply(
                lambda x : not 'Cargo' in x)].copy()
            df.reset_index(drop=True, inplace=True)
            df['Component_unit'] = component_series 

            l_df_by_year.append(
                transform_tables_negocio_annual(
                   df, t, tariff_name, year, link, 2))

        elif tariff_name == 'CMABT':
            pattern_not_tag = re.compile(r'^(?!.*Cargo).*$')
            l_tags = []
            ix_list = []
            tag_1 = df.columns.values[0]
            if not pattern_not_tag.search(tag_1):
                ix_list = [-1]
                l_tags.append(tag_1)
                df.columns = df.iloc[0]
                df.drop(0, inplace=True)
                df.reset_index(drop=True, inplace=True)

            df, tag_col, _ = extract_tag(
                df, 
                pattern_not_tag, 
                l_tags, 
                ix_list)

            df.dropna(axis=1, how='all', inplace=True)
            last_cols = 0 

            # shift columns to begin with months
            if t == 'Tarifa 2': 
                original_col_names = df.columns.values
                new_col_names = np.concatenate((
                    np.array(['Voltage_Trench_line']),
                    original_col_names[1:-1], 
                    np.array(['Tariff_old_notation'])
                ))
                df.columns = new_col_names
                df = df[np.concatenate((new_col_names[1:],
                    new_col_names[0:1]))]
                last_cols = 3

            elif t == 'Tarifa 3':
                original_col_names = df.columns.values
                new_col_names = np.concatenate((
                    original_col_names[0:-1], 
                    np.array(['Tariff_old_notation'])
                ))
                df.columns = new_col_names
                last_cols = 2

            df['Component_unit'] = tag_col

            l_df_by_year.append(transform_tables_negocio_annual(
                       df, t, tariff_name, year, link, last_cols))

        elif tariff_name == 'CMAAT':

            pattern_not_tag = re.compile(r'\({1}[\S\s]+\){1}')
            l_tags = []
            ix_list = []
            # multiindex columns
            midx = df.columns
            original_columns = [ 
                midx.levels[0].values[y] for y in midx.codes[0] ]

            # first tag in column names. Tags are Regions
            tag_1 = [ 
                midx.levels[1].values[y] for y in midx.codes[1] ][0]
            l_tags.append(tag_1)
            ix_list.append(-1)
            df.columns = original_columns

            df, tag_col, _ = extract_tag(
                df, 
                pattern_not_tag, 
                l_tags, 
                ix_list)

            df.dropna(axis=1, how='all', inplace=True)

            # shift columns to begin with months
            original_col_names = df.columns.values
            new_col_names = np.concatenate((
                np.array(['Component_unit']),
                original_col_names[1:-1], 
                np.array(['Tariff_old_notation'])
            ))
            df.columns = new_col_names
            df = df[np.concatenate((new_col_names[1:],
                new_col_names[0:1]))]

            # Add tag cols
            df['Region'] = tag_col

            l_df_by_year.append(
                transform_tables_negocio_annual(
                    df, t, tariff_name, year, link, 3))
        elif tariff_name == 'CMASR':

            l_tags = []
            ix_list = []
            original_columns = list(df.columns.values)
            pattern_fijo = re.compile(r'[Ff]ijo')
            pattern_not_tag = re.compile(r'\({1}[\S\s]+\){1}')

            # extract rows with Cargo Fijo tag
            series_fijo = df.loc[
                df.iloc[:,0].apply(
                        lambda x : True if (
                            pattern_fijo.search(x)) else False
                    )
            ].copy()

            # remove rows with Cargo Fijo tag
            df = df.loc[
                    df.iloc[:,0].apply(
                        lambda x : False if (
                            pattern_fijo.search(x)) else True
                    )
            ].copy()

            df.reset_index(drop=True, inplace=True)

            df, tag_col, ix_diff = extract_tag(
                df, 
                pattern_not_tag, 
                l_tags, 
                ix_list, 
                added_lines=1)

            # Add Cargo Fijo to each region tariff
            df = df_concat(df, series_fijo, ix_diff)

            df.dropna(axis=1, how='all', inplace=True)
            
            df.reset_index(drop=True, inplace=True)

            # rearrange columns. Month columns first
            original_col_names = df.columns.values
            new_col_names = np.concatenate((
                np.array(['Component_unit']),
                original_col_names[1:-1], 
                np.array(['Tariff_old_notation'])
            ))
            df.columns = new_col_names
            df = df[np.concatenate((new_col_names[1:],
                new_col_names[0:1]))]

            # Add region column
            df['Region'] = tag_col

            l_df_by_year.append(
                transform_tables_negocio_annual(
                   df, t, tariff_name, year, link, 3))

        elif tariff_name == 'CMASI':
            component = df.columns.values[0] 
            df.columns = df.iloc[0,:]
            df.drop(0, inplace=True)
            df.dropna(axis=1, how='all', inplace=True)
            df['Cargos'] = df['Cargos'].astype(str)+' '+component

            original_col_names = df.columns.values
            new_col_names = np.concatenate((
                np.array(['Component_unit']),
                original_col_names[1:-1], 
                np.array(['Tariff_old_notation'])
            ))
            df.columns = new_col_names
            df = df[np.concatenate((new_col_names[1:],
                new_col_names[0:1]))]

            l_df_by_year.append(
                transform_tables_negocio_annual(
                   df, t, tariff_name, year, link, 2))
    return l_df_by_year

def format_df_region(df,counter):
    if counter in [0, 1, 2, 5]:
        region = df.columns[0]
        df.columns = list(df.iloc[0,:-1].values) + ['Tarifa']
        df.drop(0, inplace=True)
        
        df['Region'] = [region for _ in range(df.shape[0])]
        df.reset_index(drop=True, inplace=True)
        return df
    elif counter in [3]:
        region = df.columns[0][1]
        midx = df.columns
        original_columns = [ 
            midx.levels[0].values[y] for y in midx.codes[0] ]
        df.columns = original_columns
        l_tags = []
        ix_list = []
        pattern_not_tag = re.compile(r'\({1}[\S\s]+\){1}')
        l_tags.append(region)
        ix_list.append(-1)
        ix_of_df_tags = (
            df.loc[
                df.iloc[:,0].apply(
                   lambda x : False if (
                       pattern_not_tag.search(str(x))) else True
                )
            ].iloc[:,0].index.values
        )
        l_tags.extend(df.iloc[ix_of_df_tags,0].values)
        ix_list.extend(ix_of_df_tags)
        ix_list.append(df.shape[0])
        ix_arr = np.array(ix_list)
        ix_diff = np.diff(ix_arr) - 1
        tag_col = [ [x]*y for x, y in zip(l_tags, ix_diff)]
        tag_col = [ x for y in tag_col for x in y]
        df = df.loc[
                df.iloc[:,0].apply(
                    lambda x : True if (
                        pattern_not_tag.search(x)) else False
                )
        ].copy()
        df['Region'] = tag_col
        df.reset_index(drop=True, inplace=True)
        return df
    elif counter in [4, 6]:
        region = df.iloc[1,0]
        df.columns = list(df.iloc[2].values)[0:-1] + ['Tarifa']
        df.drop([0, 1, 2], inplace=True)
        df.reset_index(drop=True, inplace=True)
        if counter == 6:
            df['Region'] = [region for _ in range(df.shape[0])]
            df.reset_index(drop=True, inplace=True)
            return df
        elif counter == 4:
            l_tags = []
            ix_list = []
            pattern_not_tag = re.compile(r'\({1}[\S\s]+\){1}')
            l_tags.append(region)
            ix_list.append(-1)
            ix_of_df_tags = (
                df.loc[
                    df.iloc[:,0].apply(
                       lambda x : False if (
                           pattern_not_tag.search(str(x))) else True
                    )
                ].iloc[:,0].index.values
            )
            l_tags.extend(df.iloc[ix_of_df_tags,0].values)
            ix_list.extend(ix_of_df_tags)
            ix_list.append(df.shape[0])
            ix_arr = np.array(ix_list)
            ix_diff = np.diff(ix_arr) - 1
            tag_col = [ [x]*y for x, y in zip(l_tags, ix_diff)]
            tag_col = [ x for y in tag_col for x in y]
            df = df.loc[
                    df.iloc[:,0].apply(
                        lambda x : True if (
                            pattern_not_tag.search(x)) else False
                    )
            ].copy()
            df['Region'] = tag_col
            df.reset_index(drop=True, inplace=True)

            return df

def process_annual_CMAMT(soup_tables, tariff_name, year, link):
    '''
    For the case of CMAMT tariff:
    Reads the html pages and saves the tables and other relevant 
    data (as region or tariff name) set as text or headers in the
    page. Then creates the DataFrames with a standard format, 
    with each month and the collected information in a different 
    column

    '''
    tariff_tag = ''
    l_dfs = []
    found = False
    l_trs = soup_tables.find('table').findAll('tr', recursive=False)
    counter = 0
    for _, row in enumerate(l_trs):

        if row.find('span',{'class':'tituloContenidoRojo'}):
            span_elem = row.find(
                'span',
                {'class':'tituloContenidoRojo'})
            if 'Tarifa O-M' in span_elem.text:
                found = True
                tariff_tag = span_elem.text
                continue
            if found and 'Tarifa' in span_elem.text:
                tariff_tag = span_elem.text
                continue

        if found and row.find('table'):
            try:
                df = pd.read_html(row.find('table').decode())[0]
                df['Tarifa'] = [ 
                    tariff_tag for _ in range(df.shape[0])]
            except:
                pattern = re.compile(r'colspan="[0-9]*.[0-9]*"')
                match = pattern.search(row.decode())
                if match:
                    ncols = max(
                        [ len(y.findAll('td')) for 
                            y in row.findAll('tr')])
                    new_row = re.sub(
                        r'colspan="[0-9]+.[0-9]+"', 'colspan"'+\
                            str(ncols)+\
                            '"', row.decode())
                    df = pd.read_html(new_row)[0]
                    df['Tarifa'] = [ 
                        tariff_tag for _ in range(df.shape[0])]
                else:
                    df = 'sin parsear'
            finally:
                df = format_df_region(df,counter)
                l_dfs.append(df)
                counter+=1
            continue

        if (found) and ( row.text.strip()) and (
            row.text.strip().startswith('(')):
            
                if len(l_dfs) > 0 and not (
                    isinstance(l_dfs[-1], str)):
                        l_dfs[-1]['Comentarios'] = [ 
                            ' '.join(row.text.strip().split()) \
                            for _ in range(l_dfs[-1].shape[0])]

    l_df_by_year = []

    for df in l_dfs:
        t = df.loc[0,'Tarifa']
        l_df_by_year.append(
            transform_tables_CMAMT(
               df, t, tariff_name, year, link))
    return l_df_by_year


#####################################################################

def transform_tables_hogar_dbc_annual(df, year, link):
    ncolumns = len(df.columns.values)

    # skip first columns because it is December of 
    # previous year
    l_df_by_year = []
    for n in range(2, ncolumns - 2):
        col_names = [
            'Variable_component', 
            'Volumetric_total',
            'Tariff_code',
            'Season_line']
        month = df.iat[0,n].strip('.')
        try:
            month = dic_months[month]
            year_month = time.strptime(month+'-'+year, '%b-%Y')
        except:
            print('not properly converted date.\
                    year: {}, month: {}'.format(year, month))
            raise Exception

        df_f = concat_cols(
            df,
            [0,1],[n, n+1], [ncolumns-2,ncolumns], 
            first_row=1, 
            col_names = col_names)
        df_f = add_columns(df_f, year_month=year_month, link=link)
        df_f['Volumetric_total'] = df_f['Volumetric_total'].apply(
            convert_to_numeric)
        season = df_f.loc[1,'Season_line']
        if 'DE VERANO' in season and not (
            'FUERA DE VERANO' in season):
            df_f = add_season(df_f, 'summer')
        elif ('FUERA DE VERANO' in season): 
            df_f = add_season(df_f, 'out_of_summer')    
        else:
            df_f = add_season(df_f, 'annual')
        l_df_by_year.append(df_f)
    tariff_name = '_'.join(df_f.loc[1,'Tariff_code'].split())
    date = time.strftime('%Y', year_month)
    season = df_f.loc[1,'Seasontime_differentiation_group']
    file_name = '{}_date_{}.csv'.format(
        tariff_name, date)
    df = pd.concat(l_df_by_year)
    df.reset_index(drop=True, inplace=True)
    return (file_name, df)
    
def transform_tables_hogar_dac_annual(df, year, tariff_name, link):
    ncolumns = len(df.columns.values)

    # skip first columns because it is December of 
    # previous year
    l_df_by_year = []
    for n in range(2, ncolumns - 2):
        col_names = [
            'Component_unit', 
            'Charge',
            'Region',
            'Season_line']
        month = df.columns.values[n].strip('.')
        try:
            month = dic_months[month]
            year_month = time.strptime(month+'-'+year, '%b-%Y')
        except:
            print('not properly converted date.\
                    year: {}, month: {}'.format(year, month))
            raise Exception
        df_f = concat_cols(
            df,
            [0,1],[n, n+1], [ncolumns-2,ncolumns], 
            first_row=0, 
            col_names = col_names)
        df_f = add_columns(
            df_f, tariff_name=tariff_name,
            year_month=year_month, link=link)
        df_f['Charge'] = df_f['Charge'].apply(
            convert_to_numeric)
        df_f['Region'] = df_f['Region'].apply(
            lambda x : x.strip())
        df_f['Season_line'] = df_f['Season_line'].apply(
            lambda x : x.strip())
        region = df_f.loc[1, 'Region']
        season = df_f.loc[1, 'Season_line']
        if 'EN VERANO' in season:
            df_f = add_season(df_f, 'summer')
        elif ('FUERA DE VERANO' in season) and (
                'CALIFORNIA' in region): 
            df_f = add_season(df_f, 'out_of_summer')    
        else:
            df_f = add_season(df_f, 'annual')
        df_f = df_f.apply(comp_to_unit, axis=1)
        l_df_by_year.append(df_f)
    tariff_name = '_'.join(df_f.loc[1,'Tariff_code'].split())
    date = time.strftime('%Y', year_month)
    region = '_'.join(region.split())
    season = df_f.loc[1, 'Seasontime_differentiation_group']
    file_name = '{}_date_{}.csv'.format(
        tariff_name, date)
    l_df = pd.concat(l_df_by_year)
    l_df.reset_index(drop=True, inplace=True)
    return (file_name, l_df)

def transform_tables_negocio_annual(
        df, old_tariff, tariff_name, year, link, last_cols):
    ncolumns = len(df.columns.values)
    pattern_year=re.compile(r'[0-9]{4}')
    pattern_month=re.compile(r'[A-Z]{1}[a-z]{2}')

    col_names = np.concatenate((
        np.array(['Charge']), 
        df.columns.values[-last_cols:]))
    l_df_by_year = []

    for n in range(0, ncolumns - last_cols):
        date_header = df.columns.values[n].strip('.')
        if pattern_year.search(date_header):
            year_of_annotation = pattern_year.search(
                date_header).group()
        else:
            year_of_annotation = year
         
        if pattern_month.search(date_header):
            month = pattern_month.search(
                date_header).group()
        try:
            month = dic_months[month]
            year_month = time.strptime(
                month+'-'+year_of_annotation, 
                '%b-%Y')
        except:
            print('not properly converted date.\
                    year: {}, month: {}'.format(
                        year_of_annotation, 
                        month))
            raise Exception
        df_f = concat_cols(
            df,
            [n, n+1], [ncolumns-last_cols,ncolumns], 
            first_row=0, 
            col_names = col_names)

        df_f = df_f.apply(comp_to_unit, axis=1)
        df_f = df_f.apply(voltage_group, axis=1)
        df_f['Charge'] = df_f['Charge'].apply(
            convert_to_numeric)

        df_f = add_columns(
            df_f, 
            year_month=year_month, 
            link=link, 
            tariff_name=tariff_name)
        l_df_by_year.append(df_f)

    tariff_name = '_'.join(df_f.loc[1,'Tariff_code'].split())
    old_tariff = '_'.join(old_tariff.split())
    date = time.strftime('%Y', year_month)
    file_name = '{}_old_tariff_{}_date_{}.csv'.format(
            tariff_name, old_tariff, date)
    df = pd.concat(l_df_by_year)
    df.reset_index(drop=True, inplace=True)
    return (file_name, df)

def first_sunday(year, month):
    dt = datetime.datetime(year, month, 1)
    dt += datetime.timedelta(days=6-dt.weekday()) 
    return dt

def last_sunday(year, month):
    if month == 12:
        year += 1
        month = 1
    else:
        month += 1
    dt = datetime.datetime(year, month, 1)
    dt += datetime.timedelta(days=6-dt.weekday())
    dt -= datetime.timedelta(days=7)
    return dt

def get_date_with_quotes(date_header, year):
    year_month = get_date(date_header, year)
    if '(1)' in date_header or '(5)' in date_header:
        dt_sunday = last_sunday(year_month.tm_year, year_month.tm_mon)
        dt_last_saturday = dt_sunday - datetime.timedelta(days=1)
        y = dt_last_saturday.year
        m = dt_last_saturday.month
        d = dt_last_saturday.day
        date_end = '-'.join([str(x) for x in [d, m, y]])
        date_end = time.strptime(date_end, '%d-%m-%Y')
        date_end = time.strftime('%d-%m-%Y', date_end)
        date_start = time.strftime('%d-%m-%Y', year_month)
    elif '(2)' in date_header or '(6)' in date_header:
        dt_sunday = last_sunday(year_month.tm_year, year_month.tm_mon)
        y = dt_sunday.year
        m = dt_sunday.month
        d = dt_sunday.day
        date_start = '-'.join([str(x) for x in [d, m, y]])
        date_start = time.strptime(date_start, '%d-%m-%Y')
        date_start = time.strftime('%d-%m-%Y', date_start)
        ndays = calendar.monthrange(
            year_month.tm_year,
            year_month.tm_mon)[1]
        date_end = time.strptime(
            '-'.join(
                map(str,(
                    ndays, 
                    year_month.tm_mon, 
                    year_month.tm_year)
                )
            ), '%d-%m-%Y'
        )
        date_end = time.strftime('%d-%m-%Y', date_end)
    elif '(3)' in date_header:
        date_start = time.strptime('01-04-'+year, '%d-%m-%Y')
        date_start = time.strftime('%d-%m-%Y', date_start)
        dt_sunday = first_sunday(year_month.tm_year, year_month.tm_mon)
        dt_last_saturday = dt_sunday - datetime.timedelta(days=1)
        y = dt_last_saturday.year
        m = dt_last_saturday.month
        d = dt_last_saturday.day
        date_end = '-'.join([str(x) for x in [d, m, y]])
        date_end = time.strptime(date_end, '%d-%m-%Y')
        date_end = time.strftime('%d-%m-%Y', date_end)
    elif '(4)' in date_header:
        dt_sunday = first_sunday(year_month.tm_year, year_month.tm_mon)
        y = dt_sunday.year
        m = dt_sunday.month
        d = dt_sunday.day
        date_start = '-'.join([str(x) for x in [d, m, y]])
        date_start = time.strptime(date_start, '%d-%m-%Y')
        date_start = time.strftime('%d-%m-%Y', date_start)
        date_end = time.strptime('30-04-'+year, '%d-%m-%Y')
        date_end = time.strftime('%d-%m-%Y', date_end)
    return date_start, date_end

def get_date(date_header, year):

    pattern_year=re.compile(r'[0-9]{4}')
    pattern_month=re.compile(r'[A-Z]{1}[a-z]{2}')

    if pattern_year.search(date_header):
        year_of_annotation = pattern_year.search(date_header).group()
    else:
        year_of_annotation = year
     
    if pattern_month.search(date_header):
        month = pattern_month.search(date_header).group()
    try:
        month = dic_months[month]
        year_month = time.strptime(
            month+'-'+year_of_annotation, 
            '%b-%Y')
        return year_month
    except:
        print('not properly converted date.\
                year: {}, month: {}'.format(
                    year_of_annotation, 
                    month))
        raise Exception


def transform_tables_CMAMT(df, old_tariff, tariff_name, year, link):
    ncolumns = len(df.columns.values)
    # skip last columns because it is December of 
    # previous year
    l_df_by_year = []
    if 'Comentarios' in df.columns.values:
        ncols = 3
    else:
        ncols = 2

    for n in range(1, ncolumns - (ncols + 1)):
        col_names = [
            'Component_unit', 
            'Charge',
            'Tariff_code_old_notation',
            'Region']
        if ncols == 3:
            col_names = col_names + ['Comentarios']
        df_f = concat_cols(
            df,
            [0,1],[n, n+1], [ncolumns-ncols,ncolumns], 
            first_row=0, 
            col_names = col_names)
        df_f = df_f.apply(comp_to_unit, axis=1)
        date_header = df.columns.values[n].strip('.')
        
        l_additions = ['(1)', '(2)', '(3)', '(4)', '(5)', '(6)']
        if any([ x in date_header for x in l_additions]):
            date_start, date_end = get_date_with_quotes(
                date_header, 
                year)
            df_f = add_columns(
                df_f, 
                link=link, 
                date_start_end=(date_start, date_end),
                tariff_name=tariff_name)
        else:
            year_month = get_date(date_header, year)
            df_f = add_columns(
                df_f, 
                year_month=year_month, 
                link=link, 
                tariff_name=tariff_name)

        df_f['Charge'] = df_f['Charge'].apply(
            convert_to_numeric)
        l_df_by_year.append(df_f)
    tariff_name = '_'.join(df_f.loc[1,'Tariff_code'].split())
    old_tariff = '_'.join(old_tariff.split())
    date = time.strftime('%Y', year_month)
    file_name = '{}_old_tariff_{}_date_{}.csv'.format(
            tariff_name, old_tariff, date)
    df = pd.concat(l_df_by_year)
    df.reset_index(drop=True, inplace=True)
    return (file_name, df)
