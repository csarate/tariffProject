import time
import datetime
import os
import requests_html

import pandas as pd

from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup

from . import aux_mexico 
from ..utils_test import util

def savedf_power_upperlimit(dic_pow_upp_lmt, soup, year):
    try:
        df = pd.read_html(
            soup.find('table').findAll('table')[1].decode())[0]
    except:
        print('Could not read_html of power upper limit table\
            from year: {}'.format(year))
        util.log('Could not read_html of power upper limit table\
            from year: {}'.format(year))
        raise Exception 
    dic_ = aux_mexico.transform_upp_lmt(df)
    dic_pow_upp_lmt[year] = dic_
    return dic_pow_upp_lmt

def savedf_tariff_table_a(
        output_path, data, soup, tariff_name, link, 
        **kwargs):

    # set the date (year_month and season depending on tafiff code)
    year_month = kwargs['year_month']
    tariffname = '_'.join(tariff_name.split(' '))
    date = time.strftime('%m-%Y', year_month)
    file_name = '{}_date_{}.csv'.format(
        tariffname, 
        date)

    # create the list of inner tables
    l_tables = soup.find('table').findAll('table')
    table = l_tables[-1]

    try:
        df = aux_mexico.transform_tables_agricola(
            table, 
            tariff_name, 
            year_month, 
            link) 
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
            datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            'MEXICO',
            'CFE',
            '',
            '',
            ''])
        util.dump_files_s3(data, output_path, local)
    except:
        print('Not able to save tariff: {},\
            date: {}, season: {}'.format(
                tariff_name, 
                date))
        util.log('Not able to save tariff: {},\
            date: {}, season: {}'.format(
                tariff_name, 
                date))

def savedf_tariff_table_h(
        output_path, data, soup, tariff_name, link, 
        **kwargs):
    min_mensual = aux_mexico.find_minimo_mensual(soup)
    if not min_mensual[0]:
        tags = ', '.join(
            [ str(key)+':'+str(val) for key, val in kwargs.items()])
        print('Did not found mínimo mensual in tariff: {},\
            {}'.format(
                tariff_name, 
                tags))
        util.log('Did not found mínimo mensual in tariff: {},\
            {}'.format(
                tariff_name, 
                tags))
        return False

    # set the date (year_month and season depending on tafiff code)
    year_month = kwargs['year_month']
    season = kwargs.setdefault('season', '')

    # create the list of inner tables
    l_tables = soup.find('table').findAll('table')
    tariffname = '_'.join(tariff_name.split(' '))
    date = time.strftime('%m-%Y', year_month)

    # process and format the dataframes
    try:
        df = aux_mexico.transform_tables_hogar(
            l_tables,
            tariff_name, 
            year_month, 
            link, 
            min_mensual,
            season)
        if season:
            file_name = '{}_date_{}_season_{}.csv'.format(
                tariffname, 
                date,
                season)
        else:
            file_name = '{}_date_{}.csv'.format(
                tariffname, 
                date)
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
            datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            'MEXICO',
            'CFE',
            '',
            '',
            ''])
        util.dump_files_s3(data, output_path, local)
    except:
        print('Not able to save tariff: {},\
            date: {}, season: {}'.format(
                tariff_name, 
                date,
                season))
        util.log('Not able to save tariff: {},\
            date: {}, season: {}'.format(
                tariff_name, 
                date,
                season))

def savedf_tariff_table_n(
        output_path, data, soup, tariff_name, link, 
        **kwargs):
    print('tariff name', tariff_name)
    year_month = kwargs['year_month']
    region = kwargs['region']
    # create the list of inner tables
    if tariff_name in [
        'APBT', 'APMT', 'PDBT', 'GDBT', 'GDMTO','GDMTH', 'DIST']:
        ntable = 2
    elif tariff_name in ['RABT', 'RAMT', 'DIT']:
        ntable = 3
    table = soup.find('table').findAll('table')[
        ntable].find('table')

    tariffname = '_'.join(tariff_name.split(' '))
    date = time.strftime('%m-%Y', year_month)
    try:
        df = aux_mexico.transform_tables_negocio(
            table,
            tariff_name, 
            year_month, 
            region, 
            link)
        file_name = '{}_date_{}_region_{}.csv'.format(
            tariffname, 
            date, 
            region)
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
            datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            'MEXICO',
            'CFE',
            '',
            '',
            ''])
        util.dump_files_s3(data, output_path, local)
    except:
        print('Not able to save tariff: {},\
            date: {}, region: {}'.format(
                tariff_name, 
                date,
                region))
        util.log('Not able to save tariff: {},\
            date: {}, region: {}'.format(
                tariff_name, 
                date,
                region))


def scraper_month(
        output_path, data, past_links, session, soup, 
        tariff_service, tariff_name, link_tariff):
 
    # loop over years
    year_selector = aux_mexico.Selector(soup, 'year')
    for year in year_selector.list_property('value'):
    #for year in ['2022']:
        print('year : {}'.format(year))

        # if year is not the selected year make a post
        # that retrieves the new year page 
        if year != year_selector.selected:
            form_filler = aux_mexico.form_to_post(
                soup,
                tariff_service=tariff_service,
                tariff_name=tariff_name)
            form_filler.fill_target(
                target = 'year', 
                year = year)
            data_to_post = form_filler.form
            r = util.post_to_link(
                    session, 
                    link_tariff,
                    data_to_post,
                    local)
            if not r:
                continue
            #r = session.post(
            #    link_tariff, 
            #    data=data_to_post)
            soup = BeautifulSoup(r.text, 'lxml')
        
        # from here the year is set
        if tariff_name == 'Domestica Alto Consumo':
            power_upper_limit = util.read_files_s3(
                't_hogar_pow_upp_limit.json', 
                local)
            if not power_upper_limit:
                power_upper_limit = {}
            try:
                savedf_power_upperlimit(
                    power_upper_limit,
                    soup, 
                    year)
            except:
                util.log('Not able to save the power\
                    upper limit table for DAC tariff of\
                    year: {}'.format(
                        year))
                print('Not able to save the power\
                    upper limit table for DAC tariff of\
                    year: {}'.format(
                        year))

            util.dump_files_s3(
                power_upper_limit, 
                't_hogar_pow_upp_limit.json',
                local)

        # for tariffs 'Domestica Alto Consumo' and 'Tarifa 1'
        # with no seasonality
        month_selector = aux_mexico.Selector(soup, 'month')
        for month in month_selector.list_property('value'):
        #for month in ['1']:
            print('month', month)
            if month == '0':
                continue
            year_month = time.strptime(
                str(month)+'-'+str(year), '%m-%Y' )
            tariffname = '_'.join(tariff_name.split(' '))
            date = time.strftime('%m-%Y', year_month)
            file_name = '{}_date_{}.csv'.format(
                tariffname, 
                date)
            if file_name in past_links:
                continue
            if month != month_selector.selected:
                form_filler = aux_mexico.form_to_post(
                    soup,
                    tariff_service=tariff_service,
                    tariff_name=tariff_name)
                form_filler.fill_target(
                    target = 'month', 
                    month = month)
                data_to_post = form_filler.form
                r = util.post_to_link(
                        session, 
                        link_tariff,
                        data_to_post,
                        local)
                soup = BeautifulSoup(r.text, 'lxml')
                if tariff_name in ['9N', '9CU']:
                    savedf_tariff_table_a(
                        output_path,
                        data,
                        soup, 
                        tariff_name,
                        link_tariff,
                        year_month=year_month) 
                else:
                    savedf_tariff_table_h(
                        output_path,
                        data,
                        soup, 
                        tariff_name,
                        link_tariff,
                        year_month=year_month) 

def scraper_season(
        output_path, data, past_links, session, soup,
        tariff_service, tariff_name, link_tariff):
    # loop over years
    year_selector = aux_mexico.Selector(soup, 'year')
    for year in year_selector.list_property('value'):
    #for year in ['2022']:
        print('year : {}'.format(year))

        # if year is not the selected year make a post
        # that retrieves the new year page 
        if year != year_selector.selected:
            form_filler = aux_mexico.form_to_post(
                soup,
                tariff_service=tariff_service,
                tariff_name=tariff_name)
            form_filler.fill_target(
                target = 'year', 
                year = year)
            data_to_post = form_filler.form
            r = util.post_to_link(
                    session, 
                    link_tariff,
                    data_to_post,
                    local)
            soup = BeautifulSoup(r.text, 'lxml')
        # for tariffs 1B 1C 1D 1E 1F there is seasonality.
        # Some months have a summer period tariff and an 
        # out_of_summer period tariff, depending on when 
        #summer begins in a region.
        #--------------------------------------------------------
        # summer
        # start  JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC
        #--------------------------------------------------------
        # FEB(2)   0   1   1   1   1   1   1   0   0   0   0   0
        # MAR(3)   0   0   1   1   1   1   1   1   0   0   0   0
        # APR(4)   0   0   0   1   1   1   1   1   1   0   0   0
        # MAY(5)   0   0   0   0   1   1   1   1   1   1   0   0
        #
        # (0:out_of_summer, 1:summer).
        # Only need to sample a subset: summer start at feb 
        # (all months), and may (only feb, mar, apr, aug, sep, 
        # oct and nov)

        # loop over month verano 1
        start_summer_selector = aux_mexico.Selector(
            soup, 
            'month_v1')
        for month1 in start_summer_selector.list_property('value'):
        #for month1 in ['2']:
            if month1 in ['0', '3', '4']:
                continue
            print('month_v1', month1)
            if month1 != start_summer_selector.selected:
                form_filler = aux_mexico.form_to_post(
                    soup,
                    tariff_service=tariff_service,
                    tariff_name=tariff_name)
                form_filler.fill_target(
                    target = 'month_v1', 
                    month1 = month1)
                data_to_post = form_filler.form
                r = util.post_to_link(
                        session, 
                        link_tariff,
                        data_to_post,
                        local)
                soup = BeautifulSoup(r.text, 'lxml')
             
            # loop over month verano 2
            month_selector = aux_mexico.Selector(
                soup, 
                'month_v2')
            for month2 in month_selector.list_property('value'):
            #for month2 in ['1']:
                if month2 == '0':
                    continue
                if (month1 == '5') and (
                        month2 not in [
                            '2', '3', '4', '8', '9', '10']):
                    continue
                print('month_v2', month2)
                year_month = time.strptime(
                    str(month2)+'-'+str(year), '%m-%Y' )
                season = aux_mexico.dic_season[
                    str(month1), str(month2)]
                tariffname = '_'.join(tariff_name.split(' '))
                date = time.strftime('%m-%Y', year_month)
                file_name = '{}_date_{}_season_{}.csv'.format(
                    tariffname, 
                    date,
                    season)
                if file_name in past_links:
                    continue
                if month2 != month_selector.selected:
                    form_filler = aux_mexico.form_to_post(
                        soup,
                        tariff_service=tariff_service,
                        tariff_name=tariff_name)
                    form_filler.fill_target(
                        target = 'month_v2', 
                        month2 = month2)
                    data_to_post = form_filler.form
                    #print('data_to_post form = {}'.format(data_to_post))
                    r = util.post_to_link(
                        session, 
                        link_tariff,
                        data_to_post,
                        local)
                    soup = BeautifulSoup(r.text, 'lxml')
                savedf_tariff_table_h(
                    output_path,
                    data,
                    soup, 
                    tariff_name, 
                    link_tariff,
                    year_month=year_month,
                    season=season)


def scraper_region(
        output_path, data, past_links, session, soup, 
        tariff_service, tariff_name, link_tariff):

    # loop over years
    year_selector = aux_mexico.Selector(soup, 'year')
    for year in year_selector.list_property('value'):
    #for year in ['2022']:
        print('year : {}'.format(year))
    
        # if year is not the selected year make a post
        # that retrieves the new year page 
        if year != year_selector.selected:
            form_filler = aux_mexico.form_to_post(
                soup,
                tariff_service=tariff_service,
                tariff_name=tariff_name)
            form_filler.fill_target(
                target = 'year', 
                year = year)
            data_to_post = form_filler.form
            r = util.post_to_link(
                session, 
                link_tariff,
                data_to_post,
                local)
            soup = BeautifulSoup(r.text, 'lxml')
            

        # loop over states
        estados_selector = aux_mexico.Selector(soup, 'estado')
        l_values = estados_selector.list_property('value')
        l_text = estados_selector.list_property('text')
        for v_estado, n_estado in list(zip(l_values, l_text)):
        #for v_estado, n_estado in list(zip(l_values, l_text))[0:2]:
            if v_estado == '0':
                continue
            print(n_estado)
            if v_estado != estados_selector.selected:
                form_filler = aux_mexico.form_to_post(
                    soup,
                    tariff_service=tariff_service,
                    tariff_name=tariff_name)
                form_filler.fill_target(
                    target = 'estado', 
                    estado = v_estado)
                data_to_post = form_filler.form
                r = util.post_to_link(
                    session, 
                    link_tariff,
                    data_to_post,
                    local)
                soup = BeautifulSoup(r.text, 'lxml')

            # loop over municipalities
            municipios_selector = aux_mexico.Selector(
                soup, 
                'municipio')
            l_values = municipios_selector.list_property('value')
            l_text = municipios_selector.list_property('text')
            #for v_municipio, n_municipio in list(zip(l_values,l_text))[0:2]:
            for v_municipio, n_municipio in list(zip(l_values,l_text)):
                if v_municipio == '0':
                    continue
                print(n_municipio)
                if v_municipio != municipios_selector.selected:
                    form_filler = aux_mexico.form_to_post(
                        soup,
                        tariff_service=tariff_service,
                        tariff_name=tariff_name)
                    form_filler.fill_target(
                        target = 'municipio', 
                        municipio = v_municipio)
                    data_to_post = form_filler.form
                    r = util.post_to_link(
                        session, 
                        link_tariff,
                        data_to_post,
                        local)
                    soup = BeautifulSoup(r.text, 'lxml')

                # loop over divisions
                divisiones_selector = aux_mexico.Selector(
                    soup, 
                    'division')
                l_values = divisiones_selector.list_property('value')
                l_text = divisiones_selector.list_property('text')
                for v_division, n_division in list(zip(l_values, l_text)):
                #for v_division, n_division in list(zip(l_values, l_text))[0:2]:
                    if v_division == '0':
                        continue
                    print(n_division)
                    if v_division != divisiones_selector.selected:
                        form_filler = aux_mexico.form_to_post(
                            soup,
                            tariff_service=tariff_service,
                            tariff_name=tariff_name)
                        form_filler.fill_target(
                            target = 'division', 
                            division = v_division)
                        data_to_post = form_filler.form
                        r = util.post_to_link(
                            session, 
                            link_tariff,
                            data_to_post,
                            local)
                        soup = BeautifulSoup(r.text, 'lxml')

                    # loop over months
                    month_selector = aux_mexico.Selector(soup, 'month')
                    for month in month_selector.list_property('value'):
                    #for month in month_selector.list_property('value')[0:2]:
                        if month == '0':
                            continue
                        year_month = time.strptime(
                            str(month)+'-'+str(year), '%m-%Y' )
                        tariffname = '_'.join(tariff_name.split(' '))
                        region = '_'.join([v_estado,v_municipio, 
                            v_division])
                        date = time.strftime('%m-%Y', year_month)
                        file_name = '{}_date_{}_region_{}.csv'.format(
                            tariffname, 
                            date, 
                            region)
                        if file_name in past_links:
                            continue
                        print('month', month)
                        form_filler = aux_mexico.form_to_post(
                            soup,
                            tariff_service=tariff_service,
                            tariff_name=tariff_name)
                        form_filler.fill_target(
                            target = 'month', 
                            month = month)
                        data_to_post = form_filler.form
                        r = util.post_to_link(
                            session, 
                            link_tariff,
                            data_to_post,
                            local)
                        soup = BeautifulSoup(r.text, 'lxml')
                        savedf_tariff_table_n(
                            output_path,
                            data,
                            soup, 
                            tariff_name, 
                            link_tariff,
                            year_month=year_month, 
                            region=region)
                        

def scraper_cfe_current_tariffs(time_to_search , output_path, local):

    util.create_s3dirs()
    util.log(local,'Started the scraping')
    print('beggining with the scraping of mexico')
    beginning_time = time.time()
    session = requests_html.HTMLSession()
    retries = Retry(
        total=5,
        backoff_factor=0.1,
        status_forcelist=[ 500, 502, 503, 504 ],
        raise_on_status=True
    )
    session.mount('http://', HTTPAdapter(max_retries=retries))

    base_link = 'https://app.cfe.mx/Aplicaciones/CCFE/Tarifas'

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
    # get the links to domestic tariffs as lists. Invert to begin
    # with domésticas alto Consumo

    util.log(local,'Started the scraping hogar tariffs')
    print('beggining with the scraping of hogar tariffs')
    link_domesticas = 'https://app.cfe.mx/Aplicaciones/CCFE/Tarifas/TarifasCRECasa/Casa.aspx'
    r = util.connect_to_link(session, link_domesticas, local) 
    if not r:
        return False

    soup = BeautifulSoup(r.text, 'lxml')
    #soup = BeautifulSoup(response.html.html , 'lxml')
    
    l_a = aux_mexico.a_tariffs(soup, 'domesticas')[::-1]
    for a_tag in l_a:
        suffix = a_tag['href']
        link_tariff = base_link + suffix.strip('..')
        tariff_name = a_tag['title'] 
        r = util.connect_to_link(session, link_tariff, local) 
        if not r:
            return False
        #r = session.get(link_tariff)
        soup = BeautifulSoup(r.text, 'lxml')
        if tariff_name in ['Domestica Alto Consumo', 'Tarifa 1']:
            tariff_service = 'Hogar_by_month'
            print(tariff_name, tariff_service)
            scraper_month(
                output_path,
                data,
                past_links,
                session, 
                soup, 
                tariff_service, 
                tariff_name, 
                link_tariff)
        else:
            tariff_service = 'Hogar_by_season'
            print(tariff_name, tariff_service)
            scraper_season(
                output_path,
                data,
                past_links,
                session, 
                soup, 
                tariff_service, 
                tariff_name, 
                link_tariff)

    print('beggining with the scraping of negocio tariffs')
    link_negocio = 'https://app.cfe.mx/Aplicaciones/CCFE/Tarifas/TarifasCRENegocio/Negocio.aspx'
    r = util.connect_to_link(session, link_negocio, local) 
    if not r:
        return False
    #r = session.get(link_negocio)
    #print('session status_code : {}'.format(r.status_code))
    soup = BeautifulSoup(r.text, 'lxml')

    # get the links to tariffs as lists
    l_a = aux_mexico.a_tariffs(soup, 'negocios')
    for a_tag in l_a:
        suffix = a_tag['href']
        link_tariff = base_link + suffix.strip('..')
        tariff_name = a_tag.text 
        if a_tag.text not in ['9CU', '9N']: 
            r = util.connect_to_link(session, link_tariff, local) 
            if not r:
                return False
           # r = session.get(link_tariff)
            soup = BeautifulSoup(r.text, 'lxml')
            tariff_service='Negocio'
            print(tariff_name, tariff_service)
            scraper_region(
                output_path,
                data,
                past_links,
                session, 
                soup, 
                tariff_service, 
                tariff_name, 
                link_tariff)
            pass
        elif a_tag.text in ['9CU', '9N']: 
            r = util.connect_to_link(session, link_tariff, local) 
            if not r:
                return False
            #r = session.get(link_tariff)
            soup = BeautifulSoup(r.text, 'lxml')
            tariff_service='Agricola'
            print(tariff_name, tariff_service)
            scraper_month(
                output_path,
                data,
                past_links,
                session, 
                soup, 
                tariff_service, 
                tariff_name, 
                link_tariff)

    print('beggining with the scraping of industria tariffs')
    link_industria = 'https://app.cfe.mx/Aplicaciones/CCFE/Tarifas/TarifasCREIndustria/Industria.aspx'
    r = util.connect_to_link(session, link_industria, local) 
    if not r:
        return False
    #r = session.get(link_industria)
    print('session status_code : {}'.format(r.status_code))
    soup = BeautifulSoup(r.text, 'lxml')

    # get the links to tariffs as lists
    l_a = aux_mexico.a_tariffs(soup, 'industria')
    for a_tag in l_a:
        suffix = a_tag['href']
        link_tariff = base_link + suffix.strip('..')
        tariff_name = a_tag.text 
        if a_tag.text in ['DIST', 'DIT']: 
            r = util.connect_to_link(session, link_tariff,local) 
            if not r:
                return False
            #r = session.get(link_tariff)
            soup = BeautifulSoup(r.text, 'lxml')
            tariff_service='Industria'
            print(tariff_name, tariff_service)
            scraper_region(
                output_path,
                data,
                past_links,
                session, 
                soup, 
                tariff_service, 
                tariff_name, 
                link_tariff)
    return True
