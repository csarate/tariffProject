import os
import re
import time
import datetime
import requests
from requests.adapters import HTTPAdapter, Retry
import requests_html

import pandas as pd

from bs4 import BeautifulSoup

from ..utils_test import util
from . import parser_peru

convert_months = {
    'Ene' : 'Jan',
    'Feb' : 'Feb',
    'Mar' : 'Mar',
    'Abr' : 'Apr',
    'May' : 'May', 
    'Jun' : 'Jun',
    'Jul' : 'Jul',
    'Ago' : 'Aug',
    'Set' : 'Sep',
    'Oct' : 'Oct',
    'Nov' : 'Nov',
    'Dic' : 'Dec'
}
link_osinergmin = 'https://www.osinergmin.gob.pe/seccion/institucional/regulacion-tarifaria/pliegos-tarifarios/electricidad/pliegos-tarifiarios-cliente-final'


def cut_old_dates(options_dates, date_lower_limit):
    ldates = []
    for date in options_dates:
        match = re.match(
            r'[0-9]*/[A-Z]{1}[a-z]{2}/[0-9]{4}', 
            date.strip()
        )
        if match.group():
            day, mon, year = match.group().split('/')
            try:
                mon = convert_months[mon]
            except KeyError as e:
                print('badly format month {}'.format(mon))

                util.log(
                    'local_peru', 
                    'badly format month {}'.format(mon)
                )
                continue
            time_date = time.strptime(
                day+'/'+mon+'/'+year, 
                '%d/%b/%Y')
            if time_date >= date_lower_limit:
                ldates.append(date)
            else:
                continue
        else:
            print('badly format date {}'.format(date))
            util.log(
                'local_peru', 
                'badly format date {}'.format(date)
            )
            continue
    return ldates

# for testing
def cut_newest_dates(options_dates, date_upper_limit):
    ldates = []
    for date in options_dates:
        match = re.match(
            r'[0-9]*/[A-Z]{1}[a-z]{2}/[0-9]{4}', 
            date.strip()
        )
        if match.group():
            day, mon, year = match.group().split('/')
            try:
                mon = convert_months[mon]
            except KeyError as e:
                print('badly format month {}'.format(mon))

                util.log(
                    'local_peru', 
                    'badly format month {}'.format(mon)
                )
                continue
            time_date = time.strptime(
                day+'/'+mon+'/'+year, 
                '%d/%b/%Y')
            if time_date <= date_upper_limit:
                ldates.append(date)
            else:
                continue
        else:
            print('badly format date {}'.format(date))
            util.log(
                'local_peru', 
                'badly format date {}'.format(date)
            )
            continue
    return ldates

def scraper_osinergmin(time_to_search , output_path, date_start, date_end):
    '''
    time_to_search = float (seconds)
    output_path = to test the script output_path is the filename 
        with the saved links in local s3 dir
    '''
    beginning_time = time.time()

    # check if local dirs are created and create them if they 
    # are missing
    util.create_s3dirs()

    # log the start of the script
    util.log(
        'local_peru',
        'Started the scraping')
    print('Started the scraping of Peru tariffs')

    # search tariffs newer than the date_lower_limit
    # date_lower_limit = time.strptime('01-08-2012','%d-%m-%Y')
    # date_upper_limit = time.strptime('01-10-2012','%d-%m-%Y')
    date_lower_limit = time.strptime(date_start,'%d-%m-%Y')
    date_upper_limit = time.strptime(date_end,'%d-%m-%Y')
    # read already processed and saved links so as not to repeat
    data = util.read_files_s3(output_path,'local_peru')
    if not data:
        data = []
    
    # append to a list the already gathered links 
    past_links = []
    for line in data:
        try:
            past_links.append(line[1])
        except IndexError as e:
            util.log(
                'local_peru',
                'badly formed link on saved links = {}'.format(
                line)
            )

    # es útil este while????
    while time.time() - beginning_time < time_to_search:

        s = requests_html.HTMLSession()
        retries = Retry(
            total=5,
            backoff_factor=0.1,
            status_forcelist=[ 500, 502, 503, 504 ],
            raise_on_status=True
        )
        s.mount('http://', HTTPAdapter(max_retries=retries))
        
        response = util.connect_to_link(s, link_osinergmin, 'local_peru') 
        if not response:
            return False

        response.html.render(timeout=10 , sleep=5)
        soup = BeautifulSoup(response.html.html , 'lxml')
        area_links = soup.find_all('area')
        if area_links:
            pages = [ 
                x['href'] for x in area_links if 'coords' in x.attrs 
            ]
        else:
            util.log(
                'local_peru',
                'Table with links to regions not found in {}.\
                Leaving'.format(link_osinergmin)
            )
            return False

        for page in pages:

            print("open region page")

            response = util.connect_to_link(s, page, 'local_peru')
            if not response:
                continue
            
            soup = BeautifulSoup(response.text , "lxml")

            options_zones = [ 
                item['value'] 
                for item in soup.select('#DDLSE option')
                if soup.select('#DDLSE option')
            ]
 
            options_dates = [
                item['value']
                for item in soup.select('#DDLFecha option')
                if soup.select('#DDLFecha option')
            ]

            # select dates from date_lower_limit
            options_dates = cut_old_dates(
                options_dates, date_lower_limit
            )
            options_dates = cut_newest_dates(
                options_dates, date_upper_limit
            )

            if not options_zones: 
                print('zones Not Found in page {}'.format(page))
                util.log(
                    'local_peru', 
                    'zones Not Found in page {}'.format(page))
                continue
            elif not options_dates:
                print('dates Not Found in page {}'.format(page))
                util.log(
                    'local_peru', 
                    'dates Not Found in page {}'.format(page))
                continue
            # iterate over zone options
            for zone in options_zones:

                # iterate over date options
                for date_start, date_end in zip(
                    options_dates[1:], options_dates[:-1]):
                    #for date in options_dates:
                    date = date_end
                    options = \
                        {
                            'DDLSE': zone ,
                            'DDLFecha': date
                        }

                    match_date_zone = '{}_{}'.format(
                        zone.strip(), 
                        date.replace("/","-").strip()
                    )
                    if any([ (match_date_zone in x) for x in past_links ]):
                        print(
                            'skiped already saved zone:{}, date:{}'.format(
                                zone.strip(), 
                                date.replace("/","-").strip())
                        )
                        util.log('local_peru',
                            'skiped already saved zone:{}, date:{}'.format(
                                zone.strip(), 
                                date.replace("/","-").strip())
                        )
                        continue

                    response = util.post_to_link(s, page, options, 'local_peru')
                    if not response:
                        continue

                    # search company name 
                    soup = BeautifulSoup(response.text, 'lxml')
                    interconnection = soup.find(
                        'span' , 
                        {'id': 'LBInter'}
                    )
                    if interconnection:
                        interconnection = interconnection.text.strip()
                    else:
                        interconnection = 'Not Found'

                    sector = soup.find(
                        'span' , 
                        {'id': 'LBSector'}
                    )
                    if sector:
                        sector = sector.text.strip()
                    else:
                        sector = 'Not Found'
                    company_name = soup.find(
                        'span' , 
                        {'id': 'LBEmpresa'}
                    )
                    if company_name:
                        company_name = company_name.text.strip()
                    else:
                        print('Company name not found in:\n\
                            page = {}\n\
                            zone = {}\n\
                            date = {}\n'.format(
                                page, zone, date
                            )
                        )
                        util.log(
                            'local_peru', 
                            'Company name not found in:\n\
                            page = {}\n\
                            zone = {}\n\
                            date = {}\n'.format(
                                page, zone, date
                            )
                        )
                        continue
                    # if company_name, date and zone (subarea) are set
                    # try to get html tariff table
                    print('Getting data from:\n\
                        company = {}\n\
                        date = {}\n\
                        zone = {}\n'.format(
                            company_name, date, zone
                        )
                    )
                    util.log(
                        'local_peru',
                        'Getting data from:\n\
                        company = {}\n\
                        date = {}\n\
                        zone = {}\n'.format(
                            company_name, date, zone
                        )
                    )
                    file_name_base = '{}_{}_{}'.format(
                        company_name, 
                        zone.strip(), 
                        date.replace("/","-").strip()
                    )

                    if not soup.find('table'):
                        print('html table not found in:\n\
                            page = {}\n zone = {}\n\
                            date = {}'.format(
                                page, zone, date
                            )
                        )
                        util.log(
                            'local_peru', 
                            'html table not found in:\n\
                            page = {}\n zone = {}\n\
                            date = {}'.format(
                                page, zone, date
                            )
                        )
                        continue
                    l_dfs = pd.read_html(response.text)
                    df_tariff = l_dfs[0]
                    print('calling parser with:\n\
                            company_name: {}\n\
                            date_start: {}\n\
                            date_end: {}\n\
                            sector: {}\n\
                            interconnection: {}\n'.format(
                                company_name, date_start, date_end,
                                sector, interconnection)
                                )
 
                    try:
                        df_tariff_table = \
                            parser_peru.format_tariff_table_peru(
                                soup, 
                                df_tariff, 
                                company_name, 
                                page, 
                                date_start, 
                                date_end, 
                                sector, 
                                interconnection
                            )
                    except:
                        print('Error parsing html')
                        util.log(
                            'local_peru',
                            'Error parsing html')
                        return False

                    file_name = file_name_base+'.csv'
                    df_tariff_table.to_csv(
                        os.path.join(
                            util.setup_dic['local_peru'], 
                            file_name
                        )
                    )
                    print('saved file {}'.format(file_name))
                    util.log(
                        'local_peru',
                        'saved file {}'.format(file_name))
                    data.append([
                        link_osinergmin, 
                        file_name,
                        datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S"),
                        0,
                        'PERU',
                        'osinergmin',
                        '',
                        '',
                        ''])
                    util.dump_files_s3(data, output_path, 'local_peru')
            print('Ended with page {}'.format(page))
            util.log(
                'local_peru', 
                'Ended with page {}'.format(page))
        util.log(
            'local_peru', 
            'Ended with Peru scraping')
        return True

