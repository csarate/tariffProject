import requests
import pandas as pd
import time
import re

from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup

from ..utils_test import util
from .aux_tariff import TariffParserArgentina 



def argentina_tariffs(time_to_search, output_path,date_lower_limit):
    '''
    time_to_search = float (seconds)
    output_path = to test the script output_path is the filename with
        the saved links in local s3 dir
    '''
    # create dir to save results if it is not created
    util.create_s3dirs()
    # log the start of the script
    util.log('local_argentina','Started the scraping')
    print('Started the scraping of Argentina tariffs')
    date_lower_limit = time.strptime(date_lower_limit,'%Y%m')

    # read already processed and saved links so as not to repeat
    data = util.read_files_s3(output_path,'local_argentina')
    if not data:
        data = []

    # get the already prcessed links in second position
    past_links = []
    for line in data:
        try:
            past_links.append(line[1])
        except IndexError as e:
            util.log('local_argentina',
                'badly formed link on saved links = {}'.format(
                line)
            )
    # urls
    enre_web = 'https://www.enre.gov.ar/web/'
    tariff_table_general_link = 'https://www.enre.gov.ar/web/TARIFASD.nsf/\
        todoscuadros?OpenView&amp;collapseview'
    base_link = tariff_table_general_link.split('&')[0]

    # counter for the already visited links
    previous_items = 0
    new_items = 0
    
    # list of the past_links
    list_of_links = []

    beginning_time = time.time()

    while time.time() - beginning_time < time_to_search:
        print('Execution time = ' + str(round(time.time() - beginning_time)) , 'sec')
        s = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=0.1,
            status_forcelist=[ 500, 502, 503, 504 ],
            raise_on_status=True
        )
        s.mount('http://', HTTPAdapter(max_retries=retries))

        try:

            next_link =  '{}&Start={}'.format(base_link, new_items+1)
            response = s.get(next_link)

        except requests.ConnectionError as e:

            util.log('local_argentina','ConnectionError = {} trying to connect\
                to {}'.format(e, next_link)
            )
            return False
        
        except requests.Exception as e:

            util.log('local_argentina',
                'Exception other than ConnectionError ocurred = {}\
                trying to connect to {}'.format(e, next_link)
            )
            return False
        else:
            # check if a 200 response is received to follow
            if response.status_code != 200:
                util.log('local_argentina',
                    'Received a status code {} trying to connect \
                    to {}'.format(str(response.status_code), next_link)
                )
                return False
        # moving on with a 200 status code response. It still could be
        # a page not found message.

        # parse the url
        print('Connected to {}'.format(next_link))
        soup = BeautifulSoup(response.text, 'html5lib')

        # check if the url has a table with links
        if not soup.find('table'):
            util.log('local_argentina',
                'Not logged on the right url = {}'.format(
                    next_link)
            )
            return False
        table_with_links = soup.find('table')

        # check if the table has the links
        if not table_with_links.findAll('a'): 
            util.log('local_argentina',
                'Not logged on the right url = {}'.format(
                    next_link)
            )
            return False
        list_of_links.extend(table_with_links.findAll('a'))
         
        new_items = len(list_of_links)

        # if the number of links is less than 30 the pagination reach 
        # the last page
        if new_items - previous_items < 30:
            last_iteration = True
        else:
            last_iteration = False
            previous_items = new_items

        # iteration over links to the tariff tables per period
        for tl in table_with_links.findAll('a'):
            tariff_link = tl.get('href')[5:]
            tariff_link = '{}{}'.format(enre_web, tariff_link)
            # if the link was already processed skip
            if tariff_link in past_links:
                continue
            match_date = re.search('[0-9]{2}-[0-9]{4}', tl.text)
            if match_date:
                print(
                    'Scraping Tariff of period {}'.format(
                        match_date.group())
                )
                str_date = match_date.group()
                date = time.strptime(str_date, '%m-%Y')
                if date <= date_lower_limit:
                    
                    util.log('local_argentina',
                        'Ended with the scraping of argentina\
                        tariff tables'
                    )
                    print(
                        'Ended with the scraping of argentina\
                        tariff tables'
                    )
                    return True
                # check the case
                elif date == time.strptime('201602','%Y%m'):
                    if len(re.findall('\(.*?\)', tl.text)) != 0 :
                        continue
            # if a date was not identified skip the link
            else:
                util.log('local_argentina',
                    'did not found a date format on the \
                    link = {}'.format(tl)
                )
                print(
                    'did not found a date format on the \
                    link = {}'.format(tl)
                )
                continue

            # try to connect to the tariff table of each period
            try:

                response = s.get(tariff_link)

            except requests.ConnectionError as e:

                util.log('local_argentina', 'ConnectionError = {}, trying to connect\
                    to {}'.format(e, tariff_link)
                )
                continue
               
            except requests.Exception as e:

                util.log('local_argentina',
                    'Exception other than ConnectionError\
                    ocurred = {} trying to connect to {}'.format(
                        e, tariff_link)
                )
                continue
            else:

                # check if a 200 response is received to follow
                if response.status_code != 200:
                    util.log('local_argentina',
                        'Received a status code {} trying to connect \
                        to {}'.format(
                            str(response.status_code), tariff_link)
                    )
                    continue
            soup = BeautifulSoup(response.text, 'html5lib')
            
            # Parse the tariff table to a pandas dataframe 
            tariff_table_parser = TariffParserArgentina(
                    soup, 
                    date,
                    tariff_link)

            try: 
                df_tariff = tariff_table_parser.to_dataframe()
            except Exception as e:
                util.log('local_argentina',
                    'fail to parse tariff table from link {},\
                    with error {}'.format(tariff_link, e)
                )
                print(
                    'fail to parse tariff table from link {},\
                    with error {}'.format(tariff_link, e)
                )
                continue
            else:
                if tariff_table_parser.check_social_tariff_page():
                    type_of_tariff = 'social_tariff'
                else:
                    type_of_tariff = 'regular_tariff'

            # save the file in s3
            filename = 'argentina_tariff_ENRE_{}_{}.csv'.format(
                 time.strftime('%Y%m',date), type_of_tariff )
            util.dump_files_s3_v2(
                df_tariff.to_csv(), filename, 'local_argentina' )

            data.append([
                time.strftime('%Y%m',date), tariff_link]) 
            print('saved the file {} from period {}'.format(filename, match_date.group()))

            # save the new processed link in data
            util.dump_files_s3(data, output_path, 'local_argentina')

        # after exiting the loop check if it was last iteration and 
        # return
        if last_iteration:

            util.log('local_argentina',
                'Ended with the scraping of argentina tariff tables'
            )
            return True

