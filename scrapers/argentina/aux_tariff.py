import time
import calendar
import datetime

import pandas as pd

date_format = '%Y-%m-%d'

def concat_cols( t, *args, first_row=0, col_names=''):
        
    t_new = pd.concat(
        [ t.iloc[first_row:, rng[0]:rng[1]] for rng in args ], 
        axis=1 
    )
    if col_names:
        t_new.columns = col_names
    else:
        t_new.columns = [ 'Unnamed_'+str(x) for x in range(
            t_new.shape[1])
        ]
    return t_new


def numeric_(c):
    if (c.dtype == object):
        return pd.to_numeric(c.str.replace(',','.'), errors='ignore')
    else:
        return pd.to_numeric(c, errors='ignore')

def del_trailing_point(s):
    return s.rstrip('.')

class TariffParserArgentina:
    '''
    class 
    '''
    def __init__(self, soup, date, document_link):
        '''
        '''
        self.date = date
        self.soup = soup
        self.document_link = document_link
        self.currency = 'ARS'
        self.country = 'Argentina'
        self.group = self._date_range()


    def _date_range(self):
        '''
        '''
        now = datetime.datetime.now()
        now = datetime.date.timetuple(now)
        range_1 = (time.strptime('12-2017', '%m-%Y'),
                now)
        range_2 = (time.strptime('02-2017', '%m-%Y'), 
            time.strptime('11-2017', '%m-%Y'))
        range_3a = (time.strptime('03-2016', '%m-%Y'), 
            time.strptime('01-2017', '%m-%Y'))
        range_3b = (time.strptime('01-2012', '%m-%Y'), 
            time.strptime('01-2016', '%m-%Y'))
        if (self.date >= range_1[0]) and (
                self.date <= range_1[1]):
            return 1
        if (self.date >= range_2[0]) and (
                self.date <= range_2[1]):
            return 2
        if (self.date >= range_3a[0]) and (
                self.date <= range_3a[1]):
            return 3
        if (self.date >= range_3b[0]) and (
                self.date <= range_3b[1]):
            return 3
        if (self.date == time.strptime('02-2016', '%m-%Y')):
            return 1


    def _tables_from_html(self):

        container = self.soup.find(
            'div', 
            {'class':'container'}
        )
        html_section = ''

        if self.check_social_tariff_page():
            for child in container.children:
                html_section+=str(child)
        elif (self.group == 1) or (self.group == 2): 
            for child in container.children:
                if 'PEAJE' in child.text:
                    break
                else:
                    html_section+=str(child)
        elif self.group == 3:
            for child in container.children:
                if 'TSP' in child.text:
                    break
                else:
                    html_section+=str(child)
        # qué hacer en este caso
        elif self.group == 0:
            pass

        tables = pd.read_html(
            html_section, 
            header=0, 
            encoding='utf-8',
        )

        return tables


    def check_social_tariff_page(self):
        '''
        '''
        if self.group == 2:
            try:
                if 'PEAJE' in self.soup.find('h5').text:
                    return False
            except AttributeError:
                    return True
        else:
            return False


    def _transform_table(self, t):
        '''
        '''
        utilities = t.columns[2:].to_list()
        tariff_col = [ 
            t.columns.values[0] for x in range(t.shape[0])]
        t.columns = ['Component', 'Unit'] + utilities 
        t['Tariff_code'] = tariff_col
        currency_col = [ 
            self.currency for x in range(t.shape[0])]
        t['$local'] = currency_col
        doclink_col = [ 
            self.document_link for x in range(t.shape[0])]
        t['Document_link'] = doclink_col
        country_col = [ 
            self.country for x in range(t.shape[0])]
        t['Country'] = country_col
        date_start = time.strftime(date_format, self.date) 
        t['Date_start'] = [ 
            date_start for x in range(t.shape[0])]
        ndays = calendar.monthrange(
            self.date.tm_year, 
            self.date.tm_mon)[1] 
        date_end = time.strptime(
            '-'.join(
                map(str,(self.date.tm_year,self.date.tm_mon, ndays )
                )
            ), date_format
        )
        date_end = time.strftime(date_format, date_end)
        t['Date_end'] = [ 
            date_end for x in range(t.shape[0])]
        col_utility = [ 
            u for u in utilities for x in range(t.shape[0])]
        col_charge = pd.concat([t[u] for u in utilities ], axis=0)
        t_bloque = t[['Component', 'Unit', 'Tariff_code', '$local', 
            'Document_link', 'Country', 'Date_start', 'Date_end']]
        t = pd.concat([t_bloque for x in utilities], axis=0)
        t['Utility'] = col_utility
        t['Charge'] = col_charge
        t['Charge'] = numeric_(t['Charge'])
        t['Unit'] = t['Unit'].apply(del_trailing_point) 
        t = t[['Country', 'Utility', 'Document_link', 'Date_start', 
            'Date_end', '$local', 'Component', 'Unit', 'Charge', 
            'Tariff_code']]
        return t 


    # range_2 tarifa social

    def _transform_social_table(self, t):
        t_1 = concat_cols(
            t, [0,2], [2,4], 
            first_row=1, 
            col_names=['Component', 'Unit', 'EDENOR', 'EDESUR']
        )
        t_2 = concat_cols(
            t, [0,2], [4,6], 
            first_row=1, 
            col_names=['Component', 'Unit', 'EDENOR', 'EDESUR']
        )
        t_3 = concat_cols(
            t, [0,2], [6,8], 
            first_row=1, 
            col_names=['Component', 'Unit', 'EDENOR', 'EDESUR']
        )
        transp_table = pd.concat([t_1, t_2, t_3], axis=0) 
        utilities = transp_table.columns[2:].to_list()
        tariff_col = [ 
            t.iat[0,0] for x in range(transp_table.shape[0])]
        transp_table['Tariff_code_0'] = tariff_col
        tariff_step = t.columns.values[2:]
        tariff_step = list(map(lambda x:x.split('.1')[0], tariff_step))
        transp_table['Tariff_step'] = tariff_step
        transp_table['Tariff_code'] = \
            transp_table['Tariff_code_0'].astype(str)\
            + ' ' \
            + transp_table['Tariff_step'].astype(str)
        currency_col = [ 
            self.currency for x in range(transp_table.shape[0])]
        transp_table['$local'] = currency_col
        doclink_col = [ 
            self.document_link for x in range(transp_table.shape[0])]
        transp_table['Document_link'] = doclink_col
        country_col = [ 
            self.country for x in range(transp_table.shape[0])]
        transp_table['Country'] = country_col
        date_start = time.strftime(date_format, self.date) 
        transp_table['Date_start'] = [ 
            date_start for x in range(transp_table.shape[0])]
        ndays = calendar.monthrange(
            self.date.tm_year, 
            self.date.tm_mon)[1] 
        date_end = time.strptime(
            '-'.join(
                map(str,(self.date.tm_year,self.date.tm_mon, ndays, )
                )
            ), date_format
        )
        date_end = time.strftime(date_format, date_end)
        transp_table['Date_end'] = [ 
            date_end for x in range(transp_table.shape[0])]

        t_bloque = transp_table[['Country', 'Document_link', 
            'Date_start', 'Date_end', '$local', 'Component', 
            'Unit', 'Tariff_code']]

        col_charge = pd.concat(
            [transp_table[u] for u in utilities ], axis=0
        )
        col_utility = [ 
            u for u in utilities for x in range(transp_table.shape[0])]
        t = pd.concat([t_bloque for x in utilities], axis=0)

        t['Charge'] = col_charge
        t['Utility'] = col_utility
        t['Charge'] = numeric_(t['Charge'])
        t['Unit'] = t['Unit'].apply(del_trailing_point) 
        t = t[['Country', 'Utility', 'Document_link', 'Date_start', 
            'Date_end', '$local', 'Component', 'Unit', 'Charge',
            'Tariff_code']]

        return t

    def _format_tables(self, tables):
        '''
        '''
        # range_1 y rango_3
        if (self.group == 1) or (self.group == 3):
            tariff_sheet = pd.concat(
                [ self._transform_table(t) for t in tables ], 
                axis = 0
            )
            return tariff_sheet

        elif (
                ( self.group == 2 
                    ) and ( self.check_social_tariff_page())
        ):
            tariff_sheet = pd.concat(
                [ self._transform_social_table(t) for t in tables ], 
                axis = 0
            )
            return tariff_sheet
        elif (
                (self.group == 2
                    ) and (
                    not self.check_social_tariff_page())
        ):
            tariff_sheet_residential = pd.concat(
                [ self._transform_social_table(t) for t in tables 
                if t.shape[1]>4 ] , 
                axis=0
            )
            tariff_sheet = pd.concat(
                [self._transform_table(t) for t in tables 
                if t.shape[1]==4], 
                axis = 0
            )
            return pd.concat(
                [tariff_sheet_residential, 
                tariff_sheet], 
                axis=0)
        elif (
                (self.group == 0)
        ):
            return

    def to_dataframe(self):
        '''
        
        '''
        tables = self._tables_from_html()
        df_tariff = self._format_tables(tables)
        return df_tariff
