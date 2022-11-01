import datetime
import os
import json
import requests
import shutil
import sys

import numpy as np
import pandas as pd

from bs4 import BeautifulSoup
from tqdm.auto import tqdm
import glob
import pandas as pd
import scrapers.utils_test.util_s3 as util_s3
import io


#s3_dir = '/home/carla/CALDEN/BID_ws/2022_IDB_Tariff/04-Scrapers_tarifas/local_s3'
s3_dir = 'D:/GIT/02 Proyectos/2022_IDB_Tariff/04-Scrapers_tarifas/local_s3'

# Parameter received from Lambda function
event: dict = {}

# Log in memory to speed up operations
str_full_log_text: str = ""

# local dir configuracion (Testing Purpouse)
s3_dir = os.getcwd()
setup_dic = {
    'local_argentina': os.path.join(s3_dir, 'local_s3', 'argentina'),
    'local_brasil': os.path.join(s3_dir, 'local_s3', 'brasil'),
    'local_peru': os.path.join(s3_dir, 'local_s3', 'peru'),
    'local_el_salvador': os.path.join(s3_dir, 'local_s3', 'el_salvador'),
    'local_mexico': os.path.join(os.getcwd(), 'local_s3','mexico'),
    'local_nicaragua': os.path.join(os.getcwd(), 'local_s3','nicaragua'),
    'local_panama': os.path.join(os.getcwd(), 'local_s3','panama') }


# ############################## #
# Set event value for the module #
# ############################## #
def set_event(exec_event: dict):
    global event
    event = exec_event
    util_s3.set_event(event)


# ######################################### #
# Create local folder for testing purpoused #
# ######################################### #
def create_s3dirs():
    if event["ExecutionMode"] == "Local":
        for c in setup_dic:
            if not os.path.exists(setup_dic[c]):
                os.mkdir(setup_dic[c])


# ################################# #
# Reset log file before adding logs #
# ################################# #
def start_log(local):
    global str_full_log_text

    str_full_log_text = ""

    if event["ExecutionMode"] == "Local":
        log_file = os.path.join(setup_dic[local], 'logfile.log')
        # Remove file if exists
        if os.path.isfile(log_file):
            try:
                os.remove(log_file)
            except Exception as e:
                raise e
    else:
        # S3 file
        log_file: str = f"{local}/logfile.log"
        if util_s3.file_exists(log_file):
            # Delete file
            util_s3.delete_file(log_file)


# ################# #
# Add log to memory #
# ################# #
def log(local, args, end='\n'):
    global str_full_log_text

    dt = datetime.datetime.now().strftime('[%Y%m%d_%H%M%S] ->')
    text = '{}'.format(dt), ''.join([str(i) for i in args]), end
    str_full_log_text += ' '.join(text)

    print(text)


# ############# #
# Save log file #
# ############# #
def save_log(local):
    global str_full_log_text

    if event["ExecutionMode"] == "Local":
        log_file = os.path.join(setup_dic[local], 'logfile.log')
        # Save Log in local file
        with open(log_file, 'a') as f:
            f.write(str_full_log_text)
    else:
        # S3 file
        log_file: str = f"{local}/logfile.log"

        # Save S3 log file
        util_s3.save_file(log_file, str_full_log_text)


# ################# #
# Read json from S3 #
# ################# #
def read_files_s3(filename, local):
    str_content: str = ""

    if event["ExecutionMode"] == "Local":
        try:
            f = open(os.path.join(setup_dic[local], filename), 'r')
        except FileNotFoundError:
            f = open(os.path.join(setup_dic[local], filename), 'w')
            f.close()

        with open(os.path.join(setup_dic[local], filename), 'r') as f:
            str_content = f.read()
    else:
        # Read file from S3 bucket
        s3_filename: str = f"{local}/{filename}"
        if not util_s3.file_exists(s3_filename):
            util_s3.save_file(s3_filename, "")
        else:
            str_content = util_s3.load_file(s3_filename)

    # Parse the value of the file
    try:
        json.loads(str_content)
    except ValueError:
        return str_content
    else:
        return json.loads(str_content)


# ################################# #
# dump list in S3 given a list data #
# and the filepath in S3 Bucket     #
# ################################# #
def dump_files_s3(data_list, filename, local):
    if event["ExecutionMode"] == "Local":
        with open(os.path.join(setup_dic[local], filename), 'w') as f:
            f.write(json.dumps(data_list))
    else:
        s3_filename: str = f"{local}/{filename}"
        util_s3.save_file(s3_filename, json.dumps(data_list))


# ############################ #
# Dump bytes information in S3 #
# ############################ #
def dump_files_s3_v2(data, filename, local):
    if event["ExecutionMode"] == "Local":
        with open(os.path.join(setup_dic[local], filename), 'w') as f:
            if sys.platform == 'win32':
                f.write(data.replace("\r\n", "\r"))
            else:
                f.write(data)
    else:
        s3_filename: str = f"{local}/{filename}"
        util_s3.save_file(s3_filename, data)


# ############################ #
# Read dataframe from CSV File #
# ############################ #
def get_df_from_csv(str_csv_file: str, **kwargs) -> pd.DataFrame:
    df_csv: pd.DataFrame
    if event["ExecutionMode"] == "Local":
        df_csv = pd.read_csv(str_csv_file, **kwargs)
    else:
        str_string_csv: str = util_s3.load_file(str_csv_file)
        csv_string_io = io.StringIO(str_string_csv)
        df_csv = pd.read_csv(csv_string_io, **kwargs)
    return df_csv


# Save files from de web in S3
def save_URL_files_s3(link, local, prefix):
    filename = link.split('/')[-1]
    # make an HTTP request within a context manager
    with requests.get(link, stream=True) as r:
        # check header to get content length, in bytes
        total_length = int(r.headers.get("Content-Length"))

        # implement progress bar via tqdm
        with tqdm.wrapattr(r.raw, "read", total=total_length, desc=filename) as raw:
            filename_full = prefix + filename
            # save the output to a file
            with open(os.path.join(setup_dic[local], filename_full), 'wb') as output:
                shutil.copyfileobj(raw, output)

def concat_cols( t, *args, first_row=0, col_names=''):
        
    t_new = pd.concat(
        [ t.iloc[first_row:, rng[0]:rng[1]] for rng in args ], 
        axis=1 
    )
    if isinstance(col_names, np.ndarray) or isinstance(
        col_names, list) and (len(col_names) > 0):
        t_new.columns = col_names
    else:
        t_new.columns = [ 'Unnamed_'+str(x) for x in range(
            t_new.shape[1])
        ]
    return t_new

def connect_to_link(s, link, local):

    try:
        response = s.get(link)

    except requests.ConnectionError as e:

        log(
            local,
            'ConnectionError = {} trying to connect\
            to {}'.format(e, link)
        )
        return False
    
    except requests.Exception as e:

        log(
            local,
            'Exception other than ConnectionError ocurred = {}\
            trying to connect to {}'.format(e, link)
        )
        return False
    else:
        # check if a 200 response is received to follow
        if response.status_code != 200:
            log(
                local,
                'Received a status code {} trying to connect \
                to {}'.format(
                    str(response.status_code), 
                    link)
            )
            return False
    print('Connected to {}'.format(link))
    log(
        local, 
        'Connected to {}'.format(link))

    return response 

def post_to_link(s, link, json, local):
    
    posting_to = ', '.join(
        [ str(k)+':'+str(v) for k, v in json.items()])
    try:
        response = s.post(link, data=json)

    except requests.ConnectionError as e:

        log(
            local,
            'ConnectionError = {} trying to connect\
            to link: {}, posting to: {}'.format(
                e, 
                link,
                posting_to)
        )
        return False
    
    except requests.Exception as e:

        log(
            local,
            'Exception other than ConnectionError ocurred = {}\
            trying to connect to link: {}, posting to: {}'.format(
                e,
                link,
                posting_to)
        )
        return False
    else:
        # check if a 200 response is received to follow
        if response.status_code != 200:
            log(
                local,
                'Received a status code {} trying to connect \
                to link: {}, posting to: {}'.format(
                    str(response.status_code), 
                    link,
                    posting_to)
            )
            return False
    # moving on with a 200 status code response. It still could be
    # a page not found message.
    #print('Connected to link: {}, posting to: {}'.format(
    #    link,
    #    posting_to)
    #)
    log(local, 'Connected to link: {}, posting to: {}'.format(
        link,
        posting_to)
    )

    return response 

# ########################## #
# List all files in a folder #
# ########################## #
def list_files(filetype, local) -> list:
    lst_files_in_folder: list = []

    if event["ExecutionMode"] != "Local":
        lst_files_in_folder = util_s3.files_in_folder(local, filetype.replace("*", ""))
    else:
        pth = setup_dic[local] + '/**/*' + filetype
        lst_files_in_folder = list(glob.iglob(pth, recursive=True))

    return lst_files_in_folder


# split a list in chunks
def split_list(list_a, chunk_size):
    for i in range(0, len(list_a), chunk_size):
        yield list_a[i:i + chunk_size]


# Create a dataframe from multiple files
def DF_from_files(filetype, local):
    arq = list_files(filetype, local)
    DS = pd.DataFrame()
    for f in arq:
        nombre = os.path.basename(f)
        lote = f.split("\\")[1]
        df = pd.read_csv(f, sep=',', decimal=".", encoding='cp1252', parse_dates=['Date_start', 'Date_end'],
                         dayfirst=False)
        df['Document_name'] = nombre
        DS = DS.append(df, ignore_index=True)
    return DS


def tariff_columns_dict():
    dict = {'COUNTRY': 'Country',
            'UTILITY': 'Utility',
            'Tariff_Name_in_document': 'Tariff Name in the document',
            'Document_name': 'Document Name',
            'Document_link': 'Document Link',
            'Add_links': 'Additional links',
            'Taxes_link': 'Tax Links',
            'Date_start': 'Date start',
            'Date_end': 'Date end',
            'Billing_frequency': 'Billing Frequency',
            'Voltage_group': 'Tariff Voltage Group (Options: Low Voltage, Medium Voltage, High Voltage)',
            'Voltage_trench_start': 'Tariff Voltage Trench start',
            'Voltage_trench_end': 'Tariff Voltage Trench end',
            'Sector': 'Tariff Sector (Options: Residential, Commercial, Industrial, etc…)',
            'Special_category': 'Tariff Special Category (Sector Subgroup , normally in the Tariff Name)',
            'Special_category_detail': 'Tariff Special Category Detail',
            'Region': 'Tariff Region',
            'Power_trench_start': 'Tariff Power Trench start',
            'Power_trench_end': 'Tariff Power Trench end',
            'Volumetric_trench_start': 'Tariff Volumetric Trench start',
            'Volumetric_trench_end': 'Tariff Volumetric Trench end',
            'Min_consumption_vol': 'Value of minimum consumption requirement (kWh)',
            'Structure_subtype': 'Tariff Structure Subtype (options: Flat rate, Linear Increasing Rate, '
                                 'Block Increasing Rate) (only volumetric)',
            'Differentiation_type': 'Tariff Differentiation Type (Option: Two Time Slots, Three Time Slots, …)',
            'Daytime_group': 'Daytime Group (Options: Peak, OffPeak, Intermediate, etc)',
            'Daytime_group_time_differentiation_start': 'Daytime Group Time Differentiation start',
            'Daytime_group_time_differentiation_end': 'Daytime Group Time Differentiation end',
            'Weektime_differentiation_group': 'Weektime Differentiation Group (Options: weekdays, weekends, …)',
            'Seasontime_differentiation_group': 'Seasontime Differentiation group (Options: summer, winter, spring, '
                                                'autumn, etc)',
            'Temperature_differentiation_start': 'Temperature Differentiation start',
            'Temperature_differentiation_end': 'Temperature Differentiation end',
            'Contracted_Power_Rate': 'Contracted Power Rate ($ local/kW)',
            'Realized_power_rate': 'Realized Power Rate ($ local/kW)',
            'Volumetric_total': 'Volumetric Tariff ($ local/ kWh)',
            'Volumetric_energy_component': 'Volumetric Tariff ($ local/ kWh) - Energy Purchase',
            'Volumetric_distribution_component': 'Volumetric Tariff ($ local/ kWh) - Distribution Component',
            'Charges_component_fixed': 'Charges component fixed grid ($ local/client)',
            'Charges_component_commercialization': 'Charges component commercialization ($ local/client)',
            'Taxes_component_percentual': 'Taxes component percentual',
            'Subsidies_component_percentual': 'Subsidies components percentual',
            'Street_lighting_component_fixed': 'Street Lighting components fixed',
            'Street_lighting_component_variable': 'Street Lighting components variable',
            'CURRENCY': '$ local',
            'Observation': 'Observation',
            'Tax_included': 'Tax included (Yes: 1; No: 0)',
            'Existence_fixed_charge': 'Existence of Fixed Charge',
            'Existence_power_rate': 'Existence of Power rate ($ local/kW)',
            'Existence_volumetric_rate_w_minimum': 'Existence of Volumetric Rate with minimum consumption requirement',
            'Generation_component_variable': 'Generation component variable',
            'Generation_component_fixed': 'Generation component fixed',
            'Transmission_component_variable': 'Transmission component variable',
            'Transmission_component_fixed': 'Transmission component fixed',
            'Distribution_component_variable': 'Distribution component variable',
            'Distribution_component_fixed': 'Distribution component fixed',
            'Commercialization_component_variable': 'Commercialization component variable',
            'Commercialization_component_fixed': 'Commercialization component fixed',
            'Losses_component_fixed': 'Losses component fixed',
            'Losses_component_variable': 'Losses component variable',
            'Restrictions_component_variable': 'Restrictions component variable',
            'Restrictions_component_fixed': 'Restrictions component fixed'}
    return dict


# TODO: Ver como hacer con esta funcion en S3
def tariff_to_xlsx(name, df, local):
    col_dict = tariff_columns_dict()
    AllCol = list(col_dict.keys())

    # Create a Pandas Excel writer using XlsxWriter as the engine.
    if event["ExecutionMode"] != "Local":
        path = f"{local}/{name}.xlsx"
        excel_file_binary_output = io.BytesIO()
        writer = pd.ExcelWriter(excel_file_binary_output, engine='xlsxwriter', datetime_format='dd/mm/yyyy',
                                date_format='dd/mm/yyyy')
    else:
        path = os.path.join(setup_dic[local], name + '.xlsx')
        writer = pd.ExcelWriter(path, engine='xlsxwriter', datetime_format='dd/mm/yyyy', date_format='dd/mm/yyyy')

    wk = writer.book

    # Formats
    header0_format = wk.add_format(
        {'bold': True, 'text_wrap': True, 'font_size': 11, 'fg_color': '#B8CCE4', 'border': 1, 'valign': 'vcenter',
         'align': 'center'})
    col_format = wk.add_format({'font_size': 9, 'valign': 'vcenter', 'align': 'right'})
    col_format_l = wk.add_format({'font_size': 9, 'valign': 'vcenter', 'align': 'left'})

    # Convert the dataframe to an XlsxWriter Excel object. Note that we turn off
    # the default header and skip one row to allow us to insert a user defined
    # header.

    hoja = name.split('_')[2] + "_" + name.split('_')[3]

    df[AllCol].sort_values(by=['Date_start', 'UTILITY', 'Tariff_Name_in_document']).to_excel(writer, sheet_name=hoja,
                                                                                             header=False, index=False,
                                                                                             startrow=2)

    max_col = len(AllCol)
    max_row = len(df)

    wsh = writer.sheets[hoja]

    # Write the column headers with the defined format.
    for col_num, index in enumerate(col_dict):
        wsh.write(0, col_num, col_dict[index], header0_format)
        wsh.write(1, col_num, index, header0_format)

    wsh.set_column(0, 1, 20, col_format_l)
    wsh.set_column(3, max_col - 1, 20, col_format)
    wsh.set_column(2, 2, 50, col_format_l)

    wsh.autofilter(0, 0, max_row, max_col - 1)
    wsh.freeze_panes(1, 3)

    wsh.set_row(0, 90)

    writer.save()
    if event["ExecutionMode"] != "Local":
        excel_binary_data = excel_file_binary_output.getvalue()
        util_s3.save_file(path, excel_binary_data)

    return True


# ------------------------------------------
#  Agregados por CRS desde el 23-09-2022
# ------------------------------------------
# dump list in df given a list data
def MonthDict():
    dict = \
        {
        '01': 'January', 'enero': 'January', 'Enero': 'January', 'ENERO': 'January', 'ene': 'January',
        '02': 'February', 'febrero': 'February', 'FEBRERO': 'February', 'Febrero': 'February', 'feb': 'February',
        '03': 'March', 'marzo': 'March', 'MARZO': 'March', 'Marzo': 'March', 'mar': 'March',
        '04': 'April', 'abril': 'April', 'ABRIL': 'April', 'Abril': 'April', 'abr': 'April',
        '05': 'May', 'mayo': 'May', 'MAYO': 'May', 'Mayo': 'May', 'may': 'May',
        '06': 'June', 'junio': 'June', 'JUNIO': 'June', 'Junio': 'June', 'jun': 'June',
        '07': 'July','julio': 'July', 'JULIO': 'July', 'Julio': 'July', 'jul': 'July',
        '08': 'August', 'agosto': 'August', 'AGOSTO': 'August', 'Agpsto': 'August', 'ago': 'August',
        '09': 'September', 'septiembre': 'September', 'SEPTIEMBRE': 'September', 'Septiembre': 'September', 'sep': 'September',
        '10': 'October', 'octubre': 'October', 'OCTUBRE': 'October', 'Octubre': 'October', 'oct': 'October',
        '11': 'November', 'noviembre': 'November', 'NOVIEMBRE': 'November', 'Noviembre': 'November', 'nov': 'November',
        '12': 'December', 'diciembre': 'December', 'DICIEMBRE': 'December', 'Diciembre': 'December', 'dic': 'December'
        }
    
    return dict


# ------------------------------------------------------------------
# Vefificación y creación de entorno de directorios de trabajo
# ------------------------------------------------------------------
def lectura_links_procesados(output_path, local):
    '''
    time_to_search = float (seconds)
    output_path = check_social_tariff_pageo test the script output_path is the filename with
        the saved links in s3 dir
    '''
    # read already processed and saved links so as not to repeat
    data = read_files_s3(output_path, local)
    if not data:
        data = []
    
    # get the already prcessed links in second position
    past_links = []
    for line in data:
        try:
            past_links.append(line[2])
        except IndexError as e:
            log('badly formed link on saved links = {}'.format(line))
    
    return data, past_links


# ------------------------------------------------------------------
# Vefificación estatus de conexión a links
# ------------------------------------------------------------------
def status_code_url(s, url, local):
    try:
        response = s.get(url, verify=False)
    except requests.ConnectionError as e:
        log(local, 'ConnectionError = {} trying to connect\
            to {}'.format(e, url))
        return False
    except requests.Exception as e:
        log(local, 'Exception other than ConnectionError ocurred = {}\
                 trying to connect to {}'.format(e, url))
        return False
    else:
        # check if a 200 response is received to follow
        if response.status_code != 200:
            log(local, 'Received a status code {} trying to connect \
                     to {}'.format(str(response.status_code), url))
            return False
    
    # moving on with a 200 status code response. It still could be
    # a page not found message.
    print('Connected to {}'.format(url))
    soup = BeautifulSoup(response.text, 'html5lib')
    soup.encode("UTF-8")
    
    return True, soup, response


# ------------------------------------------------------------------
# Descarga y grabación de cada PDF
# ------------------------------------------------------------------
def dump_pdf(url, nombre, local):
    response = requests.get(url)
    if response.status_code != 200:
        log(local, 'Received a status code {} trying to connect \
                     to {}'.format(str(response.status_code), url))
        print('ConnectionError trying to connect to {}'.format(url))
        return False, nombre
    
    # moving on with a 200 status code response. It still could be
    # a page not found message.
    with open(
            os.path.join(setup_dic[local], nombre), 'wb'
            # os.path.join(s3_dir, filename) ,'w'
    ) as f:
        f.write(response.content)
    return True, os.path.join(setup_dic[local], nombre)


# ------------------------------------------------------------------
# Inserción de fila en un dataframa en una posición determinada
# ------------------------------------------------------------------
def inserta_fila_df(row_number, df, row_value):
    start_upper = 0
    end_upper = row_number
    start_lower = row_number
    end_lower = df.shape[0]
    upper_half = [*range(start_upper, end_upper, 1)]
    lower_half = [*range(start_lower, end_lower, 1)]
    lower_half = [x.__add__(1) for x in lower_half]
    index_ = upper_half + lower_half
    df.index = index_
    df.loc[row_number] = row_value
    df = df.sort_index()
    return df


# ------------------------------------
# Desanidación Tablas Anidadas
# ------------------------------------
def desanidar_lista(lista, lista_d):
    for e, elemento in enumerate(lista):
        if isinstance(elemento, list):
            lista_d = desanidar_lista(elemento, lista_d)
        else:
            lista_d.append(elemento.strip())
    return lista_d


