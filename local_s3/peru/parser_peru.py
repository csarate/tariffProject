import numpy as np
import pandas as pd

from bs4 import BeautifulSoup

from ..utils_test import util

def create_dics(df, tag):
    '''
    df = dataframe
    tag = string, to select the dictionary
    '''
    # create dictionaries with block_tariff number as key 
    # and tariff as value
    if tag == 'tariff_code':
        dic_tariff = df.groupby(
            ['nblock_tariff'])[0].agg(
                lambda x : '_'.join([y for y in x if y]) 
            ).to_dict()
        dic_tariff.setdefault(0, '')
        return dic_tariff
    elif tag == 'tariff_description':
        # create dictionaries with block_tariff number as key 
        # and description as value
        df_description = df.groupby(
            ['nblock_tariff', 'is_descrip'])[1].agg(
                lambda x : ' '.join([ y for y in x if y])
            )
        dic_descrip = df_description.loc[:, True].to_dict()
        dic_descrip.setdefault(0, '')
        return dic_descrip
    elif tag == 'user_category': 
        # create dictionaries with block_ab as key and type 
        # of user as value
        dic_ab = df.loc[
            df['block_ab'] == True
        ][[1, 'nblock_ab']].set_index('nblock_ab')[1].to_dict()
        dic_ab.setdefault(0, '')
        return dic_ab
    elif tag == 'power_trench': 
        # create dictionaries with block_power as key and 
        # power trench as value
        dic_power = df.loc[
            df['block_power'] == True
        ][[1, 'nblock_power']].set_index('nblock_power')[1].to_dict()
        dic_power.setdefault(0,'')
        return dic_power
    elif tag == 'component_prefix':
        # create dic with prefix group number value (prefix string)
        dic_prefix = df.loc[
            df['prefix'] == True
        ][['nblock_prefix', 1]].set_index('nblock_prefix')[1].to_dict()
        dic_prefix.setdefault(0,'')
        return dic_prefix
    

def number_tariff_blocks(blocks_tuples):

    counter_t, counter_ab, counter_p = 0, 0, 0
    l_blocks = [] 

    for t, ab, p, u in blocks_tuples:
        if t:
            nline = [counter_t + 1, 0, 0]
            counter_t += 1
        elif ab:
            nline = [counter_t, counter_ab+1, 0]
            counter_ab += 1
        elif p:
            nline = [counter_t, counter_ab, counter_p+1]
            counter_p += 1
        elif u:
            nline = [counter_t, 0, 0]
        else:
            nline = last_line
        l_blocks.append(nline)
        last_line = nline

    return l_blocks

def tariff_cols(r, dic_tariff, dic_ab, dic_p, dic_descrip):
    key_tariff = r['nblock_tariff']
    key_ab = r['nblock_ab']
    key_p = r['nblock_power']
    try:
        v_tariff = dic_tariff[key_tariff]
    except KeyError:
        util.log(
            'local_peru',
            'key: {} not in tariff_dictionary'.format(
            key_tariff))
        print('key: {} not in tariff dictionary'.format(
            key_tariff))
        raise KeyError
    try:
        v_ab = dic_ab[key_ab]
    except KeyError:
        print('key: {} not in user_category dictionary'.format(
            key_ab))
        util.log(
            'local_peru',
            'key: {} not in user_category_dictionary'.format(
            key_tariff))
        raise KeyError
    try:
        v_p = dic_p[key_p]
    except KeyError:
        print('key: {} not in power trench dictionary'.format(
            key_p))
        util.log(
            'local_peru',
            'key: {} not in power_trench dictionary'.format(
            key_p))
        raise KeyError
    tariff = ' '.join([v_tariff, v_ab, v_p]).strip()
    r['Tariff_code'] = tariff
    try:
        v_descrip = dic_descrip[key_tariff]
    except KeyError:
        print('key: {} not in description dictionary'.format(
            key_tariff))
        util.log(
            'local_peru',
            'key: {} not in description dictionary'.format(
            key_tariff))
        raise KeyError
    r['Description_tariff'] = v_descrip
    return r

def number_prefix_blocks(block_prefix_tuples):
    counter_prefix = 0
    l_blocks_prefix =[]
    last_line = []
    for p, u in block_prefix_tuples:
        if p:
            nline = [counter_prefix + 1]
            counter_prefix += 1
        elif u:
            nline = [0]
        else:
            nline = last_line
        l_blocks_prefix.extend(nline)
        last_line = nline
    return l_blocks_prefix

def prefix_component(r, dic_prefix):
    key_prefix = r['nblock_prefix']
    try:
        v_prefix = dic_prefix[key_prefix]
    except KeyError:
        print('key: {} not in prefix dictionary'.format(
            key_prefix))
        util.log(
            'local_peru',
            'key: {} not in prefix dictionary'.format(
            key_prefix))
        raise KeyError
    v_component = r[1]
    component = ' '.join([v_prefix, v_component]).strip()
    r['Tariff_component'] = component
    return r

def parse_dataframe(df_tariff, unindented, nr_columns):
    # df_tariff is the dataframe
    df_tariff['unindented'] = unindented

    df_tariff.fillna('', inplace=True)

    df_tariff = df_tariff.loc[~
        (df_tariff[1].str.contains('TENSIÓN')
        ) & ~(
            df_tariff[3].str.contains('Sin IGV')
        )].copy()
    
    # mark the start of a tariff code, a users group or power trench
    # as True False
    df_tariff['block_tariff'] = df_tariff[0].astype(str).apply(
        lambda x : 'TARIFA' in x)
    df_tariff['block_ab'] = df_tariff[1].astype(str).apply(
        lambda x : ') ' in x)
    df_tariff['block_power'] = df_tariff[1].astype(str).str.contains(
        '^[0-9]')
    
    # mark lines that contain tariff description as True False
    # criteria is that more than half letters are uppercase
    df_tariff['is_descrip'] = df_tariff[1].astype(str).apply(
        lambda x : (sum([ y.isupper() for y in x]) > 0.5*len(x))
    )

    # save the block markers as tuples 
    blocks_tuples = df_tariff[
        ['block_tariff', 'block_ab', 'block_power', 'unindented']
    ].apply(tuple, axis=1)

    # list of lists with blocks number 
    l_blocks = number_tariff_blocks(blocks_tuples)
    number_blocks_df = pd.DataFrame(
        data=np.array(l_blocks), 
        columns=['nblock_tariff', 'nblock_ab', 'nblock_power']
    )
    df_tariff_blocks = pd.concat(
        [
            df_tariff.reset_index(drop=True), 
            number_blocks_df.reset_index(drop=True)
        ], 
        axis=1
    )

    # create dictionaries with block number as keys 
    dic_tariff = create_dics(
        df_tariff_blocks, 
        tag='tariff_code')
    dic_descrip = create_dics(
        df_tariff_blocks, 
        tag='tariff_description')
    dic_ab = create_dics(
        df_tariff_blocks, 
        tag='user_category')
    dic_power = create_dics(
        df_tariff_blocks, 
        tag='power_trench')

    # create new columns with tariff_code and tariff description
    df_tariff_blocks = df_tariff_blocks.apply(
        tariff_cols, 
        args=(dic_tariff, dic_ab, dic_power, dic_descrip), 
        axis=1
    )

    # remove lines marked as tariff code, descriptions, power
    # trench or users categories
    df_tariff_components = df_tariff_blocks.loc[
        ~df_tariff_blocks[
            ['block_tariff', 'block_ab', 'block_power', 'is_descrip']
        ].any(axis=1)
    ].copy()

    # mark lines as a prefix of a tariff component value if 
    # line begins with 'Cargo' and has no charge of unit value
    df_tariff_components['prefix'] = (
        df_tariff_components[1].str.contains('^Cargo')
    ) & (df_tariff_components[2] == '')

    # list of tuples with information about if the line is a 
    # prefix for the component value and if it is indented
    block_prefix_tuples = df_tariff_components[
        ['prefix', 'unindented']
    ].apply(tuple, axis=1)

    # list with numbered prefix groups
    l_blocks_prefix = number_prefix_blocks(block_prefix_tuples)
    df_tariff_components['nblock_prefix'] = l_blocks_prefix

    dic_prefix = create_dics(
        df_tariff_components, 
        tag='component_prefix'
    ) 

    # write the full tariff component adding the prefix
    
    df_tariff_components = df_tariff_components.apply(
        prefix_component,
        args=(dic_prefix,), 
        axis=1
    )

    # delete the lines marked as prefix from dataframe 
    df_tariff_table = df_tariff_components.loc[
        ~df_tariff_components['prefix']]

    # rename columns
    if nr_columns == 4:
        df_tariff_table = df_tariff_table.rename(
            columns={
                2 : 'Unit',
                3 : 'Charge'}
        )
        nrows = df_tariff_table.shape[0]
        df_tariff_table['Charge_MCTER'] = [''] * nrows 

    elif nr_columns == 5:
        df_tariff_table = df_tariff_table.rename(
            columns={
                2 : 'Unit',
                3 : 'Charge',
                4 : 'Charge_MCTER'}
        )
    else:
        print('nr of columns not 4 not 5')
        util.log(
            'local_peru',
            'nr of columns are not 4 not 5')
        raise Exception
    # select and reorder a group of columns
    df_tariff_table = df_tariff_table[
        [
            'Tariff_code', 
            'Description_tariff', 
            'Tariff_component',
            'Unit', 
            'Charge',
            'Charge_MCTER'
        ]
    ]
    
    return df_tariff_table

def format_tariff_table_peru(
        soup, df_tariff, utility, document_link, 
        date_start, date_end, sector, interconnection
):
    nr_columns = df_tariff.shape[1] 

    table = soup.find('table')
    # save True or False if second column text is indented
    # indentetion is not save in dataframe
    unindented = [ not(ltrs.findAll('td')[1].text.startswith('  '))  
        for ltrs in table.findAll('tr')]

    df_tariff_table = parse_dataframe(df_tariff, unindented, nr_columns)
       
    # fill columns Country, Utility, Document_link, Date_start, 
    # Date_end, $local, 
    nrows = df_tariff_table.shape[0]
    df_tariff_table['Country'] = ['Peru'] * nrows
    df_tariff_table['Document_link'] = [document_link] * nrows
    df_tariff_table['Utility'] = [utility] * nrows
    df_tariff_table['Date_start'] = [date_start] * nrows
    df_tariff_table['Date_end'] = [date_end] * nrows
    df_tariff_table['$local'] = ['S/.'] * nrows
    df_tariff_table['Sector'] = [sector] * nrows
    df_tariff_table['Interconnection'] = [interconnection] * nrows

    return df_tariff_table
    
