import pandas as pd
import numpy as np

import os
import requests
from time import sleep



def main_query_from_new_ndc11(new_ndc11):
    """
    Main function which will run the entire pipeline from a list of provided NDC11 codes. If this
    is the first time running the code, it will create entirely new files raw files, otherwise it 
    will update the raw files based on ndc11s and resulting rxcuis which are missing in the 
    already existing ndc_properties and rxcui_to_atc files. 

    Parameters
    ----------
    new_ndc11 : list of str
        NDC11 formatted codes (11 digit strings) used to query RxNorm API. Will first checks 
        the gold file for overlapping codes to only query those that are missing. 

    Result
    ----------
    ndc_to_atc.pkl
        Will be written in the data/clean folder which contains one row per unique NDC11 with 
        the rxcui and ATC information merged on. 
    """

    update_RxNorm_ndc_properties(new_ndc11)
    new_rxcuis = get_new_rxcuis()
    update_RxNorm_rxcui_to_atc(new_rxcuis)
    raw_to_clean_atc_levels()
    raw_to_clean_ndc_to_atc()



####################################################################################################
####################################################################################################
def raw_to_clean_ndc_to_atc():
    """
    Combine Raw NDC properties and rxcui -> ATC files into one cleaned file. Also adds in
    columns for the different levels of ATC classifications
    """

    df_ndc = pd.read_pickle(Params().raw_path+'ndc_properties.pkl')
    df_rxcui = pd.read_pickle(Params().raw_path+'rxcui_to_atc.pkl')

    df = df_ndc.merge(df_rxcui, on='rxcui', how='left')
    df = df.rename(columns={'ndcItem': 'drug_ndc'})

    # Add in the levels
    for level in Params().atc_levels:
        dfl = pd.read_pickle(Params().clean_path+f'atc_{level}_level.pkl')
        dfl = dfl.set_index(f'atc_{level}')[f'atc_{level}_desc']
        
        df[f'atc_{level}'] = df['ATC'].str.extract(r'^'+'('+'|'.join(dfl.index)+')')
        df[f'atc_{level}_desc'] = df[f'atc_{level}'].map(dfl)


    df.to_pickle(Params().clean_path+'ndc_to_atc.pkl')


def update_RxNorm_ndc_properties(new_ndc11):
    """
    Wrapper script to call query_RxNorm_ndc_properties, which will update the raw file with any 
    codes that are currently missing.

    Parameters
    ----------
    new_ndc11 : list of str
        NDC11 formatted codes (i.e. 11 digit strings) used to query RxNorm API. Will first checks 
        the gold file for overlapping codes to only query those that are missing. 
    """

    file = 'ndc_properties.pkl'

    # Open original file
    try:
        df = pd.read_pickle(Params().raw_path+file)
    except FileNotFoundError:
        df = pd.DataFrame(columns=['ndcItem'])

    # Additional codes
    to_query = pd.Series(new_ndc11)
    to_query = to_query[~to_query.isin(df['ndcItem'])]

    # Remove any null codes or those that aren't NDC 11. (str methods drop NaN)
    to_query = to_query[to_query.str.len().eq(11) & to_query.str.isnumeric()].to_numpy()

    print(f'Querying an additional {len(to_query):,} NDC codes')
    df_adnl = query_RxNorm_ndc_properties(to_query)
    df = pd.concat([df, df_adnl], ignore_index=True, sort=False)

    df.to_pickle(Params().raw_path+file)


def update_RxNorm_rxcui_to_atc(new_rxcuis):
    """
    Wrapper script to call query_RxNorm_rxcui_codes and query_RxNorm_rxcui_IN_codes to ultimately
    get the ATC codes associated with each RXnorm ID 

    Parameters
    ----------
    new_rxcuis : list of str
        rxcuis returned from RxNorm database. Will first find the rxcui_IN identifier which we 
        ultimatley use to get the ATC codes
    """

    file = 'rxcui_to_atc.pkl'

    # Open original file
    try:
        df = pd.read_pickle(Params().raw_path+file)
    except FileNotFoundError:
        df = pd.DataFrame(columns=['rxcui'])

    # Additional codes
    to_query = pd.Series(new_rxcuis)
    to_query = to_query[~to_query.isin(df['rxcui'])]
    to_query = to_query.dropna().drop_duplicates().to_numpy()

    print(f'Querying an additional {len(to_query):,} RXCUI codes')
    if len(to_query) > 0:
        df_adnl = RxNorm_rxcui_to_atc(to_query)
        df = pd.concat([df, df_adnl], ignore_index=True, sort=False)

        df.to_pickle(Params().raw_path+file)



################################### API Queries ####################################################
####################################################################################################
def RxNorm_rxcui_to_atc(rxcuis):
    """
    This wrapper to deal with mapping of rxcui -> rxcui_IN -> ATC.

    Parameters
    ----------
    rxcuis : list of str
        List of RxNorm rxcui codes.

    Returns
    -------
    pd.DataFrame
        rxcui -> rxcui_IN, ATC + additional properties that may be useful
    """

    df = query_RxNorm_for_rxcui_IN_codes(rxcuis)
    df_atc = query_RxNorm_by_rxcui_IN_codes(df['rxcui_IN'].dropna().unique())

    df = df.merge(df_atc.rename(columns={'RxCUI': 'rxcui_IN'}), how='left')

    return df


def query_RxNorm_by_rxcui_IN_codes(rxcui_INs):
    """
    Given a list of rxcui codes, queries RxNorm API and returns all codes related to that ID. Mainly
    used to return the ATC class of the drug.

    Parameters
    ----------
    rxcui_INs : list of str
        List of RxNorm rxcui_IN codes.

    Return
    ------
    pd.DataFrame
        DataFrame of all information returned from RxNorm API
    """

    # In case provided input was not unique, save time.
    rxcui_INs = np.unique(rxcui_INs)

    l = []
    for rxcui_IN in rxcui_INs:
        r = f'https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui_IN}/allProperties.json?prop=codes'
        s = requests.get(r).json()
        
        d = {}
        if s['propConceptGroup']['propConcept'] is not None:
            for sd in s['propConceptGroup']['propConcept']:
                d[sd['propName']] = sd['propValue']        
        l.append(d)
        sleep(0.03)

    df = pd.DataFrame(l)
    return df


def query_RxNorm_for_rxcui_IN_codes(rxcuis):
    """
    Given a list of rxcui codes, queries RxNorm API and returns the rxcui_IN code, which is used to
    used to return the ATC class of the drug.

    Parameters
    ----------
    rxcuis : list of str
        List of RxNorm rxcui codes.

    Return
    ------
    pd.DataFrame
        DataFrame of rxcui, rxcui_IN codes
    """

    # In case provided input was not unique, save time.
    rxcuis = np.unique(rxcuis)

    l =[]
    for rxcui in rxcuis:
        r = f'https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/related.json?tty=IN'
        s = requests.get(r).json()

        try:
            rxcui_IN = s['relatedGroup']['conceptGroup'][0]['conceptProperties'][0]['rxcui']
            d = {'rxcui': rxcui, 'rxcui_IN': rxcui_IN}
        except KeyError:
            d = {'rxcui': rxcui, 'rxcui_IN': np.NaN}

        l.append(d)
        sleep(0.03)

    df = pd.DataFrame(l)
    return df


def query_RxNorm_ndc_properties(ndc11):
    """
    Given a list of NDC11 codes, queries RxNorm API and returns information about the drug, namely
    the rxcui ID, which allows us to classify NDC codes into ATC classes.

    Parameters
    ----------
    ndc11 : list of str
        NDC11 formatted codes (i.e. 11 digit strings) used to query RxNorm API.

    Return
    ------
    pd.DataFrame
        DataFrame of all information returned from RxNorm API.
    """

    # In case provided input was not unique, save time.
    ndc11 = np.unique(ndc11)

    l =[]
    for ndc in ndc11:
        r = f'https://rxnav.nlm.nih.gov/REST/ndcproperties.json?id={ndc}'
        s = requests.get(r).json()
        if s:
            s = s['ndcPropertyList']['ndcProperty'][0]
            d = {k: s[k] for k in ['ndc10', 'ndc9', 'ndcItem', 'rxcui', 'splSetIdItem']}
            
            for i, val in enumerate(s['packagingList']['packaging']):
                d[f'packaging_{i}'] = val
            if s['propertyConceptList'] is not None:
                for sd in s['propertyConceptList']['propertyConcept']:
                    d[sd['propName']] = sd['propValue']
        # If there was no response, need to check drug history, NDC may be inactive
        else: 
            r = f'https://rxnav.nlm.nih.gov/REST/ndcstatus.json?ndc={ndc}'
            s = requests.get(r).json()
            if s:
                rxcui = s['ndcStatus']['rxcui']
                rxcui = {'': np.NaN}.get(rxcui, rxcui)
                d = {'ndcItem': s['ndcStatus']['ndc11'], 'rxcui': rxcui}
            else:
                d = {'ndcItem': ndc}

        l.append(d)
        sleep(0.02)

    df = pd.DataFrame(l)
    return df


####################################################################################################
####################################################################################################
def raw_to_clean_atc_levels():
    """
    Transform raw atc csvs (provided with repo) into cleaned standard pickle files
    """
    
    for level in Params().atc_levels:
        df = pd.read_csv(Params().labels_path+f'atc_{level}_level.csv', sep='|')
        df.to_pickle(Params().clean_path+f'atc_{level}_level.pkl')


####################################################################################################
####################################################################################################
def get_new_rxcuis():
    """
    Identifies rxcuis which exist in the ndc_properties file, but have not been updated in the
    rxcui_to_atc file

    Return
    ------
    list of str
        rxcuis
    """

    try:
        dfndc = pd.read_pickle(Params().raw_path+'ndc_properties.pkl')
    except FileNotFoundError:
        dfndc = pd.DataFrame(columns=['rxcui'])

    try:
        dfrx = pd.read_pickle(Params().raw_path+'rxcui_to_atc.pkl')
    except FileNotFoundError:
        dfrx = pd.DataFrame(columns=['rxcui'])

    new_rxcuis = dfndc[~dfndc['rxcui'].isin(dfrx['rxcui'])].rxcui.dropna().drop_duplicates().tolist()
    return new_rxcuis      


####################################################################################################
####################################################################################################
class Params():
    """
    Parameters for data files 
    """

    def __init__(self):
        self.path = ((os.path.dirname(os.path.realpath(__file__))).replace('\\', '/')) + '/'
        self.raw_path = self.path+'data/raw/'
        self.clean_path = self.path+'data/clean/'
        self.labels_path = self.path+'data/labels/'

        self.atc_levels = [1, 2, 3, 4]