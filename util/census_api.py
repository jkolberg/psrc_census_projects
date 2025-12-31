import pandas as pd


def get_table(variables,year,for_predicates,in_predicates,dataset_url,api_key,timeout=15):
    """
    Takes in a list of variables and returns a dataframe
    """
    # create base url
    HOST = "https://api.census.gov/data"
    base_url = "/".join([HOST,str(year),dataset_url])
    # split variables into chunks of 49
    chunks = [variables[x:x+49] for x in range(0, len(variables), 49)]
    df = pd.DataFrame()
    for chunk in chunks:
        # create url
        predicates = {}
        predicates["get"] = ",".join(chunk)
        predicates["for"] = for_predicates
        if in_predicates == None:
            pass
        else:
            predicates["in"] = in_predicates
        predicates["key"] = api_key
        # make api request
        r = requests.get(base_url,params=predicates, timeout=timeout)
        chunk_df = pd.DataFrame(r.json()[1:], columns=r.json()[0])
        # merge chunked data into a single df
        if df.empty:
            df = chunk_df
        else:
            df.drop(columns=['state','county','tract'],inplace=True,errors='ignore')
            df = df.merge(chunk_df,left_index=True,right_index=True)
    return df
    
def combine_groups(variables_dict,df):
    """
    Takes in a dictionary of variables and a dataframe
    """
    for key, value in variables_dict.items():
        df[key] = df[value].astype(float).sum(axis=1)
        # drop old columns
        df = df.drop(value,axis=1)

    return df

def create_in_predicates(geog,cnty_str,state_id):
    """
    Takes in a geography and returns in_predicates

    Parameters
    ----------
    geog : str
        'state', 'county', 'place', 'tract' or 'block group'
    cnty_str : list
        list of strings of county fips codes
    state_id : int
        2 digit state fips code
    """
    # create base url
    if geog in ['tract','block group','block']:
        counties_str = ','.join(cnty_str)
        in_predicates = f'state:{state_id}',f'county:{counties_str}'
    elif geog in ['county','place','congressional district']:
        in_predicates = f'state:{state_id}'
    elif geog == 'state':
        in_predicates = None
    else:
        raise ValueError("geog must be: 'state', 'county', 'congressional district', 'place', 'tract', 'block group' or 'block'")
    return in_predicates

def get_dec_data(variables_dict, year, geog,dataset, api_key,filter, counties):
    """
    Takes in a dictionary of variables and returns decennial data in a dataframe.

    Parameters
    ----------
    variables_dict : dict
        dictionary of variables
    year : int
        2010 or 2020
    geog : str 
        'state', 'county', 'place', 'tract', 'block group' or 'block'
    dataset : str
        'pl, 'dhc','ddhca', 'dp', 'sf1', 'sf2'
    api_key : str
        your census api key
    counties : list
        list of 5 digit county fips codes (first 2 digits are state fips)
    """
    filter_cnty_str = {
        'counties': [str(cnty)[2:] for cnty in counties],
    }

    in_predicates = create_in_predicates(geog,filter_cnty_str[filter], int(str(counties[0])[:2]))

    for_predicates = f'{geog}:*'
    dataset_url = f'dec/{dataset}'

    # convert variables_dict to list
    start_vars = ['GEO_ID','NAME']
    variables = [ i for j in variables_dict.values() for i in j ]
    variables = start_vars + variables
    
    # get data
    df = get_table(variables,year,for_predicates,in_predicates,dataset_url,api_key,timeout=60)

    #sum groups
    df = combine_groups(variables_dict,df)

    # convert geoid to an integer
    geog_slices = {
        'block': -15,
        'tract': -11,
        'block group': -12,
        'county': -5,
        'place': -7,
        'state': -2
    }

    df['geoid'] = df['GEO_ID'].str.slice(start=geog_slices[geog]).astype('int64')

    # only keep geoid and variables columns
    df.rename(columns={'NAME':'name'},inplace=True)
    df = df[['geoid','name'] + list(variables_dict.keys())]
    return df