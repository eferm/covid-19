import re
from pathlib import Path

import requests
import pandas as pd

CACHE_DIR = Path('..', 'cache')
DATA_DIR = Path('..', 'data')

# US/Canada states/province/territory mapping
ISO_3166_2 = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'American Samoa': 'AS',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'District of Columbia': 'DC',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Guam': 'GU',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC',
    'North Dakota': 'ND',
    'Northern Mariana Islands':'MP',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Palau': 'PW',
    'Pennsylvania': 'PA',
    'Puerto Rico': 'PR',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'Vermont': 'VT',
    'Virgin Islands': 'VI',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY',
    # Canada
    'Ontario': 'ON',
    'Quebec': 'QC',
    }


def underscore(s):
    """camelCase to under_score"""
    s = re.sub(r'(?<!^)(?=[A-Z])', '_', s).lower()
    return s


def printif(flag, text):
    if flag:
        print(text)


def get_df(url, path, refresh, verbose, errors, **pandas_kwargs):
    """Retrieve DataFrame from either server or local cache.
    """
    path = Path(path)
    printif(verbose, f"Reading '{path.name}'...")
    try:
        file = get_file(url, path, refresh=refresh, verbose=verbose)
        df = pd.read_csv(file, **pandas_kwargs)
        return df
    except requests.HTTPError:
        printif(verbose, f"Could not find {path.name} on server.")
        if errors == 'raise':
            raise
    except Exception:
        printif(verbose, "Unknown exception")
        raise


def get_file(url, path, refresh=False, verbose=False):
    """Either download file from `url/filepath`,
    or retrieve cached file from `host/filepath`
    """
    host = re.sub(r'^https?://(?:www\.)?', '', url)
    filepath = Path(host, path)

    if not refresh:
        cached_input = _get_cached_input(filepath)
        if cached_input:
            printif(verbose, f"Found cached file '{filepath.name}'")
            return cached_input

    printif(verbose, f"No cached file '{filepath.name}', fetching from server...")
    resp = requests.get(f"{url}/{path}")
    resp.raise_for_status()
    
    printif(verbose, f"Fetched file '{filepath.name}'. Caching for later.")
    cached_input = _cache_input(filepath, resp.text)
    return cached_input


def _get_cached_input(filepath):
    path = CACHE_DIR.joinpath(filepath)
    return path if path.exists() else None


def _cache_input(filepath, text):
    path = CACHE_DIR.joinpath(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w') as f:
        f.write(text)
    return path


def write_csv(df, filename):
    path = DATA_DIR.joinpath(filename)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
