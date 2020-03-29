from pathlib import Path
import requests


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


def printif(flag, text):
    if flag:
        print(text)


def get_file(url, filepath, refresh=False, verbose=False):
    filepath = Path(filepath)
    if not refresh:
        cached_input = _get_cached_input(filepath)
        if cached_input:
            printif(verbose, f"Found cached file '{filepath.name}'")
            return cached_input

    printif(verbose, f"No cached file '{filepath.name}', fetching from server...")
    resp = requests.get(f"{url}/{filepath}")
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
