import re
import datetime as dt
from pathlib import Path

import pandas as pd
import numpy as np

from utils import get_file, printif, ISO_3166_2


class JHU:
    """Note that data source consists of 1 file per day, and (so far)
    3 different schemas that change over time. This pipeline therefore
    spends a fair bit of code normalizing data into a common shape."""
    
    def __init__(self, force_refresh=False, verbose=False):
        self._refresh = force_refresh
        self._verbose = verbose
    
    @property
    def raw(self):
        return self._consolidate(self._get_data(self._refresh, self._verbose))

    @property
    def clean(self):
        return self.raw.pipe(self._patch).pipe(self._clean)
    
    @property
    def timestamp(self):
        return self._timestamp if hasattr(self, '_timestamp') else None

    def _get_data(self, refresh, verbose):
        """Download or fetch data from cache.
        
        Downloads 1 file per day, and unions files with the same schema.
        Unioning across schemas is taken care of `self._consolidate`.

        Returns:
            List[DataFrame]: One DataFrame for each schema version.
        """

        source = {
            'url': 'https://raw.githubusercontent.com',
            'repo': 'CSSEGISandData/COVID-19',
            'branch': 'master',
            'dir': 'csse_covid_19_data/csse_covid_19_daily_reports',
        }

        schemas = {
            '20200122': {
                'dates': pd.date_range('2020-01-22', '2020-02-29'),
                'kwargs': dict(parse_dates=['Last Update']),
            },
            '20200301': {
                'dates': pd.date_range('2020-03-01', '2020-03-21'),
                'kwargs': dict(parse_dates=['Last Update']),
            },
            '20200322': {
                'dates': pd.date_range('2020-03-22', dt.datetime.today()),
                'kwargs': dict(parse_dates=['Last_Update']),
            },
        }

        schemas_dfs = {}

        for schema_name, schema in schemas.items():
            files_dfs = []
            for date in schema['dates']:
                filename = f"{date.strftime('%m-%d-%Y')}.csv"
                filepath = Path(source['repo'], source['branch'], source['dir'], filename)
                printif(verbose, f"Read {filepath.name}...")
                try:
                    file = get_file(source['url'], filepath, refresh=refresh, verbose=verbose)
                    df = pd.read_csv(file, **schema['kwargs']).assign(filedate=date)
                    files_dfs.append(df)
                except:
                    printif(verbose, f"Skipping file {filepath.name} as it does not exist.")

            # validate schemas
            columns = list(map(lambda df: set(df.columns), files_dfs))
            assert all(x == columns[0] for x in columns), f"schemas differ for {schema_name}"

            schemas_dfs[schema_name] = pd.concat(files_dfs)

        return schemas_dfs

    def _consolidate(self, df_by_schema):
        """Normalize schemas and union. The second step for
        combining data from across the source files.

        Returns:
            DataFrame: Unioned data with normalized schema
        """

        def normalize(s):
            return re.sub('[/ ]', '_', s).lower()

        # normalize schemas and concat
        df = pd.concat([

            df_by_schema['20200122']
            .rename(columns=normalize)
            .assign(admin2=None),

            df_by_schema['20200301']
            .drop(['Latitude', 'Longitude'], axis=1)
            .rename(columns=normalize)
            .assign(admin2=None),

            df_by_schema['20200322']
            .drop(['FIPS', 'Lat', 'Long_', 'Active', 'Combined_Key'], axis=1)
            .rename(columns=normalize)
        ])

        # reindex
        cols = [
            'filedate',
            'country_region',
            'province_state',
            'admin2',
            'confirmed',
            'deaths',
            'recovered',
            'last_update',
        ]

        df = df.sort_values(cols).reset_index(drop=True)[cols]
        self._timestamp = max(df['last_update']).strftime("%Y-%m-%d %H:%M:%S")
        return df

    def _patch(self, df):
        """Fix spot errors in data.

        Returns:
            DataFrame: Patched data
        """
        def row_index(filedate, country=None, state=None):
            idx = df['filedate'] == pd.to_datetime(filedate)
            if country:
                idx &= df['country_region'] == country
            if state:
                idx &= df['province_state'] == state
            return idx

        df = df.copy()

        # there are 5 cases in Travis, CA that were later attributed to Diamond Princess
        df.loc[row_index('2020-02-21', state='Travis, CA'), 'confirmed'] = 0

        # 2020-03-12
        # 2020-03-15
        # source: https://www.worldometers.info/coronavirus/
        series = 'confirmed'
        df.loc[row_index('2020-03-12', 'Italy'), series] = 15113
        df.loc[row_index('2020-03-12', 'France'), series] = 2876
        df.loc[row_index('2020-03-12', 'Spain'), series] = 3146
        df.loc[row_index('2020-03-12', 'Germany'), series] = 2745
        df.loc[row_index('2020-03-12', 'United Kingdom'), series] = 590

        df.loc[row_index('2020-03-15', 'France'), series] = 5423
        df.loc[row_index('2020-03-15', 'United Kingdom'), series] = 1391

        series = 'deaths'
        df.loc[row_index('2020-03-12', 'Italy'), series] = 1016
        df.loc[row_index('2020-03-12', 'France'), series] = 61
        df.loc[row_index('2020-03-12', 'Spain'), series] = 86
        df.loc[row_index('2020-03-12', 'Germany'), series] = 6
        df.loc[row_index('2020-03-12', 'United Kingdom'), series] = 10

        df.loc[row_index('2020-03-15', 'France'), series] = 127
        df.loc[row_index('2020-03-15', 'United Kingdom'), series] = 35

        series = 'recovered'
        df.loc[row_index('2020-03-12', 'Italy'), series] = 1258
        df.loc[row_index('2020-03-12', 'Spain'), series] = 189

        # fill in estimated data to Hubei
        # useful for offset plots which origin from cases >= 100 and deaths >= 10
        # values estimated using Excel's GROWTH function, using the first 10 values
        china_patch = pd.DataFrame(
            [(dt.datetime(2020, 1, 18), 'China', 'Hubei', None, 96, 4, np.nan, None),
             (dt.datetime(2020, 1, 19), 'China', 'Hubei', None, 132, 6, np.nan, None),
             (dt.datetime(2020, 1, 20), 'China', 'Hubei', None, 182, 8, np.nan, None),
             (dt.datetime(2020, 1, 21), 'China', 'Hubei', None, 250, 11, np.nan, None),
            ],
            columns=df.columns)
        df = pd.concat([df, china_patch])

        return df

    def _clean(self, df):
        """Clean and resolve country/state value labels.

        Returns:
            DataFrame: Clean data
        """
        df = df.copy()

        df = df.replace({np.nan: None})  # np.nan -> None
        df['country_region'] = df['country_region'].str.strip()
        df['province_state'] = df['province_state'].str.strip()

        df['day'] = df['filedate'].dt.strftime('%Y-%m-%d')

        # resolve dirty labels
        # for older data this will resolve mixed county/state information to the state
        # i.e. it will lose some older county information
        # before:
        # country,             state, deaths,         last_update
        #      US, Orange County, CA,      2, 2020-02-09 01:00:00
        #      US,   Los Angeles, CA,      2, 2020-02-09 02:00:00
        #      US,        California,      1, 2020-02-09 03:00:00
        # after:
        # country,             state, deaths,         last_update
        #      US,        California,      2, 2020-02-09 01:00:00
        #      US,        California,      2, 2020-02-09 02:00:00
        #      US,        California,      1, 2020-02-09 03:00:00
        df['_province_state'] = df['province_state']  # store orig value
        df['province_state'] = df['province_state'].replace(self.get_state_resolution(df))
        # resolve dirty countries
        df['country_region'] = df['country_region'].replace(self.get_country_resolution())

        renames = {
            'country_region': 'country',
            'province_state': 'state',
            'admin2': 'county',
        }

        return (
            df
            .rename(columns=renames)
            .assign(
                state=lambda df: df['state'].fillna(df['country']),
                county=lambda df: df['county'].fillna(df['state'])
            )
            # sum together resolved countries/states
            # before:
            # country,        state, deaths,         last_update
            #      US,   California,      2, 2020-02-09 01:00:00
            #      US,   California,      2, 2020-02-09 02:00:00
            #      US,   California,      1, 2020-02-09 03:00:00
            # after:
            # country,        state, deaths,         last_update
            #      US,   California,      5, 2020-02-09 03:00:00
            .groupby(['filedate', 'day', 'country', 'state', 'county'])
            .agg({
                'confirmed': sum,
                'deaths': sum,
                'recovered': sum,
                'last_update': max,
                '_province_state': list,
            })
            .reset_index()
            .sort_values(['filedate', 'country', 'state', 'county'])
            .reset_index(drop=True)
        )
    
    @staticmethod
    def get_country_resolution():
        # this dict not meant to start holy wars
        return {
            'Bahamas, The': 'Bahamas',
            'Congo (Brazzaville)': 'Congo',
            'Congo (Kinshasa)': 'Congo',
            'Gambia, The': 'Gambia',
            'Hong Kong SAR': 'Hong Kong',
            'Iran (Islamic Republic of)': 'Iran',
            'Korea, South': 'South Korea',
            'Macao SAR': 'Macau',
            'Mainland China': 'China',
            'Republic of Ireland': 'Ireland',
            'Republic of Korea': 'South Korea',
            'Republic of Moldova': 'Moldova',
            'Republic of the Congo': 'Congo',
            'Russian Federation': 'Russia',
            'Taiwan*': 'Taiwan',
            'Taipei and environs': 'Taiwan',
            'The Bahamas': 'Bahamas',
            'The Gambia': 'Gambia',
            'United Kingdom': 'UK',
            'Viet Nam': 'Vietnam',
            'occupied Palestinian territory': 'Palestine',
        }
    
    @staticmethod
    def get_state_resolution(df):

        oneoffs = {
            # US
            'Omaha, NE (From Diamond Princess)': 'Diamond Princess',
            'Travis, CA (From Diamond Princess)': 'Diamond Princess',
            'Unassigned Location (From Diamond Princess)': 'Diamond Princess',
            'Lackland, TX (From Diamond Princess)': 'Diamond Princess',
            'Grand Princess Cruise Ship': 'Grand Princess',
            'United States Virgin Islands': 'Virgin Islands',
            'Virgin Islands, U.S.': 'Virgin Islands',
            # Canada
            'Edmonton, Alberta': 'Alberta',
            'Calgary, Alberta': 'Alberta',
            # UK
            'United Kingdom': 'UK',
            # France
            'Fench Guiana': 'French Guiana',
        }

        df = (
            df
            .copy()
            .loc[df['country_region'].isin(['US', 'Canada', 'UK', 'France']), ['province_state']]
            .rename(columns={'province_state': 'raw'})
            .drop_duplicates()
            .dropna()
            .sort_values('raw')
            .reset_index(drop=True)
        )

        df['_clean'] = df['raw'].map(lambda s: str(s).strip().replace('.', ''))

        # "Alabama" -> "AL"
        # df['_abbrv'] = df['_clean'].map(states)

        # "County, AL" -> "AL"
        df['_extracted'] = df['_clean'].str.extract(r', (\w{2})$')
        df['_extracted'] = df['_extracted'].map(dict(map(reversed, ISO_3166_2.items())))

        # coalesce to resolved property
        # df['resolved'] = df['_abbrv'].fillna(df['_extracted']).fillna(df['_clean'])
        df['resolved'] = df['_extracted'].fillna(df['_clean'])

        d = df.set_index('raw')['resolved'].to_dict()
        d.update(oneoffs)

        return d


class CTP:
    """The COVID Tracking Project"""
    
    def __init__(self, force_refresh=True, verbose=False):
        self._refresh = force_refresh
        self._verbose = verbose
    
    @property
    def raw(self):
        return self._get_data(self._refresh, self._verbose)

    @property
    def clean(self):
        return self.raw.pipe(self._clean)
    
    @property
    def timestamp(self):
        return self._timestamp if hasattr(self, '_timestamp') else None

    def _get_data(self, refresh, verbose):
        source = {
            'url': 'http://covidtracking.com/api',
            'filepath': Path('states/daily.csv'),
        }
        file = get_file(source['url'], source['filepath'], refresh=refresh, verbose=verbose)
        df = pd.read_csv(file, parse_dates=['dateChecked'])
        self._timestamp = max(df['dateChecked']).strftime("%Y-%m-%d %H:%M:%S")
        return df

    def _clean(self, df):
        def normalize(s):
            s = re.sub(r'(?<!^)(?=[A-Z])', '_', s).lower()
            return s
        df = self.raw.rename(columns=normalize)
        # map AK -> Alaska
        states = dict(map(reversed, ISO_3166_2.items()))
        df['state'] = df['state'].map(states)
        df = df.sort_values(['date', 'state']).reset_index(drop=True)
        return df


class NYT:
    """The New York Times"""
    
    def __init__(self, force_refresh=True, verbose=False):
        self._refresh = force_refresh
        self._verbose = verbose
    
    @property
    def raw(self):
        return self._get_data(self._refresh, self._verbose)

    @property
    def clean(self):
        return self.raw.pipe(self._clean)
    
    @property
    def timestamp(self):
        return self._timestamp if hasattr(self, '_timestamp') else None

    def _get_data(self, refresh, verbose):
        source = {
            'url': 'https://raw.githubusercontent.com',
            'repo': 'nytimes/covid-19-data',
            'branch': 'master',
            'filename': 'us-counties.csv',
        }
        filepath = Path(source['repo'], source['branch'], source['filename'])
        file = get_file(source['url'], filepath, refresh=refresh, verbose=verbose)
        df = pd.read_csv(file, parse_dates=['date'])
        self._timestamp = max(df['date']).strftime("%Y-%m-%d %H:%M:%S")
        return df

    def _clean(self, df):
        df = df.copy()
        df['fips'] = df['fips'].astype('Int64')
        df = df.sort_values(['date', 'state', 'county']).reset_index(drop=True)
        return df
