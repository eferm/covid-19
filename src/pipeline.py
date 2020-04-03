import re
import abc
import datetime as dt
from pathlib import Path

import requests
import pandas as pd
import numpy as np

from utils import get_df, underscore, ISO_3166_2


class DataSource(abc.ABC):
    """A data source.
    """
    def __init__(self, name, shortname, force_refresh, verbose):
        self.name = name
        self.shortname = shortname
        # private
        self._refresh = force_refresh
        self._verbose = verbose
        self._raw = None
        self._clean = None

    @property
    @abc.abstractmethod
    def raw(self):
        pass

    @property
    @abc.abstractmethod
    def clean(self):
        pass


class ECDC(DataSource):
    """European Centre for Disease Prevention and Control
    
    Source: https://www.ecdc.europa.eu/en/publications-data/
    download-todays-data-geographic-distribution-covid-19-cases-worldwide
    """
    def __init__(self, force_refresh=True, verbose=False):
        super(ECDC, self).__init__(
            'European Centre for Disease Prevention and Control',
            'ECDC',
            force_refresh,
            verbose
        )
        self.host = 'https://opendata.ecdc.europa.eu'
        self.path = 'covid19/casedistribution/csv'

    @property
    def raw(self):

        if self._raw is not None:
            return self._raw

        df = get_df(
            self.host,
            self.path,
            self._refresh,
            self._verbose,
            errors='raise',
            parse_dates=['dateRep'],
            dayfirst=True,
            keep_default_na=False,  # don't treat Namibia as 'NA'
        )
        self._raw = df  # cache
        return df

    @property
    def clean(self):

        if self._clean is not None:
            return self._clean

        df = (
            self.raw
            .rename(columns=underscore)
            .replace(r'_', ' ', regex=True)
            .replace({
                'United Kingdom': 'UK',
                'United States of America': 'US',
            })
            .assign(date=lambda df: df['date_rep'])
            .sort_values(['date', 'countries_and_territories'])
            .reset_index(drop=True)
        )
        
        # adjust date for tz
        df.loc[:, ['date']] = df.groupby('countries_and_territories')['date'].shift()
        df = df.loc[df['date'].notnull()]

        # make metrics cumulative
        df.loc[:, ['cases', 'deaths']] = (
            df
            .groupby('countries_and_territories')
            [['cases', 'deaths']]
            .cumsum()
        )

        # filter out last values if they're suddenly 0
        prev = ['prev_cases', 'prev_deaths']
        df = df.assign(
            prev_cases=df['cases'],
            prev_deaths=df['deaths']
        )
        df.loc[:, prev] = (
            df
            .groupby('countries_and_territories')
            [prev]
            .shift()
        )

        idx = (
            df
            .reset_index()
            .groupby('countries_and_territories')
            [['index', 'cases', 'deaths', *prev]]
            .last()
            .loc[lambda df: (df['cases'] == df['prev_cases'])
                 & (df['deaths'] == df['prev_deaths'])]
            ['index']
        )
        df = (
            df
            .drop(index=idx)
            .drop(columns=['prev_cases', 'prev_deaths'])
        )
        
        # forward fill missing values
        df = (
            df
            .set_index('date')
            .groupby('countries_and_territories')
            .resample('D')
            .ffill()
            .interpolate()
            .drop(columns=['countries_and_territories'])
            .reset_index()
        )
        self._clean = df  # cache
        return df


class JHU(DataSource):
    """Johns Hopkins University
    Center for Systems Science and Engineering
    """
    def __init__(self, force_refresh=False, verbose=False):
        super(JHU, self).__init__(
            'Johns Hopkins University',
            'JHU',
            force_refresh,
            verbose
        )
        self.host = 'https://raw.githubusercontent.com'
        self.path = 'CSSEGISandData/COVID-19' \
            + '/master/csse_covid_19_data/csse_covid_19_daily_reports'
    
    @property
    def raw(self):

        if self._raw is not None:
            return self._raw

        dfs = self._get_data(self._refresh, self._verbose)
        df = self._consolidate(dfs)
        self._raw = df  # cache
        return df

    @property
    def clean(self):
        if self._clean is not None:
            return self._clean

        df = (
            self.raw
            .pipe(self._patch_errors)
            .pipe(self._clean_and_resolve)
        )
        self._clean = df  # cache
        return df

    def _get_data(self, refresh, verbose):
        """Download or fetch data from cache.
        
        Downloads 1 file per day, and unions files with the same schema.
        Unioning across schemas is taken care of `self._consolidate`.

        Returns:
            List[DataFrame]: One DataFrame for each schema version.
        """
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
                df = get_df(
                    self.host,
                    f"{self.path}/{filename}",
                    refresh,
                    verbose,
                    errors='ignore',
                    **schema['kwargs']
                )
                if df is not None:
                    files_dfs.append(df.assign(_filedate=date, _filename=filename))
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
            .assign(admin2=np.nan),

            df_by_schema['20200301']
            .drop(['Latitude', 'Longitude'], axis=1)
            .rename(columns=normalize)
            .assign(admin2=np.nan),

            df_by_schema['20200322']
            .drop(['FIPS', 'Lat', 'Long_', 'Active', 'Combined_Key'], axis=1)
            .rename(columns=normalize)
        ])

        # reindex
        cols = [
            '_filedate',
            '_filename',
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

    def _patch_errors(self, df):
        """Fix spot errors in data.

        Returns:
            DataFrame: Patched data
        """
        def row(filedate, country=None, state=None):
            idx = df['_filedate'] == pd.to_datetime(filedate)
            if country:
                idx &= df['country_region'] == country
            if state:
                idx &= df['province_state'] == state
            return idx

        df = df.copy()

        # there are 5 cases in Travis, CA that were later attributed to Diamond Princess
        df.loc[row('2020-02-21', state='Travis, CA'), 'confirmed'] = 0

        # fill in estimated data to Hubei
        # useful for offset plots which origin from cases >= 100 and deaths >= 10
        # values estimated using Excel's GROWTH function, using the first 10 values
        china_patch = pd.DataFrame(
            [(dt.datetime(2020, 1, 18), '', 'China', 'Hubei', None, 96, 4, np.nan, None),
             (dt.datetime(2020, 1, 19), '', 'China', 'Hubei', None, 132, 6, np.nan, None),
             (dt.datetime(2020, 1, 20), '', 'China', 'Hubei', None, 182, 8, np.nan, None),
             (dt.datetime(2020, 1, 21), '', 'China', 'Hubei', None, 250, 11, np.nan, None),
            ],
            columns=df.columns)
        df = pd.concat([df, china_patch])

        return df

    def _clean_and_resolve(self, df):
        """Clean and resolve country/state value labels.

        Returns:
            DataFrame: Clean data
        """
        df = df.copy()

        df['country_region'] = df['country_region'].str.strip()
        df['province_state'] = df['province_state'].str.strip()

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
        df['province_state_grouped'] = df['province_state']  # store orig value
        df['province_state'] = df['province_state'].replace(self._get_state_resolution(df))
        # resolve dirty countries
        df['country_region'] = df['country_region'].replace(self._get_country_resolution())

        return (
            df
            .assign(
                date=lambda df: df['_filedate'],
                # for groupby
                province_state=lambda df: df['province_state'].fillna(""),
                admin2=lambda df: df['admin2'].fillna("")
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
            .groupby(['date', 'country_region', 'province_state', 'admin2',
                      '_filedate', '_filename'])
            .agg({
                'confirmed': sum,
                'deaths': sum,
                'recovered': sum,
                'last_update': max,
                'province_state_grouped': list,
            })
            .reset_index()
            .replace(r'^\s*$', np.nan, regex=True)
            .sort_values(['date', 'country_region', 'province_state', 'admin2'])
            .reset_index(drop=True)
        )
    
    @staticmethod
    def _get_country_resolution():
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
    def _get_state_resolution(df):

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
            .replace({np.nan: None})  # needed to avoid mapping from np.nan
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


class CTP(DataSource):
    """The COVID Tracking Project
    """
    def __init__(self, force_refresh=True, verbose=False):
        super(CTP, self).__init__(
            'The COVID Tracking Project',
            'CTP',
            force_refresh,
            verbose
        )
        self.host = 'http://covidtracking.com'
        self.path = 'api/states/daily.csv'
    
    @property
    def raw(self):

        if self._raw is not None:
            return self._raw

        df = get_df(
            self.host,
            self.path,
            self._refresh,
            self._verbose,
            errors='raise',
            parse_dates=['dateChecked', 'date'],
        )
        self._raw = df  # cache
        return df

    @property
    def clean(self):

        if self._clean is not None:
            return self._clean

        # map AK -> Alaska
        states = dict(map(reversed, ISO_3166_2.items()))
        df = (
            self.raw
            .rename(columns=underscore)
            .assign(state=lambda df: df['state'].map(states))
            .sort_values(['date', 'state'])
            .reset_index(drop=True)
        )
        self._clean = df  # cache
        return df


class NYT(DataSource):
    """The New York Times"""
    
    def __init__(self, force_refresh=True, verbose=False):
        super(NYT, self).__init__(
            'The New York Times',
            'NYT',
            force_refresh,
            verbose
        )
        self.host = 'https://raw.githubusercontent.com'
        self.path = 'nytimes/covid-19-data/master/us-counties.csv'
    
    @property
    def raw(self):

        if self._raw is not None:
            return self._raw

        df = get_df(
            self.host,
            self.path,
            self._refresh,
            self._verbose,
            errors='raise',
            parse_dates=['date'],
        )
        self._raw = df  # cache
        return df

    @property
    def clean(self):

        if self._clean is not None:
            return self._clean

        df = (
            self.raw
            .assign(fips=lambda df: df['fips'].astype('Int64'))
            .sort_values(['date', 'state', 'county'])
            .reset_index(drop=True)
        )
        self._clean = df  # cache
        return df


class DPC(DataSource):
    """Dipartimento della Protezione Civile
    Presidenza del Consiglio dei Ministri
    """
    def __init__(self, force_refresh=True, verbose=False):
        super(DPC, self).__init__(
            'Dipartimento della Protezione Civile',
            'DPC',
            force_refresh,
            verbose
        )
        self.host = 'https://raw.githubusercontent.com'
        self.path = 'pcm-dpc/COVID-19' \
            + '/master/dati-regioni/dpc-covid19-ita-regioni.csv'
    
    @property
    def raw(self):

        if self._raw is not None:
            return self._raw

        df = get_df(
            self.host,
            self.path,
            self._refresh,
            self._verbose,
            errors='raise',
            parse_dates=['data'],
        )
        self._raw = df  # cache
        return df

    @property
    def clean(self):

        if self._clean is not None:
            return self._clean

        df = (
            self.raw
            .assign(date=lambda df: df['data'].astype('<M8[D]'))
            .sort_values(['date', 'codice_regione'])
            .reset_index(drop=True)
        )
        self._clean = df  # cache
        return df
