"""
| Project Implementation for IZV 2020/2021
| Script download.py
| Date: 30.10.2020
| Author: Mikhail Abramov
| xabram00@stud.fit.vutbr.cz
"""

import os
import re
import gzip
import glob
import numpy
import pickle
import zipfile
import requests 

from bs4 import BeautifulSoup
from datetime import datetime
from datetime import date

"""
Start statistics year
"""
FIRST_YEAR = 2016

"""
Columns name and dtype dictionary: {№:(REGION, DTYPE)}
"""
columns_names_dtypes = {
    0:  ("Region","U3"), 
    1:  ("ID","=U12"),
    2:  ("Communication type","i1"),
    3:  ("Communication number","i4"),
    4:  ("YYYY-MM-DD","datetime64[D]"),
    5:  ("Weekday","i1"),
    6:  ("Time","=U5"),
    7:  ("Accident type","i1"),
    8:  ("Moving vehicles crash type","i1"),
    9:  ("Fixed obstacle type","i1"),
    10: ("Accident specific","i1"),
    11: ("Accident culprit","i1"),
    12: ("Culprit alcohol level","i1"),
    13: ("Accident main causes","i2"),
    14: ("Persons killed","i1"),
    15: ("Persons seriously  injured","i1"),
    16: ("Persons slightly injured","i1"),
    17: ("Total material damage","i4"),
    18: ("Road surface type","i1"),
    19: ("Road surface condition","i1"),
    20: ("Comunication condition","i1"),
    21: ("Wind condition","i1"),
    22: ("Visibility","i1"),
    23: ("Visional condition","i1"),
    24: ("Communication division","i1"),
    25: ("Accident location","i1"),
    26: ("Accident driving management","i1"),
    27: ("Driving priotities adjustment","i1"),
    28: ("Accident specific objects","i1"),
    29: ("Directions conditions","i1"),
    30: ("Number of vehicles","i1"),
    31: ("Accident city","i1"),
    32: ("Cross type","i1"),
    33: ("Vehicle type","i1"),
    34: ("Vehicle mark","i1"),
    35: ("Manufacture year","i1"),
    36: ("Vehicle characteristic","i1"),
    37: ("Skid","i1"),
    38: ("Vehicle after accident","i1"),
    39: ("Materials transported","i1"),
    40: ("Method of persons reliasing","i1"),
    41: ("Direction of driving","i2"),
    42: ("Vehicle damage","i2"),
    43: ("Driver category","i1"),
    44: ("Driver state","i1"),
    45: ("External influence","i1"), 
    46: ("a","f"),
    47: ("b","f"),
    48: ("GPS:X","f"),
    49: ("GPS:Y","f"),
    50: ("f","f"), 
    51: ("g","f"), 
    52: ("h","=U22"), 53: ("i","=U22"), 54: ("j","=U22"), 
    55: ("k","=U22"), 56: ("l","=U22"), 57: ("n","=U22"), 
    58: ("o","=U22"), 59: ("p","=U22"), 60: ("q","=U22"),
    61: ("r","=U22"), 62: ("s","=U22"), 63: ("t","=U22"),
    64: ("Accident area","i1")
}

"""
Region's csv filenames: {REGION, FILE.csv)}
"""
regions_files = {
    "PHA": "00.csv",
    "STC": "01.csv",
    "JHC": "02.csv",
    "PLK": "03.csv",
    "ULK": "04.csv",
    "HKK": "05.csv",
    "JHM": "06.csv",
    "MSK": "07.csv",
    "OLK": "14.csv",
    "ZLK": "15.csv",
    "VYS": "16.csv",
    "PAK": "17.csv",
    "LBK": "18.csv",
    "KVK": "19.csv"
}

"""
Header to avoid bot blocker
"""
headers = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en-GB,en;q=0.9,en-US;q=0.8,hi;q=0.7,la;q=0.6",
    "cache-control": "no-cache",
    "dnt": "1",
    "pragma": "no-cache",
    "referer": "https",
    "sec-fetch-mode": "no-cors",
    "sec-fetch-site": "cross-site",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36",
}

class DataDownloader:
    """
    A class used to:
        - download statistics archives;
        - parse data in these archives;
        - record parsed statistics objects - tuple(list[str], list[np.ndarray]);
        - saving parsed statistics objects cache;
    
    Attributes
    ----------
    url : str
        indicates from which address the data is being read.
        The default value is 'https://ehw.fit.vutbr.cz/izv/'.
    folder : str
        If “folder” is set, the data will be read/saved from/to that folder.
        If the folder does not exist, creates it.
        The default value is 'data'.
    cache_filename : str
        A name of the file in the specified folder,
        where the processed data from the get_list method will be stored
        and from where the data will be taken for further processing.
    regions_cache : dictionary - {REGION:statistics objects}
        Dictionary to store processed data in program

    Methods
    -------
    download_data():
        Creates folder, downloads data archives from web
    parse_region_data(region):
        Process data arcives, generate data objects for defined region
    parse_i(element):
        Parse element value like Integer object
    parse_f(element):
        Parse element value like float object
    parse_u5(element):
        Parse element value like time column with Unicode object
    parse_u_m(element):
        Parse element value like unicode and data objects
    get_list(regions=None):
        Check program cache and cache files for defined regions.
        Leads the proccess of datagathering
    """

    def __init__(
        self,
        url="https://ehw.fit.vutbr.cz/izv/",
        folder="data",
        cache_filename="data_{}.pkl.gz",
    ):
        """
        Parameters
        ----------
        url : str
            indicates from which address the data is being read.
            The default value is 'https://ehw.fit.vutbr.cz/izv/'.
        folder : str
            If “folder” is set, the data will be read/saved from/to that folder.
            If the folder does not exist, creates it.
            The default value is 'data'.
        cache_filename : str
            A name of the file in the specified folder,
            where the processed data from the get_list method will be stored
            and from where the data will be taken for further processing.
        """
        self.url = url
        self.folder = folder
        self.cache_filename = cache_filename
        self.regions_cache = {
            "PHA": None,
            "STC": None,
            "JHC": None,
            "PLK": None,
            "ULK": None,
            "HKK": None,
            "JHM": None,
            "MSK": None,
            "OLK": None,
            "ZLK": None,
            "VYS": None,
            "PAK": None,
            "LBK": None,
            "KVK": None,
        }


    def download_data(self):
        """
        Creates folder, downloads data archives from web

        Raises
        ------
            OSError
                If it is impossible to create folder
        """

        # Try to create folder.
        if not os.path.isdir(self.folder):
            try:
                os.makedirs(self.folder)
            except OSError:
                raise OSError(f"Creation of the directory {self.folder} failed")
            else:
                print (f"Successfully created the directory {self.folder}")

        # Prepare folder for new zips.
        if os.listdir(self.folder):
            print(f"Clean-up {self.folder}")
            for f in glob.glob(f'{self.folder}/*.zip'):
                print(f"...Deleting - {self.folder}/{f}")
                os.remove(f)

        # Process url.
        print(f"Processing: {self.url}")
        pattern = re.compile(rf'''(datagis-rok-.{{5}}zip)|
                                  (.datagis.{{5}}zip)|
                                  ({datetime.now().month-1}-{datetime.now().year}\.zip)''')
        s = requests.session().get(url=self.url, headers=headers)
        test_soup = BeautifulSoup(s.content,
                                 "html.parser").find_all("a",
                                                         href=rf'({datetime.now().month-1}-{datetime.now().year}\.zip)')

        # Detect last needed file for example september or october of the current year.
        if not test_soup:
            print(f"Last update: {datetime.now().month-2}-{datetime.now().year}")
            pattern = re.compile(
                rf'(datagis-rok-.{{5}}zip)|(datagis.{{5}}zip)|({datetime.now().month-2}-{datetime.now().year}\.zip)')
        else:
            print(f"Last update: {datetime.now().month-1}-{datetime.now().year}")
            pattern = re.compile(
                rf'(datagis-rok-.{{5}}zip)|(datagis.{{5}}zip)|({datetime.now().month-1}-{datetime.now().year}\.zip)')          
        soup = BeautifulSoup(s.content, "html.parser").find_all("a", href=pattern)
        
        # For each detected archive -> download it.
        for name in soup:
            zip_url = f"{self.url}/{name['href']}"
            file_name = f"{self.folder}/{zip_url.split('/')[-1]}"
            r = requests.get(zip_url, stream=True)
            if r.status_code == requests.codes.ok:
                print(f"Downloading {zip_url} ({round(int(r.headers['content-length'])/1048576,2)}Mb) into {file_name}")
                with open(file_name, "wb") as fd:
                    for chunk in r.iter_content(chunk_size=1024):
                        if chunk:
                            fd.write(chunk)
                    fd.close()
        print(f"Finished with: {self.url}")


    def parse_region_data(self, region):
        """
        For defined region:
            - Reads raw data from csv files.
            - Process raw data.

        Parameters
        ------
        region: 
            Region what data need to proceed.

        Returns
        -------
        data object : tuple(list[str], list[np.ndarray])
            Processed data object for defined region.

        Raises
        ------
        OSError
            If it is impossible to create folder.
        """

        file_name = regions_files.get(region)
        df = None
        columns_data = None

        # Check if provided region is supported
        if file_name == None:
            raise NotImplementedError(f"ERROR: {region} not found")

        # Check the number of available archives in 2020 - 5 in 2021 - 6 and etc.   
        if len(glob.glob(f"{self.folder}/*.zip")) != int(datetime.now().year)-FIRST_YEAR+1:
            if int(datetime.now().month) != 1:
                print("WARNING: Not enough data archives, need to update")
                self.download_data()

        # prepare data converter dictionary and dtypes string for (numpy.loadtxt)
        convert = dict()
        dtypes = None
        for i in range(0,64):
            if dtypes is None:
                dtypes = f'{columns_names_dtypes[i+1][1]}'
            else:
                dtypes = f'{dtypes},{columns_names_dtypes[i+1][1]}'
            if columns_names_dtypes[i+1][1][0] == 'i':
                convert[i] = lambda x: self.parse_i(x) or '-1'
            elif columns_names_dtypes[i+1][1][0] == 'f':
                convert[i] = lambda x: self.parse_f(x) or '-1'
            elif columns_names_dtypes[i+1][1] == '=U5':
                convert[i] = lambda x: self.parse_u5(x)
            else:
                convert[i] = lambda x: self.parse_u_m(x)

        # Read csv files from zips
        print("\nParse tables:")
        for zip_file in glob.glob(f"{self.folder}/*.zip"):
            zf = zipfile.ZipFile(zip_file)
            for csv_file in zf.namelist():
                if csv_file == file_name:
                    df = numpy.loadtxt(zf.open(csv_file),
                                            delimiter=";",
                                            encoding="cp1250",
                                            converters=convert,
                                            dtype=dtypes,
                                            unpack=True,
                                            usecols=numpy.arange(0,64))
                    if columns_data == None:
                        columns_data = df
                    else:
                        for j in range(0,len(columns_data)):
                            columns_data[j] = numpy.concatenate(
                                (columns_data[j],df[j]), axis=0)
                    print(f'...Parse table {file_name} ({region}) from {zip_file} with size rows/columns: {len(columns_data)}/{len(columns_data[0])}')
        
        # Add first row with region name
        columns_data.insert(0, numpy.full((1,len(columns_data[0])), region, dtype='=U3')[0])

        # Check dataset
        print(f'\nCheck dataset for region: {region} with size rows/columns: {len(columns_data)}/{len(columns_data[0])}')
        for i in range(0, len(columns_data)):
            print(f'...{i}.\t{columns_names_dtypes[i][0]} --- {columns_data[i]} --- {columns_data[i].dtype}')
        return ([element[0] for element in columns_names_dtypes.values()], list(columns_data))


    def parse_i(self, element):
        """
        Parse provided element like intiger value
        (used like converter function for numpy.genfromtxt)

        Parameters
        ------
        element : str  
            One cell from csv file.

        Returns
        -------
        element : str
            Cleared element if data passed parse process otherwise '-1'
        """
        try:
            element = element.replace('"', '')
            int(element)
            return element
        except:
            return '-1'
        

    def parse_f(self, element):
        """
        Parse provided element like float value
        (used like converter function for numpy.genfromtxt)

        Parameters
        ------
        element : str  
            One cell from csv file.

        Returns
        -------
        element : str
            Cleared element if data passed parse process otherwise '-1'
        """
        try:
            element = element.replace('"', '').replace(',', '.')
            float(element)
            return element
        except:
            return '-1'


    def parse_u5(self, element):
        """
        Parse provided element like time value (unicode)
        (used like converter function for numpy.genfromtxt)

        Parameters
        ------
        element : str  
            One cell from csv file.

        Returns
        -------
        element : str
            Cleared element if data passed parse process otherwise '-1'
        """
        try:
            if int(element[1:3]) > 24:
                element = ''
            else:
                if int(element[3:5]) > 59:
                    element = element[1:3]
                else:
                    element = f'{element[1:3]}:{element[3:5]}'
        except:
            return ''
        return element


    def parse_u_m(self, element):
        """
        Parse provided element like unicode value
        (used like converter function for numpy.genfromtxt)

        Parameters
        ------
        element : str  
            One cell from csv file.

        Returns
        -------
        element : str
            Cleared element
        """
        return element.replace('"', '')


    def get_list(self, regions = None):
        """
        Check program cache and cache files for defined regions.
        Leads the proccess of datagathering

        Parameters
        ------
        regions : list  
            list of regions to generate data object

        Returns
        -------
        data object : tuple(list[str], list[np.ndarray])
            Processed data object for defined regions.
        """
        output = None

        # If we need to process all regions
        if regions is None:
            for i in regions_files:
                # Check program cache 
                if self.regions_cache[i] == None:
                    # Check cache file
                    if not glob.glob(f"{self.folder}/{self.cache_filename.format(i)}"):
                        # Create program and file cache
                        self.regions_cache[i] = self.parse_region_data(i)
                        print(f'\nSave dataset cache...{self.folder}/{self.cache_filename.format(i)}')
                        with gzip.open(f'{self.folder}/{self.cache_filename.format(i)}', 'wb') as cache:
                            pickle.dump(self.regions_cache[i], cache)
                        print('...Done')
                    else:
                        # Save file cache to program cache
                        print(f'\nRead dataset cache...{self.folder}/{self.cache_filename.format(i)}')
                        with gzip.open(f'{self.folder}/{self.cache_filename.format(i)}', 'rb') as cache:
                            self.regions_cache[i] = pickle.load(cache)
                        print('...Done')
                # Concentrate output for all regions
                if output == None:
                    output = self.regions_cache[i]
                else:
                    for j in range(0,len(output[1])):
                        output[1][j] = numpy.concatenate(
                            (output[1][j],self.regions_cache[i][1][j]), axis=0)
        # If we need to process specific regions
        else:
            if all(region in regions_files for region in regions):
                for i in regions:
                    # Check program cache 
                    if self.regions_cache[i] == None:
                        # Check cache file
                        if not glob.glob(f"{self.folder}/{self.cache_filename.format(i)}"):
                            # Create program and file cache
                            self.regions_cache[i] = self.parse_region_data(i)
                            print(f'\nSave dataset cache...{self.folder}/{self.cache_filename.format(i)}')
                            with gzip.open(f'{self.folder}/{self.cache_filename.format(i)}', 'wb') as cache:
                                pickle.dump(self.regions_cache[i], cache)
                            print('...Done')
                        else:
                            # Save file cache to program cache
                            print(f'\nRead dataset cache...{self.folder}/{self.cache_filename.format(i)}')
                            with gzip.open(f'{self.folder}/{self.cache_filename.format(i)}', 'rb') as cache:
                                self.regions_cache[i] = pickle.load(cache)
                            print('...Done')
                    # Concentrate output for all regions
                    if output == None:
                        output = self.regions_cache[i]
                    else:
                        for j in range(0,len(output[1])):
                            output[1][j] = numpy.concatenate(
                                (output[1][j],self.regions_cache[i][1][j]), axis=0)
            else:
                raise NotImplementedError(f"ERROR: {regions} not found")

        # Print logs and return output
        if regions is None:
            print('\nDATASET for all regions:')
        else:
            print(f'\nDATASET for regions: {regions}:')
        for i in range(0,len(output[1])):
            print(f'...{i}.\t<{output[0][i]}> --- <{output[1][i]}> --- "{output[1][i][0].dtype}"')
        print('DATASET collecting is finished\n')
        return(output)


if __name__ == "__main__":
    """
    Main:
        main for test issues with predefined regions:
            ['STC','MSK','PAK']
    """
    DataDownloader().get_list(['STC','MSK','PAK'])