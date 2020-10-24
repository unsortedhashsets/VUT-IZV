

import os, re, glob, zipfile, requests
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime


columns_names = [
    "REGION", "IDENTIFIKAČNÍ ČÍSLO", "DRUH POZEMNÍ KOMUNIKACE", "ČÍSLO POZEMNÍ KOMUNIKACE", "den, měsíc, rok",
    "weekday", "čas", "DRUH NEHODY", "DRUH SRÁŽKY JEDOUCÍCH VOZIDEL", "DRUH PEVNÉ PŘEKÁŽKY",
    "CHARAKTER NEHODY", "ZAVINĚNÍ NEHODY", "ALKOHOL U VINÍKA NEHODY PŘÍTOMEN", "HLAVNÍ PŘÍČINY NEHODY",
    "usmrceno osob", "těžce zraněno osob", "lehce zraněno osob", "CELKOVÁ HMOTNÁ ŠKODA", "DRUH POVRCHU VOZOVKY",
    "STAV POVRCHU VOZOVKY V DOBĚ NEHODY", "STAV KOMUNIKACE", "POVĚTRNOSTNÍ PODMÍNKY V DOBĚ NEHODY",
    "VIDITELNOST", "ROZHLEDOVÉ POMĚRY", "DĚLENÍ KOMUNIKACE", "SITUOVÁNÍ NEHODY NA KOMUNIKACI",
    "ŘÍZENÍ PROVOZU V DOBĚ NEHODY", "MÍSTNÍ ÚPRAVA PŘEDNOSTI V JÍZDĚ", "SPECIFICKÁ MÍSTA A OBJEKTY V MÍSTĚ NEHODY",
    "SMĚROVÉ POMĚRY", "POČET ZÚČASTNĚNÝCH VOZIDEL", "MÍSTO DOPRAVNÍ NEHODY", "DRUH KŘIŽUJÍCÍ KOMUNIKACE", "DRUH VOZIDLA",
    "VÝROBNÍ ZNAČKA MOTOROVÉHO VOZIDLA", "ROK VÝROBY VOZIDLA", "CHARAKTERISTIKA VOZIDLA", "SMYK", "VOZIDLO PO NEHODĚ",
    "ÚNIK PROVOZNÍCH, PŘEPRAVOVANÝCH HMOT", "ZPŮSOB VYPROŠTĚNÍ OSOB Z VOZIDLA", "SMĚR JÍZDY NEBO POSTAVENÍ VOZIDLA",
    "ŠKODA NA VOZIDLE", "KATEGORIE ŘIDIČE", "STAV ŘIDIČE", "VNĚJŠÍ OVLIVNĚNÍ ŘIDIČE", "a", "b", "souřadnice X",
    "souřadnice Y", "f", "g", "h", "i", "j", "k", "l", "n", "o", "p", "q", "r", "s", "t", "LOKALITA NEHODY"
]

regions = {
    "PHA": "00.csv",  # nehody na území hl. m. Prahy
    "STC": "01.csv",  # nehody na území Středočeského kraje
    "JHC": "02.csv",  # nehody na území Jihočeského kraje
    "PLK": "03.csv",  # nehody na území Plzeňského kraje
    "ULK": "04.csv",  # nehody na území Ústeckého kraje
    "HKK": "05.csv",  # nehody na území Královéhradeckého kraje
    "JHM": "06.csv",  # nehody na území Jihomoravského kraje
    "MSK": "07.csv",  # nehody na území Moravskoslezského kraje
    "OLK": "14.csv",  # nehody na území Olomouckého kraje
    "ZLK": "15.csv",  # nehody na území Zlínského kraje
    "VYS": "16.csv",  # nehody na území kraje Vysočina
    "PAK": "17.csv",  # nehody na území Pardubického kraje
    "LBK": "18.csv",  # nehody na území Libereckého kraje
    "KVK": "19.csv",  # nehody na území Karlovarského kraje
}

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


"""
    Vytvořte soubor ​ download.py​ , který bude obsahovat třídu ​ DataDownloader​ . Tato třída
    bude implementovat následující metody (můžete samozřejmě přidat další vlastní, případně
    můžete přidat další parametry do funkcí, pokud to uznáte za vhodné):
"""
class DataDownloader:

    """
    inicializátor - obsahuje volitelné parametry:
        ○ url​ - ukazuje, z jaké adresy se data načítají. Defaultně bude nastavený na
        výše uvedenou URL.
        ○ folder​ - říká, kam se mají dočasná data ukládat. Tato složka nemusí na
        začátku existovat!
        ○ cache_filename​ - jméno souboru ve specifikované složce, které říká, kam
        se soubor s již zpracovanými daty z funkce ​ get_list​ bude ukládat a odkud
        se budou data brát pro další zpracování a nebylo nutné neustále stahovat
        data přímo z webu. Složené závorky (formátovací řetězec) bude nahrazený
        tříznakovým kódem (viz tabulka níže) příslušného kraje. Pro jednoduchost
        podporujte pouze formát “pickle” s kompresí gzip.
    """

    def __init__(
        self,
        url="https://ehw.fit.vutbr.cz/izv/",
        folder="data",
        cache_filename="data_{}.pkl.gz",
    ):
        self.url = url
        self.folder = folder
        self.cache_filename = cache_filename

    """
        funkce stáhne do datové složky ​ folder​ všechny soubory s daty z adresy ​ url​ .
    """

    def download_data(self):
        if os.listdir(self.folder) :
            print(f"Clean-up {self.folder}")
            for f in glob.glob(f'{self.folder}/*'):
                print(f"...Deleting - {self.folder}/{f}")
                os.remove(f)
        print(f"Processing: {self.url}")
        pattern = re.compile(rf'''(datagis-rok-.{{5}}zip)|
                                  (.datagis.{{5}}zip)|
                                  ({datetime.now().month-1}-{datetime.now().year}\.zip)''')
        s = requests.session().get(url=self.url, headers=headers)
        test_soup = BeautifulSoup(s.content, "html.parser").find_all("a", href=rf'({datetime.now().month}-{datetime.now().year}\.zip)')
        if not test_soup:
            print(f"Last update: {datetime.now().month-1}-{datetime.now().year}")
            pattern = re.compile(rf'(datagis-rok-.{{5}}zip)|(datagis.{{5}}zip)|({datetime.now().month-1}-{datetime.now().year}\.zip)')
        else:
            print(f"Last update: {datetime.now().month}-{datetime.now().year}")
            pattern = re.compile(rf'(datagis-rok-.{{5}}zip)|(datagis.{{5}}zip)|({datetime.now().month}-{datetime.now().year}\.zip)')          
        soup = BeautifulSoup(s.content, "html.parser").find_all("a", href=pattern)
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
        file_name = regions.get(region)
        #colums = [None] * 64
        #output = (columns_names,colums)
        df = None
        if file_name == None:
            print(f"ERROR: {region} not found")
            return None
        for zip_file in glob.glob(f"{self.folder}/*.zip"):
            zf = zipfile.ZipFile(zip_file)
            for csv_file in zf.namelist():
                if csv_file == file_name:
                    print(zip_file, file_name)
                    if df is None:
                        df = np.genfromtxt(zf.open(csv_file), delimiter=";", encoding="ISO-8859-1", dtype="unicode", usecols=np.arange(0,64))
                    else:
                        df = np.append(df, np.genfromtxt(zf.open(csv_file), delimiter=";", encoding="ISO-8859-1", dtype="unicode", usecols=np.arange(0,64)), 0)
                    print(f'2d np.array size: {len(df)}x{len(df[0])}')
        output = (columns_names, list(np.insert(df, 0, region, axis=1).transpose()))
        for i in range(0,64):
            print(f'{i+1}. row name: <{output[0][i]}>; element 0: <{output[1][i]}>; elements type: "{output[1][i][0].dtype}"')


if __name__ == "__main__":
    p = DataDownloader()
    p.download_data()
    p.parse_region_data("ULK")

# parse_region_data(self, region)
# get_list(self, regions = None)
