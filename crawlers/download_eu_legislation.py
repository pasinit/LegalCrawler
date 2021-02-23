import os
import sys
import requests
from functools import partial
from multiprocessing import cpu_count, Pool
from bs4 import BeautifulSoup
from data import DATA_DIR
from crawlers.helpers import clean_text
import logging


sys.setrecursionlimit(100000)

root_dir = os.path.join(DATA_DIR, 'eu')

langs = {
    'Bulgarian': 'bg',
    'Croatian': 'hr',
    'Czech': 'cs',
    'Danish': 'da',
    'Dutch': 'nl',
    'English': 'en',
    'Estonian': 'et',
    'Finnish': 'fi',
    'French': 'fr',
    'German': 'de',
    'Greek': 'el',
    'Hungarian': 'hu',
    'Irish': 'ga',
    'Italian': 'it',
    'Latvian': 'lv',
    'Lithuanian': 'lt',
    'Maltese': 'mt',
    'Polish': 'pl',
    'Portuguese': 'pt',
    'Romanian': 'ro',
    'Slovak': 'sk',
    'Slovenian': 'sl',
    'Spanish': 'es',
    'Swedish': 'sv'
}

def get_lang_datadir(lang):
    dir = os.path.join(root_dir, lang)
    if not os.path.exists(dir):
        os.makedirs(dir)
    return dir


def get_file_by_id(celex_id, langs=('EN',)):
    for lang in langs:
        lang = lang.upper()
        url = f'https://eur-lex.europa.eu/legal-content/{lang}/TXT/HTML/?uri=CELEX:{celex_id}'
        lang_dir = get_lang_datadir(lang)
        filename = os.path.join(lang_dir, f'{celex_id}.txt')
        try:
            content = requests.get(url).text
            if 'The requested document does not exist.' in content:
                logging.warning(f'{celex_id} DOES NOT EXIST IN {lang}')
                continue
            with open(filename, 'w', encoding='utf-8') as file:
                content = clean_text(content)
                if "docHtml" in content:
                    cleantext = BeautifulSoup(content, "lxml").find(
                        "div", {"id": "docHtml"}).text
                else:
                    cleantext = BeautifulSoup(content, "lxml").text
                file.write(cleantext)
        except Exception as e:
            logging.warning(f'{e} {celex_id}')


def download_eu_law(languages=('EN',), processed_ids=None):
    if processed_ids is None:
        processed_ids = set()
    sparql_query = """http://publications.europa.eu/webapi/rdf/sparql?default-graph-uri=&query=prefix+cdm%3A+%3Chttp%3A%2F%2Fpublications.europa.eu%2Fontology%2Fcdm%23%3E%0D%0A%0D%0Aselect+%3Fcelex_id%0D%0Awhere+%7B%0D%0A%3Feu_act+cdm%3Aresource_legal_id_celex+%3Fcelex_id.%0D%0A%3Feu_act+a+%3Feu_act_type.%0D%0A%3Feu_act+cdm%3Aresource_legal_number_natural+%3Feu_act_number.%0D%0AFILTER%28%3Feu_act_type+IN+%28cdm%3Aregulation%2C+cdm%3Adirective%2C+cdm%3Adecision%29%29%0D%0A%7D&format=text%2Fhtml&timeout=0&debug=on&run=+Run+Query+"""

    try:
        content = requests.get(sparql_query).text
        celex_ids = set([span.text[1:11] for span in BeautifulSoup(
            content, "html.parser").find_all('pre')])
    except:
        logging.error('EURLEX SPARQL ENDPOINT NOT RESPONSIVE')
        return 0
    num_processes = cpu_count()
    logging.info(f'celex_ids to download: {" ".join(list(celex_ids)[:10])}')
    logging.info(f'celex_ids already processed: {" ".join(list(processed_ids)[:10])}')
    logging.info(f'ids to download: {len(celex_ids)}')
    celex_ids = celex_ids-processed_ids
    logging.info(f'ids to download after removing already processed ids: {len(celex_ids)}')
    with Pool(processes=num_processes) as pool:
        pool.starmap(partial(get_file_by_id), [
                     (cid, languages) for cid in celex_ids])

import os
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    languages = list(langs.values())
    if not os.path.exists(root_dir):
        os.makedirs(root_dir)
    else:
        ids = set()
        for root, dirs, files in os.walk(root_dir):
            for file in files:
                with open(file) as lines:
                    l = lines.read().strip()
                    if l.startswith('ERROR'):
                        os.remove(file)
                        continue

                ids.add(file.replace('.txt', ''))


    logging.info(f'found {len(ids)} ids')


    download_eu_law(languages, ids)
