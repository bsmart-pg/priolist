import os

from datetime import datetime
import time
import pandas as pd

import re
import dask.dataframe as dd
from rapidfuzz import process, distance

def calc_match(x:str, str_list: list[str]):
    res = process.extract(str(x), str_list, scorer=distance.LCSseq.normalized_distance, limit=1, score_cutoff=0.1)
    if res:
        return res[0][0]
    else:
        return None

def process_csv(file1, file2, file3, file4, file5, filter_string = None):

    print("start processing & matching")
    article = pd.read_excel(file1)
    print("done reading article xlsx")
    sales = pd.read_excel(file2, skiprows= [0,1,2,3,4,5,6,7])
    print("done reading sales xlsx")
    #jira = pd.read_excel(file3)
    jira = pd.read_csv(file3, delimiter = ";")
    print("done reading jira xlsx")
    search = pd.read_excel(file4)
    print("done reading search xlsx")
    alternatives = pd.read_excel(file5)
    print("done reading alternatives xlsx")




    ## Article processing 
    vendor_indicies = article["* LieferantenNr."].unique().tolist()
    article = article.iloc[:, [0,3,17,19,20,54]]
    article = article.dropna(subset = ["* Original Lief.MaterialNr."])
    if filter_string is not None:
        article = article[article["(*) Artikeltext Lieferant 1"].str.lower().str.contains(filter_string.lower())]
    article["* Original Lief.MaterialNr. Clean"] =  article["* Original Lief.MaterialNr."].apply(lambda x: re.sub(r'\W+', '', str(x)).strip().strip("0"))
    article["Granit-Nummer"] =  article["Granit-Nummer"].apply(lambda x: re.sub(r'\W+', '', str(x)).strip())
    lieferanten_artikel = article["* Original Lief.MaterialNr. Clean"].astype(str)
    artikelnummern = article["Granit-Nummer"].astype(str)
    markenliste = article["Marke"].str.upper().unique().tolist()

    print("Done processing articles")

    ## Sales processing
    sales = sales[sales["Lieferant 10"].isin(vendor_indicies)]
    sales = sales.dropna(subset = ["Artikelnummer"])
    sales = sales.iloc[:, [2,20,37,38]]
    sales["Artikelnummer"] =  sales["Artikelnummer"].apply(lambda x: re.sub(r'\W+', '', str(x)).strip())

    print("Done processing sales")

    ## Jira processing
    jira = jira.dropna(subset = ["OE cross reference"])
    jira = jira.iloc[:,[2,7,15]]
    jira["OE cross reference"] =  jira["OE cross reference"].apply(lambda x: re.sub(r'\W+', '', str(x)).strip().strip("0"))

    print("Done processing jira")

    ## search processing
    search = search.iloc[:,[0,2]]
    search["Suchphrase (interne Suche)"] =  search["Suchphrase (interne Suche)"].apply(lambda x: re.sub(r'\W+', '', str(x)).strip().strip("0"))
    search = search[~search["Suchphrase (interne Suche)"].str.isalpha()] # remove search with text only
    search = search.groupby("Suchphrase (interne Suche)" , as_index=False).sum().sort_values("Suchen") # add together similar searches
    search = search[search["Suchen"]>0]

    print("Done processing search")

    ## search processing
    alternatives = alternatives.iloc[:, [1,3,7,8]]
    alternatives = alternatives[alternatives["Crossrefmarke"].str.upper().isin(markenliste)]
    alternatives["Crossreferenz"] =  alternatives["Crossreferenz"].apply(lambda x: re.sub(r'\W+', '', str(x)).strip().strip("0"))

    print("Done processing alternatives")

    ## Match Sales
    start = time.time()
    ddf = dd.from_pandas(sales, npartitions=20)

    def apply_chunk(f):     # Define a function to apply to each chunk
        f['sales_match'] = f['Artikelnummer'].apply(lambda x: calc_match(str(x), artikelnummern.tolist()))
        return f
    
    ddf = ddf.map_partitions(apply_chunk) # Apply the function to each partition
    result = ddf.compute()

    new_names = [(i,"sales_" + i) for i in sales.iloc[:, 1:].columns.values]
    result.rename(columns = dict(new_names), inplace=True)

    result = result.rename(columns={"sales_match": "Granit-Nummer"})
    article = pd.merge(article, result, on="Granit-Nummer", how='left')
    
    end = time.time()
    print("Done matching sales in " + str(end - start) + "sec")

    ## Match Jira
    start = time.time()
    ddf = dd.from_pandas(jira, npartitions=20)

    def apply_chunk(j):  # Define a function to apply to each chunk
        j['jira_match'] = j['OE cross reference'].apply(lambda x: calc_match(str(x), lieferanten_artikel.tolist()))
        return j
    
    ddf = ddf.map_partitions(apply_chunk) # Apply the function to each partition
    result = ddf.compute()

    new_names = [(i,"jira_" + i) for i in jira.iloc[:, 1:].columns.values]
    result.rename(columns = dict(new_names), inplace=True)
    result = result.rename(columns={"jira_match": "* Original Lief.MaterialNr. Clean"})
    article = pd.merge(article, result, on="* Original Lief.MaterialNr. Clean", how='left')

    end = time.time()
    print("Done matching jira in " + str(end - start) + "sec")

    ## Match Search
    start = time.time()
    ddf = dd.from_pandas(search, npartitions=20)
    
    def apply_chunk(s): # Define a function to apply to each chunk
        s['search_match'] = s["Suchphrase (interne Suche)"].apply(lambda x: calc_match(str(x), lieferanten_artikel.tolist()))
        return s
    
    ddf = ddf.map_partitions(apply_chunk) # Apply the function to each partition
    result = ddf.compute()

    new_names = [(i,"search_" + i) for i in search.iloc[:, 1:].columns.values]
    result.rename(columns = dict(new_names), inplace=True)
    result = result.rename(columns={"search_match": "* Original Lief.MaterialNr. Clean"})
    article = pd.merge(article, result, on="* Original Lief.MaterialNr. Clean", how='left')

    end = time.time()
    print("Done matching search in " + str(end - start) + "sec")

    ## Match alternatives
    start = time.time()
    ddf = dd.from_pandas(alternatives, npartitions=20)
    
    def apply_chunk(s): # Define a function to apply to each chunk
        s['alternatives_match'] = s["Crossreferenz"].apply(lambda x: calc_match(str(x), lieferanten_artikel.tolist()))
        return s
    
    ddf = ddf.map_partitions(apply_chunk) # Apply the function to each partition
    result = ddf.compute()

    new_names = [(i,"alternatives_" + i) for i in alternatives.iloc[:, 1:].columns.values]
    result.rename(columns = dict(new_names), inplace=True)
    result = result.rename(columns={"alternatives_match": "* Original Lief.MaterialNr. Clean"})
    article = pd.merge(article, result, on="* Original Lief.MaterialNr. Clean", how='left')

    end = time.time()
    print("Done matching alternatives in " + str(end - start) + "sec")

    output_file = f'report_{str(datetime.now())}'
    output_file = output_file.replace(" ","_").replace("-","_").replace(":","_").replace(".","_")
    output_file = output_file + ".xlsx"
    if not os.path.exists('output/'+ output_file.split(".xlsx")[0]):
        os.makedirs('output/'+ output_file.split(".xlsx")[0])


    article['Abgang 2024 - 2025 kumuliert'] = article.loc[:,['sales_Abgang 2024','sales_Abgang 2025']].sum(axis=1, min_count=1)

    article = article.drop([
        '* LieferantenNr.',
        'Artikelnummer',
        'sales_Lieferant 10',
        'alternatives_Crossreferenz',
        'Suchphrase (interne Suche)',
        'sales_Abgang 2024',
        'sales_Abgang 2025',
        'OE cross reference',
        "* Original Lief.MaterialNr. Clean",
        "alternatives_Crossrefmarke"
        ],
        axis=1
    )

    article = article.rename(columns={
            'Granit-Nummer': 'Artikelnummer', 
            '* Original Lief.MaterialNr.': 'OE cross reference',
            'Konditionsbetrag ZPB1': 'OE Brutto Sales Price',
            'search_Suchen': 'Econda Search since Go Live',
            'jira_Vorgangsschlüssel:': 'Jira Tickets',
            'jira_Status': 'Status',
            'Art.-Nr.': 'Trex Alternatives',
            '(*) Artikeltext Lieferant 1': 'description'
        }
    )

    article.to_excel(os.path.join('output/'+ output_file.split(".xlsx")[0] + "/", output_file), index=False)

    return output_file