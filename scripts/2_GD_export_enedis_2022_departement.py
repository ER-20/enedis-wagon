#!/usr/bin/env python
# coding: utf-8

# In[48]:


# import libraries
import pandas as pd
import numpy as np
import os
import requests
import sys
from io import BytesIO
# chemin du dossier de travail
DATA_PATH = '../data/raw/'
DATA_PROCESSED = '../data/processed/'
DATA_CSV = 'enedis_2022.csv'
# import du csv
df = pd.read_csv(os.path.join(DATA_PROCESSED, DATA_CSV))
# variable pour choisir son departement
DEPARTEMENT = sys.argv[1]
df = df[df['code_departement'] == DEPARTEMENT]


# In[49]:


# fonction pour filtrer le dataset en fonction du code departement
def filter_df_by_code_departement(code_departement):
    
    df.to_csv(os.path.join(DATA_PROCESSED, f'enedis_2022_{code_departement}.csv'), index=False)

    # fonction pour l'utilisation de l'api du gouv pour identifier identifiant ban par rapport a l'adresse, nom et code commune
    input_filepath = os.path.join(DATA_PROCESSED, f'enedis_2022_{code_departement}.csv')
    url_csv = 'https://api-adresse.data.gouv.fr/search/csv/'
    data = {'result_columns': ['result_id', 'result_score'],
            'columns': ['adresse', 'nom_commune'],
            'citycode': 'code_commune'}

    def get_ban_id_from_csv(input_filepath,
                            output_filepath="temp.csv",
                            url=url_csv,
                            data=data):

        with open(input_filepath, "rb") as csv_file:
            files = {'data': csv_file}
            r = requests.post(url, files=files, data=data)
            pd.read_csv(BytesIO(r.content)).to_csv(output_filepath, index=False)
    get_ban_id_from_csv(input_filepath)

    # importation du fichier temp.csv pour filter sur la fiabilitée de la correspondance
    df_clean_ban_formated = pd.read_csv('temp.csv')
    threshold = 0.75
    df_clean_ban_formated = df_clean_ban_formated[df_clean_ban_formated['result_score'] >= threshold]

    # creation du csv
    df_clean_ban_formated.to_csv(os.path.join(DATA_PROCESSED, f'enedis_2022_{code_departement}.csv'), index=False)

    # nettoyage du fichier temp
    file_path = 'temp.csv'
    if os.path.exists(file_path):
        os.remove(file_path)
    else:
        print(f"Le fichier {file_path} n'existe pas.")

    # Compte rendu
    print(f"Concordance avec {threshold} de score: {df_clean_ban_formated['result_id'].count()}")
    print(f"Nombre d'adresse unique: {df_clean_ban_formated['result_id'].nunique()}")
    # renommage des columns
    df_clean_ban_formated.rename(columns={'result_id': 'Identifiant__BAN'}, inplace=True)
    # garder les columns utile
    df_clean_ban_formated = df_clean_ban_formated[['annee', 'nombre_de_logements', 'consommation_annuelle_totale_de_l_adresse_mwh', 'consommation_annuelle_moyenne_par_site_de_l_adresse_mwh', 'consommation_annuelle_moyenne_de_la_commune_mwh', 'Identifiant__BAN']]
    df_clean_ban_formated.reset_index(drop=True, inplace=True)
    # export enedis 13 final
    df_clean_ban_formated.to_csv(os.path.join(DATA_PROCESSED, f'enedis_2022_{code_departement}.csv'), index=False)

    return f'Fichier csv "enedis_2022_{code_departement}.csv" créé'


# In[51]:


# Filter la database d'enedis base sur le code_departement
#%%time
filter_df_by_code_departement(DEPARTEMENT)


# In[53]:


#enedis = pd.read_csv(os.path.join(DATA_PROCESSED, f'enedis_2022_{DEPARTEMENT}.csv'))
#enedis

