#!/usr/bin/env python
# coding: utf-8

# import libraries
import pandas as pd
import numpy as np
import os

# chemin du dossier de travail
DATA_PATH = '../data/raw/'
DATA_PROCESSED = '../data/processed'

# FICHIER SOURCE A TELECHARGER ICI https://drive.google.com/file/d/1XtAdpekU4BzD_TKd5DcIbXBJY5xFYedH/view?usp=drive_link
DATA_CSV = 'consommation-annuelle-residentielle-par-adresse.csv'

# import du csv pour l'exploration
df = pd.read_csv(os.path.join(DATA_PATH, DATA_CSV), sep=';')

# ICI CHOISIR L ANNEE
df = df[df['annee'] == 2022]
# to_csv du pres clean
df.to_csv(os.path.join(DATA_PROCESSED, 'enedis_2022.csv'), index=False)

# import du csv pour l'exploration
df = pd.read_csv(os.path.join(DATA_PROCESSED, 'enedis_2022.csv'))
# Pour générer un dictionnaire de base à modifier
#{k : 'float32' for k in df.columns}
# convertir le type des columns
d = {
    #'annee': 'int64', # de 2018 a 2022
    #'code_iris': 'str', # inutile?
    #'nom_iris': 'str', # inutile?
    #'numero_de_voie': 'str', # redondance
    #'indice_de_repetition': 'str', # drop indice_de_repetition: 56080 / 57678
    #'type_de_voie': 'str', # redondance
    #'libelle_de_voie': 'str', # redondance
    #'code_commune': 'str',
    'nom_commune': 'category',
    #'segment_de_client': 'category', # inutile ils sont tous RESIDENTIEL
    'nombre_de_logements': 'int64',
    'consommation_annuelle_totale_de_l_adresse_mwh': 'float64',
    'consommation_annuelle_moyenne_par_site_de_l_adresse_mwh': 'float64',
    'consommation_annuelle_moyenne_de_la_commune_mwh': 'float64',
    'adresse': 'str',
    #'tri_des_adresses': 'int64', # inutile?
    #'code_epci': 'int64', # inutile?
    'code_departement': 'str', # redondance, mais a garder temporairement pour l'appel de l'api
    'code_region': 'str'  # inutile?  mais a garder temporairement pour l'appel de l'api
 }
df = pd.read_csv(os.path.join(DATA_PROCESSED, 'enedis_2022.csv'), usecols=d.keys(), dtype=d)

# to_csv du pres clean
df.to_csv(os.path.join(DATA_PROCESSED, 'enedis_2022.csv'), index=False)

# verification des null
df.isna().sum()

# to_csv
df.to_csv(os.path.join(DATA_PROCESSED, 'enedis_2022.csv'), index=False)