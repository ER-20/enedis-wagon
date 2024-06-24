#!/usr/bin/env python
# coding: utf-8

# # imports, datasets, configurations et ressources
# defis:
# https://defis.data.gouv.fr/defis/65b76f15d7874915c8e41298/
# dataset enedis:
# https://defis.data.gouv.fr/datasets/60cac8db0480dab2ce2fcb3b
# docs API:
# https://adresse.data.gouv.fr/api-doc/adresse -> (passé en csv pour attenuer les limites/lenteurs du service)
# https://data.ademe.fr/datasets/dpe-v2-logements-existants/api-doc
# test API accès direct:
# https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-logements-existants/lines?q=95229_0414_00001
# visualisation dataset:
# https://data.ademe.fr/datasets/dpe-v2-logements-existants/full?display=table&sort=Code_INSEE_%28BAN%29%3A1&Date_r%C3%A9ception_DPE_interval=%222022-01-01%22,%222022-12-31%22&cols=Etiquette_DPE,P%C3%A9riode_construction,Surface_habitable_logement,Classe_inertie_b%C3%A2timent,Identifiant__BAN,N%C2%B0_r%C3%A9gion_%28BAN%29,Co%C3%BBt_total_5_usages,Type_%C3%A9nergie_n%C2%B01,Conso_5_usages_%C3%A9_primaire,Conso_5_usages_par_m%C2%B2_%C3%A9_primaire,Conso_chauffage_%C3%A9_primaire,Co%C3%BBt_total_5_usages_%C3%A9nergie_n%C2%B01

# import libraries
import pandas as pd
import numpy as np
import os
import requests
import sys
from io import BytesIO
# chemin des dossiers de travail
DATA_PATH = '../data/raw/'
DATA_PROCESSED = '../data/processed/'
DATA_CSV = 'enedis_2022.csv'
# import du csv enedis
df = pd.read_csv(os.path.join(DATA_PROCESSED, DATA_CSV))
# creation de la liste des departement pour automatiser nos fonctions
LIST_DEP = df['code_departement'].unique().astype(int)
#LIST_DEP = [ 48, 9]
# # fonction: update enedis, get identifiant BAN (API data.gouv)
# fonction pour filtrer le dataset en fonction du code departement
def filter_df_by_code_departement(code_departement):
    # filtre sur le code departement
    df[df['code_departement'] == code_departement].to_csv(os.path.join(DATA_PROCESSED, f'enedis_2022_{code_departement}.csv'), index=False)
    # def chemin et adresse, nom et code commune
    input_filepath = os.path.join(DATA_PROCESSED, f'enedis_2022_{code_departement}.csv')
    url_csv = 'https://api-adresse.data.gouv.fr/search/csv/'
    data = {'result_columns': ['result_id', 'result_score'],
            'columns': ['adresse', 'nom_commune'],
            'citycode': 'code_commune'}
    # fonction pour l'utilisation de l'api
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
    # compte rendu
    print("----------------Identifiant BAN----------------")
    print(f"Concordance avec {threshold} de score: {df_clean_ban_formated['result_id'].count()}")
    print(f"Nombre d'adresse unique: {df_clean_ban_formated['result_id'].nunique()}")
    # renommage des columns
    df_clean_ban_formated.rename(columns={'result_id': 'Identifiant__BAN'}, inplace=True)
    # garder les columns utile
    df_clean_ban_formated = df_clean_ban_formated[['nombre_de_logements', 'consommation_annuelle_totale_de_l_adresse_mwh', 'consommation_annuelle_moyenne_par_site_de_l_adresse_mwh', 'consommation_annuelle_moyenne_de_la_commune_mwh', 'Identifiant__BAN']]
    df_clean_ban_formated.reset_index(drop=True, inplace=True)
    # export csv enedis avec identifiant ban par departement
    df_clean_ban_formated.to_csv(os.path.join(DATA_PROCESSED, f'enedis_2022_{code_departement}.csv'), index=False)  
    return print(f'Fichier csv "enedis_2022_{code_departement}.csv" créé')

# # fonction: update logements existants get DPE (average) (API ademe)
# fonction qui recupere la moyenne des DPE en fonction d'un identifiant BAN
def get_building_dpe(building_id):
    # initialise la liste de resultat
    list_dpe=[] 
    # Set base URL pour l API ADEME DPE
    base_url = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-logements-existants/lines"  
    # Set the query parametres pour l API request
    params = {"q": building_id,
              "q_fields": "Identifiant__BAN",
              "select": "Etiquette_DPE",
              "size": 10000}
    # Envoie API request et get the response
    response = requests.get(base_url, params=params)
    # Check le status code de la response
    if response.status_code == 200:
        # si request successful, extract le DPE label a partir de response data
        data = response.json()
        if data['results']:
          for i in range(0,len(data["results"])):
            # enregistre le result de la requete pour lenregistrement des colums de LIST_COL
            #result = data["results"][i]
            if data["results"][i]["Etiquette_DPE"]:
                # append le result avec selection des champs de 
                #list_dpe.extend([result.get(field) for field in LIST_COL])
                # list_dpe.append(data["results"][i]["Etiquette_DPE"])
                list_dpe += [data["results"][i]["Etiquette_DPE"]]
                # print('test', nbr_dep, flush=True)
            else:
                print(None)
          return list_dpe
    else:
        # Si request error
        raise Exception(f"Error: {response.status_code}")

# convertir la mapping
def convertir(list_antoine_dpe):
    # Dictionnaire pour convertir les lettres en chiffres
    dpe_to_num = {'A': 35, 'B': 90, 'C': 145, 'D': 215, 'E': 290, 'F': 375, 'G': 460}
    # Dictionnaire pour convertir les chiffres en lettres
    num_to_dpe = {1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E', 6: 'F', 7: 'G'}
    # Convertir chaque DPE en chiffre
    num_list = [dpe_to_num[dpe] for dpe in list_antoine_dpe]
    # Calculer la moyenne
    moyenne = sum(num_list) / len(num_list)
    # Arrondir la moyenne à l'entier le plus proche
    moyenne_arrondie = round(moyenne)


    # Convertir le score moyen en lettre
    score_dpe = num_to_dpe[moyenne_arrondie]
    # classe DPE par intervale

    return score_dpe
    
# calcul de la moyenne DPE
def get_average_dpe(iban):
    # Create an empty list to store the DPE labels
    dpe_labels = []
    # Iterate over the BAN IDs in the DataFrame
    for ban in iban:
        # Get the DPE label for the current BAN ID
        dpe = get_building_dpe(ban)
        # Add the DPE label to the list
        if dpe:
          #dpe_labels.append(convertir(dpe))
          dpe_labels.append(dpe)
        else:
          dpe_labels.append(None)
    # Add the DPE labels to the DataFrame
    df_ban["LIST_CLASS_DPE"] = dpe_labels
    return dpe_labels

# # execution
# filter la database d'enedis base sur le code_departement
nbr_dep = 0
for dep in LIST_DEP:
    nbr_dep += 1
    if os.path.isfile(os.path.join(DATA_PROCESSED, f'enedis_2022_{dep}_dpe.csv')):
        print(f'"enedis_2022_{dep}_dpe.csv" existe deja')
    else:
        print(f"###############################################")
        print (f"-------------------{nbr_dep}/{len(LIST_DEP)}------------------------")
        print(f"###############################################")
        print(f"----------------Département {dep}-----------------")
        
        filter_df_by_code_departement(dep)
        df_ban = pd.read_csv(os.path.join(DATA_PROCESSED, f'enedis_2022_{dep}.csv'))
        # get average function v1
        get_average_dpe(df_ban['Identifiant__BAN'])
        # compte rendu
        print("-------------------DPE-------------------------")
        print(f"Nombre identifiant ban traité: {len(df_ban)}")
        # export csv enedis avec identifiant ban par departement
        df_ban.to_csv(os.path.join(DATA_PROCESSED, f'enedis_2022_{dep}_dpe.csv'), index=False)
        print(f'Fichier csv "enedis_2022_{dep}_dpe.csv" créé')
        # nettoyage du fichier intermediaire
        file_path = os.path.join(DATA_PROCESSED, f'enedis_2022_{dep}.csv')
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f'Fichier csv intermediaire "enedis_2022_{dep}.csv" supprimé.')
        else:
            print(f"Le fichier {file_path} n'existe pas.")
print("Fin d'execution!")

# # concat de tous les departements
# créer un DataFrame vide avec les noms de colonnes
dff = pd.DataFrame(columns=['nombre_de_logements', 'consommation_annuelle_totale_de_l_adresse_mwh', 'consommation_annuelle_moyenne_par_site_de_l_adresse_mwh', 'consommation_annuelle_moyenne_de_la_commune_mwh', 'Identifiant__BAN'])
# Exclure les colonnes vides ou entièrement NA (evite le warning de deprecated)
dff = dff.dropna(axis=1, how='all')
# Parcourir la liste des départements
for dep in LIST_DEP:
    # Lire le fichier CSV correspondant au département
    df_dep = pd.read_csv(os.path.join(DATA_PROCESSED, f'enedis_2022_{dep}_dpe.csv'))
    # Ajouter les données du département au DataFrame final
    dff = pd.concat([dff, df_dep], ignore_index=True)
# Enregistrer le DataFrame final dans un fichier CSV
dff.to_csv(os.path.join(DATA_PROCESSED, 'enedis_2022_final.csv'), index=False)
print("enedis_2022_final.csv généré, Fin!")