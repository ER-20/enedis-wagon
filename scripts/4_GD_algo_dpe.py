#!/usr/bin/env python
# coding: utf-8

# import libraries
import pandas as pd
import numpy as np
import os

# chemin des dossiers de travail
DATA_PATH = '../data/raw/'
DATA_PROCESSED = '../data/processed/'
DATA_CSV = 'enedis_2022_final.csv'

DPE_TO_NUM = {'A': 35, 'B': 90, 'C': 145, 'D': 215, 'E': 290, 'F': 375, 'G': 460}
NUM_TO_DPE = {
    (0, 70): 'A',
    (70, 110): 'B',
    (110, 180): 'C',
    (180, 250): 'D',
    (250, 330): 'E',
    (330, 420): 'F',
    (420, 5000000000): 'G'
}
# import du csv
df = pd.read_csv(os.path.join(DATA_PROCESSED, DATA_CSV))

def moy_liste_de_lettres(valeur, DPE_TO_NUM=DPE_TO_NUM) -> int:
    if isinstance(valeur, str):
        # eval() -> converti en liste
        return int(np.mean([DPE_TO_NUM[lettre] for lettre in eval(valeur)])) # valeur = "[1, 2, 5]"

df["dpe_converti_conso_moyen"] = df["LIST_CLASS_DPE"].map(moy_liste_de_lettres)

def dpe_conso_to_letter(valeur, NUM_TO_DPE=NUM_TO_DPE) -> str:
    # Define the bins and labels using the NUM_TO_DPE dictionary
    bins = [num_range[0] for num_range in NUM_TO_DPE.keys()] + [float('inf')]
    labels = [dpe_score for dpe_score in NUM_TO_DPE.values()]
    # Use pd.cut() to bin the values into the appropriate DPE scores
    s_dpe = pd.cut(valeur, bins=bins, labels=labels, right=False)
    return s_dpe

df["dpe_calcule_sur_conso"] = dpe_conso_to_letter(df["dpe_converti_conso_moyen"])

df.to_csv(os.path.join(DATA_PROCESSED, 'enedis_2022_final_calculs.csv'), index=False)

"""
seq = [2, 4, 6, 8]
new = []
for el in seq:
    new.append(el**2)
new
"""

"""
seq = [2, 4, 6, 8]
# liste comprehensive
[el**3 for el in seq]
"""