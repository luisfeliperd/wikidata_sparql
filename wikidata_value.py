#!/usr/bin/env python
# coding: utf-8

#Required Packages
import requests
import pandas as pd
import time
from collections import defaultdict
from fuzzywuzzy import fuzz
labels_dict = defaultdict(list)

#API Endpoint
api_url = 'https://query.wikidata.org/sparql'

#Values to reconcile database
tabela_df = pd.read_csv('planilha_teste.csv', encoding='utf-8')

value = 'Karajá'

query = """ SELECT DISTINCT ?sujeito ?sujeitoLabel ?sujeitoAltLabel WHERE {
                          
                          {?sujeito ?label "%s".}
                          UNION
                          {?sujeito ?label "%s"@en.}
                          UNION
                          {?sujeito ?label "%s"@pt-br.}
                                                    
                          ?sujeito wdt:P31 ?instance.
                          
                          SERVICE wikibase:label { bd:serviceParam wikibase:language "pt-br", "pt", "en". }
                        }"""%(value, value, value)

r = requests.get(api_url, params = {'format': 'json', 'query': query})
data = r.json()

#Faz um dicionário com lista das labels para cada resultado retornado
for item in data['results']['bindings']:
    labels_dict[item['sujeito']['value']] = item['sujeitoAltLabel']['value'].split(',')
    labels_dict[item['sujeito']['value']].append(item['sujeitoLabel']['value'])


#Limpar Repetições da lista de labels
for key in labels_dict.keys():
    labels_dict[key] = list(set(labels_dict[key]))

#Avaliar o score do match entre o valor procurado com o valor procurado
for key in labels_dict.keys():
    for item in labels_dict[key]:
        print("Valor:", value, "|| Objeto", key, "|| Label:", item, "|| Socre:", fuzz.token_set_ratio(value, item))
