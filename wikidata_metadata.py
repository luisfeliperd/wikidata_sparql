#!/usr/bin/env python
# coding: utf-8

# # Script to map metadata to wikidata entities (instance of)

# This script uses SPARQL Wikidata API endpoint, and searchs the values of a database (csv file) to determine to wich instance this values belongs. This provide 3 possible wikidata entities that represents each metadatas/columns of the inputed database.



#Required Packages
import requests
import pandas as pd
import time


#API Endpoint
api_url = 'https://query.wikidata.org/sparql'

#Values to reconcile database
tabela_df = pd.read_csv('planilha_teste.csv', encoding='utf-8')


def reconcile_database(database):
    
    #Dataframe to store results
    result_df = pd.DataFrame(columns = ['column','value','qid','qid_instance','instance_label'])

    #For each metadata/column on values to reconcile database
    for column in database.columns:
        print("Trabalhando no metadado ", column)
        
        #For each value of a column/metadata
        for value in tabela_df[column]:
            print("Procurando Resultados para ", value)
            
            #SPARQL query. Returns subject QID, instance QID and instance label, searching the value for subjects
            #with matching labels.
            query = """ SELECT DISTINCT ?sujeito ?instancia_de_que ?instancia_de_queLabel WHERE {
                          ?sujeito ?label "%s".
                          ?sujeito wdt:P31 ?instancia_de_que.
                          FILTER(?instancia_de_que NOT IN(wd:Q4167836, wd:Q4167410))
                          SERVICE wikibase:label { bd:serviceParam wikibase:language "pt-br", "pt", "en". }
                        }""" %(value)
            
            #Request the query to wikidata api and outputs a json.
            r = requests.get(api_url, params = {'format': 'json', 'query': query})
            data = r.json()

            #For each result inset data on result_df
            for item in data['results']['bindings']:
                result_df = result_df.append({'column':column,'value':value,
                                            'qid':item['sujeito']['value'],
                                            'qid_instancia':item['instancia']['value'], 
                                            'instance_label':item['instanciaLabel']['value'] },
                                             ignore_index=True)
                    
            time.sleep(1)
                    
    return result_df

print(reconcile_database(tabela_df).to_csv("resultado.csv", encoding='utf-8'))
