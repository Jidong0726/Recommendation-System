# -*- coding: utf-8 -*-
"""
Created on Thu Nov  7 10:22:02 2019

@author: jidong
"""
import pandas as pd
import json
import urllib.request as urllib2
import requests


def query_dataset_information(keyword,begin,limit):
    base = 'https://dataverse.harvard.edu'
    rows = 10
    start = begin
    page = 1
    condition = True
    table = pd.DataFrame(columns = ['name','url','doi_id', 'description','publish_date', 'author' ,'score'])
    while (condition):
        url = base + '/api/search?type=dataset&show_relevance=true&q='+ keyword + "&start=" + str(start) + "&per_page=10"
        data = json.load(urllib2.urlopen(url))
        total = data['data']['total_count']
        #print ("=== Page", page, "===")
        #print ("start:", start, " total:", total)
        for i in data['data']['items']:
            #authors = i.get('authors')
            #if len(authors)!=0:
             #   author = authors[0]
            #else:
             #   author = None
            tuples = [i.get('name'),i.get('url'),i.get('global_id'),i.get('description'),i.get('published_at'),i.get('authors'),i.get('score')]
            dic = dict(zip(table.columns, tuples))
            table = table.append(dic,ignore_index = True)
        start = start + rows
        page += 1
        condition = (start < total) and (start < limit) 
    return table


def from_doi_get_metrics(table):
    base = 'https://dataverse.harvard.edu'
    for index, row in table.iterrows():
        doi = row['doi_id']
        response = requests.get(base +'/api/datasets/:persistentId/makeDataCount/downloadsTotal?persistentId='+doi)
        print (response.content)
    return


if __name__ == "__main__":
    data = query_dataset_information('financials',20)
    #from_doi_get_metrics(data)