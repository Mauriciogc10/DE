# script api.py get_most_relevant_items_for_category
# consulta api para consultar api de mercado libre

import requests
import json
import datetime

DATE=str(datetime.date.today()).replace('-','')

def get_most_relevant_items_for_category(category):
    url = f"https://api.mercadolibre.com/sites/MLA/search?category={category}&json"
    response = requests.get(url).text
    data = json.loads(response)
    response_json = data['results']

with open('/Users/tatianasorochte/airflow-docker/tmp/file.tsv') as file:
    for item = data
        _id = getKeyFromItem(item, '_id')
        site_id = getKeyFromItem(item, 'site_id')
        title = getKeyFromItem(item, 'title')
        price = getKeyFromItem(item, 'price')
        sold_quantity = getKeyFromItem(item, 'sold_quantity')
        thumbnail = getKeyFromItem(item, 'thumbnail')


        print(f"{_id} {title} {price} {sold_quantity} {thumbnail}")
        file.write(f"{_id}\t{site_id}\t{title}\t{price}\t{sold_quantity} \t{thumbnail}\t{DATE}\n")

def getKeyFromItem(item, key):
    return str(item[key]).replace("'", '').strip() if item.get(key) else "null"


