import pytz
import requests
import numpy as np
import pandas as pd
from tqdm import tqdm
from datetime import datetime

access_token          = '8oOoodx60Ro01ZsrSPmxahoX49PvEw9qonAMJWnjgCXGEA' # i get this token from network tab of inspect element
number_previous_until = 100                                              # number of preovious campaign until current campaign
df                    = pd.DataFrame()

# get current flash sale
current_campaign_id = requests.get(
    url='https://api.bukalapak.com/_exclusive/flash-deals',
    params= {
        'access_token': access_token
    }
).json()['data']['active']['id']

# loop previous current_campaign until number_previous_until previous campaign
for campaign_id in tqdm(range(current_campaign_id-1, current_campaign_id-number_previous_until-1, -1)):
    print('campaign id: ', campaign_id)

    # get start time and end time of campaign
    campaign = requests.get(
        url    = f'https://api.bukalapak.com/_exclusive/flash-deals/campaigns/{campaign_id}',
        params = {
            'access_token': access_token
        }
    ).json().get('data')
    
    # if campaign is not found or have not ended (possible to get next campaign although the campaign id is lower then current campaign id) then run to next loop
    if campaign is None or (datetime.now(tz=pytz.timezone('Asia/Jakarta')) - pd.Timestamp(campaign['end_time']).to_pydatetime()).days < 0:
        continue

    # get all of item summaries based on current flash sale id
    item_summaries = requests.get(
        url=f'https://api.bukalapak.com/_exclusive/flash-deals/campaigns/{campaign_id}/products/ids',
        params= {
            'access_token': access_token
        }
    ).json()['data']

    # get all of items
    items = requests.post(
        url='https://api.bukalapak.com/aggregate',
        params= {
            'access_token': access_token
        },
        json = {
            'aggregate': {
                item['product_id']: {
                    'method': 'GET',
                    'path'  : f'/_exclusive/flash-deals/simplified-products/{item["product_id"]}'
                } for item in item_summaries
            }
        }
    ).json()['data']
    items = list(items.values())

    # convert to dataframe
    df_items = pd.DataFrame(items)
    df_items['campaign_start_time'] = campaign['start_time']
    df_items['campaign_end_time']   = campaign['end_time']
    df_items.drop(columns=['images'], inplace=True, errors='ignore') # dropped because i think we don't need images data

    # append to main df
    df = df.append(df_items, ignore_index=False)

# save to csv
df.to_json('results/flash deal ({} - {}).json'.format(df.campaign_start_time.min(), df.campaign_end_time.max()), orient='records', indent=4)

# for extra insight, i make a list that contain number of category that sold at least 75% (ceil) of stock 
df[df.current_stock <= np.ceil(df.stock * 25/100)] \
    .category \
    .apply(lambda c: c['structure'][-1]) \
    .value_counts() \
    .reset_index() \
    .to_csv('results/top category ({} - {}).csv'.format(df.campaign_start_time.min(), df.campaign_end_time.max()), index=False, header=['detail_category', 'total'])