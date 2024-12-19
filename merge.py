# python script to consolidate several .csv files into one.

# grab all csv files names 20xx-xx-xx-listings.csv and append them into a single pandas dataframe
import glob

import pandas as pd

files = glob.glob('*-listings.csv')

dtypes = {
        'id':'int64',
        'name':'object',
        'host_id':'int64',
        'host_name':'object',
        'neighbourhood_group':'float64',
        'neighbourhood':'object',
        'latitude':'float64',
        'longitude':'float64',
        'room_type':'object',
        'price':'float64',
        'minimum_nights':'int64',
        'number_of_reviews':'int64',
        'last_review':'object',
        'reviews_per_month':'float64',
        'calculated_host_listings_count':'int64',
        'availability_365':'int64',
        'number_of_reviews_ltm':'int64',
        'license':'object',
        'date':'datetime64[ns]'
    }

# read all csv files into a single pandas dataframe
data_list = [pd.read_csv(f, dtype=dtypes).assign(date=pd.to_datetime(f[:10])) for f in files]
data = pd.concat(data_list, ignore_index=True)
data= data.drop(columns=['neighbourhood_group','license'])  # estan vacias o tienen fruta...

dates = data['date'].unique()
print(dates)

grouped = data.groupby('id').size().reset_index(name='count')
# print('grouped by id\n', grouped)

# filter the records where the number of unique dates is equal to the number of dates in the set
complete = grouped[grouped['count'] == len(dates)]
# print('grouped["count" == len(dates)\n', complete)

# get the 'id' values where all dates are present
complete_ids = complete['id']
# print('ids de "complete"\n', complete_ids)

# filter the records where 'id' is in the list of 'id' values where all dates are present
data = data[data['id'].isin(complete_ids)]
print('data filtrada\n', data)

print(data.shape)

print('recs en original', sum([d.shape[0] for d in data_list]))


data.to_csv('listings.csv', index=False)
