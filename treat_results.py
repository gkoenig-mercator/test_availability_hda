import pandas as pd

dataset_wekeo = pd.read_csv('Datasets_availability.csv')
dataset_wekeo['provider'] = dataset_wekeo['Dataset_id'].str.extract(r'EO:(\w+)')

dataset_wekeo.groupby('provider')['Available'].value_counts(normalize=True)