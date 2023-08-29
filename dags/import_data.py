import json

import kaggle
import os

# Load credentials from kaggle.json
with open('./kaggle.json', 'r') as f:
    config = json.load(f)

os.environ['KAGGLE_USERNAME'] = config['username']
os.environ['KAGGLE_KEY'] = config['key']


def download_kaggle_dataset():

    
    os.environ['KAGGLE_CONFIG_DIR'] = 'demo_dashboard\kaggle.json'

    kaggle.api.authenticate()
    kaggle.api.dataset_download_files('olistbr/brazilian-ecommerce', ' . ', unzip=True)

    return
# Example usage:
# download_kaggle_dataset('olistbr/brazilian-ecommerce', '/desired_path/', '/path_to_directory_containing_kaggle.json/')


download_kaggle_dataset()