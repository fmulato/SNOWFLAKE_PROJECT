import os
import json
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
import kagglehub

# Download latest version
path = kagglehub.dataset_download("umuttuygurr/e-commerce-customer-behavior-and-sales-analysis-tr")

print("Path to dataset files:", path)