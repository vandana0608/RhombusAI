import pandas as pd
import numpy as np
from dateutil.parser import parse
from tqdm import tqdm
import re
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def is_date(string):
    try:
        parse(string, fuzzy=True)
        return True
    except (ValueError, TypeError, OverflowError):
        return False

def is_numeric(value):
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False

def infer_and_convert_data_types(df, date_columns=None, categorical_threshold=0.5, numeric_threshold=0.8):
    logging.info("Inferring and converting data types...")
    
    for col in tqdm(df.columns, desc="Processing columns"):
        if df[col].dtype != 'object':
            continue
        
        non_null_values = df[col].dropna()
        
        if len(non_null_values) == 0:
            logging.warning(f"Column '{col}' is empty. Skipping.")
            continue
        
        if date_columns and col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            continue
        
        sample_values = non_null_values.sample(min(1000, len(non_null_values))).tolist()
        
        # Check for numeric values
        numeric_ratio = sum(is_numeric(x) for x in sample_values) / len(sample_values)
        if numeric_ratio >= numeric_threshold:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            continue
        
        # Check for boolean values
        if set(sample_values) <= {'True', 'False', True, False, 1, 0, '1', '0', 'Yes', 'No', 'Y', 'N'}:
            df[col] = df[col].map({'True': True, 'False': False, '1': True, '0': False, 
                                   1: True, 0: False, 'Yes': True, 'No': False, 'Y': True, 'N': False})
            df[col] = df[col].astype('boolean')
            continue
        
        # Check for date values
        date_ratio = sum(is_date(str(x)) for x in sample_values) / len(sample_values)
        if date_ratio >= 0.8:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            continue
        
        # Check if the column should be categorical
        unique_ratio = len(df[col].unique()) / len(df[col])
        if unique_ratio < categorical_threshold:
            df[col] = pd.Categorical(df[col])
            continue
        
        # If none of the above, leave as object type
        logging.info(f"Column '{col}' remains as object type.")
    
    return df

def optimize_dtypes(df):
    logging.info("Optimizing data types...")
    
    for col in tqdm(df.columns, desc="Optimizing columns"):
        if df[col].dtype == 'int64':
            df[col] = pd.to_numeric(df[col], downcast='integer')
        elif df[col].dtype == 'float64':
            df[col] = pd.to_numeric(df[col], downcast='float')
    
    return df

def handle_mixed_numeric(df):
    logging.info("Handling mixed numeric columns...")
    
    for col in df.columns:
        if df[col].dtype == 'object':
            numeric_mask = df[col].apply(is_numeric)
            if numeric_mask.any() and not numeric_mask.all():
                numeric_col = pd.to_numeric(df[col], errors='coerce')
                non_numeric_mask = numeric_col.isna() & ~df[col].isna()
                df[f'{col}_numeric'] = numeric_col
                df[f'{col}_non_numeric'] = df[col].where(non_numeric_mask, None)
                df = df.drop(columns=[col])
                logging.info(f"Split column '{col}' into numeric and non-numeric parts.")
    
    return df

def process_file(file_path, date_columns=None, categorical_threshold=0.5, numeric_threshold=0.8):
    logging.info(f"Processing file: {file_path}")
    
    try:
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path, low_memory=False)
        elif file_path.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(file_path)
        else:
            raise ValueError("Unsupported file format. Please use CSV or Excel files.")
    except Exception as e:
        logging.error(f"Error reading file: {e}")
        return None
    
    logging.info("Initial data types:")
    logging.info(df.dtypes)
    logging.info(f"Initial memory usage: {df.memory_usage(deep=True).sum() / 1e6:.2f} MB")
    
    df = infer_and_convert_data_types(df, date_columns, categorical_threshold, numeric_threshold)
    df = handle_mixed_numeric(df)
    df = optimize_dtypes(df)
    
    logging.info("Final data types:")
    logging.info(df.dtypes)
    logging.info(f"Final memory usage: {df.memory_usage(deep=True).sum() / 1e6:.2f} MB")
    
    return df

def get_column_stats(df):
    stats = {}
    for col in df.columns:
        col_stats = {
            'dtype': str(df[col].dtype),
            'unique_count': df[col].nunique(),
            'null_count': df[col].isnull().sum(),
        }
        if pd.api.types.is_numeric_dtype(df[col]):
            col_stats.update({
                'min': df[col].min(),
                'max': df[col].max(),
                'mean': df[col].mean(),
                'median': df[col].median(),
            })
        elif pd.api.types.is_string_dtype(df[col]):
            col_stats.update({
                'min_length': df[col].str.len().min(),
                'max_length': df[col].str.len().max(),
                'sample_values': df[col].sample(min(5, len(df[col]))).tolist(),
            })
        stats[col] = col_stats
    return stats