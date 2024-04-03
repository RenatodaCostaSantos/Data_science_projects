"""
This is a boilerplate pipeline 'data_preprocessing'
generated using Kedro 0.19.0
"""
import pandas as pd

def _separete_male_female(df: pd.DataFrame, string: pd.StringDtype) -> pd.DataFrame:
    sex_df = df[df['Sex'] == string]
    return sex_df

def replace_int_with_float(df):
    for col in df.columns:
        if df[col].dtype == 'int64':  # Check if column type is int
            df[col] = df[col].astype(float)  # Convert int to float
    return df

def _imput_median(list: pd.Series, df: pd.DataFrame,  sex: pd.StringDtype, median: pd.Float32Dtype,) -> pd.DataFrame:
    for age in list:
        try:
            df.loc[(df['Sex'] == sex) & (df['Age'] >= age) & (df['Age'] <= age + 7) & (df['Cholesterol'] == 0), 'Cholesterol'] = median
        except:
            pass

    return df

def _calculate_median_resting_bp(df: pd.DataFrame, age_range) -> float:
    """
    Calculate the median resting blood pressure for a given age range.
    
    Parameters:
        df (DataFrame): The DataFrame containing patient data.
        age_range (tuple): A tuple representing the age range as (start_age, end_age).
        
    Returns:
        float: The median resting blood pressure.
    """
    median = df.loc[(df['Age'] >= age_range[0]) & (df['Age'] <= age_range[1]) & (df['RestingBP'] != 0), 'RestingBP'].median()
    return median

def _replace_zero_resting_bp(df: pd.DataFrame, age_range: list, median: float) -> pd.DataFrame:
    """
    Replace zero 'RestingBP' values with the calculated median based on age range.
    
    Parameters:
        df (DataFrame): The DataFrame containing patient data.
        age_range (tuple): A tuple representing the age range as (start_age, end_age).
        median (float): The median resting blood pressure for the given age range.
    """
    df.loc[(df['Age'] >= age_range[0]) & (df['Age'] <= age_range[1]) & (df['RestingBP'] == 0), 'RestingBP'] = median
    return df

def _process_age_intervals(df: pd.DataFrame, age_intervals) -> pd.DataFrame:
    """
    Orchestrate the process by iterating through age intervals and calling the necessary functions.
    
    Parameters:
        df (DataFrame): The DataFrame containing patient data.
        age_intervals (list): A list of tuples representing age intervals as (start_age, end_age).
    """
    for age_range in age_intervals:
        median = _calculate_median_resting_bp(df, age_range)
        _replace_zero_resting_bp(df, age_range, median)