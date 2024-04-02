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