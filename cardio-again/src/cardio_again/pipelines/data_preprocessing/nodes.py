"""
This is a boilerplate pipeline 'data_preprocessing'
generated using Kedro 0.19.0
"""

import pandas as pd


def _separate_male_female(df: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
    """
    Separate male and female records from the input DataFrame.

    Args:
        df: Input DataFrame with 'Sex' column.

    Returns:
        Tuple containing two DataFrames, one for males and one for females.
    """
    sex = ["M", "F"]
    male = df[df["Sex"] == sex[0]]
    female = df[df["Sex"] == sex[1]]
    return male, female


def _replace_int_with_float(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace integer columns with float columns in a DataFrame.

    Args:
        df: DataFrame.

    Returns:
        DataFrame with float numbers for columns with integer numbers.
    """
    for col in df.columns:
        if df[col].dtype == "int64":  # Check if column type is int
            df[col] = df[col].astype(float)  # Convert int to float

    return df


def _divide_age_interval(df: pd.DataFrame, num_intervals: int) -> list:
    """
    Divide the interval between the minimum and maximum ages into the specified number of subintervals.men_list

    Parameters:
        min_age (int): The minimum age value.
        max_age (int): The maximum age value.
        num_intervals (int): The number of subintervals to divide the age interval into.

    Returns:
        list: A list of tuples representing the subintervals.
    """
    min_age = df["Age"].min()

    max_age = df["Age"].max()

    interval_size = (max_age - min_age) / num_intervals
    subintervals = []
    for i in range(num_intervals):
        start_age = int(min_age + i * interval_size)
        end_age = int(min_age + (i + 1) * interval_size - 1)
        subintervals.append((start_age, end_age))
    # Ensure the last subinterval ends at the maximum age
    subintervals[-1] = (subintervals[-1][0], max_age)
    return subintervals


def _calculate_median_for_columns(
    df: pd.DataFrame, columns: list, age_range: tuple
) -> pd.Series:
    """
    Calculate the median for specified columns within a given age range.

    Parameters:
        df (pd.DataFrame): The DataFrame containing patient data.
        columns (list): A list of column names for which to calculate the median.
        age_range (tuple): A tuple representing the age range as (start_age, end_age).

    Returns:
        pd.Series: A Series containing the median values for specified columns.
    """
    median_values = df.loc[
        (df["Age"] >= age_range[0]) & (df["Age"] <= age_range[1]), columns
    ].median()
    return median_values


def _replace_zero_values_for_columns(
    df: pd.DataFrame, columns: list, age_range: tuple, median_values: pd.Series
) -> pd.DataFrame:
    """
    Replace zero values in specified columns with the calculated medians based on age range.

    Parameters:
        df (pd.DataFrame): The DataFrame containing patient data.
        columns (list): A list of column names for which to replace zero values.
        age_range (tuple): A tuple representing the age range as (start_age, end_age).
        median_values (pd.Series): A Series containing the median values for specified columns.
    """
    for column in columns:
        df.loc[
            (df["Age"] >= age_range[0])
            & (df["Age"] <= age_range[1])
            & (df[column] == 0),
            column,
        ] = median_values[column]
    return df


def _process_age_intervals_for_columns(
    df: pd.DataFrame, columns: list, age_intervals: list
) -> pd.DataFrame:
    """
    Orchestrate the process by iterating through age intervals and calling the necessary functions for specified columns.

    Parameters:
        df (pd.DataFrame): The DataFrame containing patient data.
        columns (list): A list of column names for which to perform the imputation.
        age_intervals (list): A list of tuples representing age intervals as (start_age, end_age).
    """
    for age_range in age_intervals:
        median_values = _calculate_median_for_columns(df, columns, age_range)
        df = _replace_zero_values_for_columns(df, columns, age_range, median_values)
    return df


def input_data_males(df: pd.DataFrame):

    male = _separate_male_female(df)[0]
    male = _replace_int_with_float(male) 
    age_intervals = _divide_age_interval(male, num_intervals=7)
    male = _process_age_intervals_for_columns(male, ['RestingBP','Cholesterol'], age_intervals)

    return male


def input_data_females(df: pd.DataFrame):

    female = _separate_male_female(df)[1]
    female = _replace_int_with_float(female) 
    age_intervals = _divide_age_interval(female, num_intervals=7)
    female = _process_age_intervals_for_columns(female, ['RestingBP','Cholesterol'], age_intervals)

    return female


def concatenate_dfs(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """
    Concatenates two pandas DataFrames vertically.

    Args:
        df1: First DataFrame to concatenate.
        df2: Second DataFrame to concatenate.

    Returns:
        Concatenated DataFrame.
    """
    concatenated_df = pd.concat([df1, df2], ignore_index=True)
    return concatenated_df
