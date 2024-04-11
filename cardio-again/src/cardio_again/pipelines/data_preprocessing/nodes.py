"""
This is a boilerplate pipeline 'data_preprocessing'
generated using Kedro 0.19.0
"""
from sklearn.preprocessing import MinMaxScaler
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
    int_columns = df.select_dtypes(include='int64').columns
    df.loc[:, int_columns] = df.loc[:, int_columns].astype(float)
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


def input_data_males(df: pd.DataFrame, num_intervals: int, columns_strange_zeros: list):

    male = _separate_male_female(df)[0]
    male = _replace_int_with_float(male) 
    age_intervals = _divide_age_interval(male, num_intervals)
    male = _process_age_intervals_for_columns(male, columns_strange_zeros, age_intervals)

    return male


def input_data_females(df: pd.DataFrame, num_intervals: int, columns_strange_zeros: list):

    female = _separate_male_female(df)[1]
    female = _replace_int_with_float(female) 
    age_intervals = _divide_age_interval(female, num_intervals)
    female = _process_age_intervals_for_columns(female,columns_strange_zeros, age_intervals)

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


def replace_binary_strings_with_float(df: pd.DataFrame, binary_columns: pd.Series) -> pd.DataFrame:
    """
    Replace binary string values in a DataFrame column with float numbers.

    Args:
        df: Input DataFrame with binary string values.

    Returns:
        pd.DataFrame: DataFrame with binary strings replaced with float numbers.
    """
    # Define mapping dictionary for binary strings to float numbers
    binary_mapping = {'Y': 1.0, 'N': 0.0, 'M': 1.0, 'F': 0.0}  # You can extend this mapping as needed

    for column in binary_columns:
        # Replace binary strings with float numbers
        df[column] = df[column].map(binary_mapping)

    return df


def scale_columns(data: pd.DataFrame, columns_to_rescale: list) -> pd.DataFrame:
    """
    Scale some numerical columns in a DataFrame using MinMaxScaler.

    Args:
        data: Input DataFrame to be scaled.
        columns_to_rescale: List of column names to be scaled.

    Returns:
        pd.DataFrame: Scaled DataFrame.
    """
    # Initialize MinMaxScaler
    scaler = MinMaxScaler()

    # Fit scaler to the data and transform it
    scaled_data = scaler.fit_transform(data[columns_to_rescale])

    # Convert the scaled array back to a DataFrame
    scaled_df = pd.DataFrame(scaled_data, columns=columns_to_rescale)

    # Concatenate the scaled columns with the non-scaled columns
    non_scaled_columns = data.drop(columns=columns_to_rescale)
    scaled_df = pd.concat([scaled_df, non_scaled_columns], axis=1)

    return scaled_df