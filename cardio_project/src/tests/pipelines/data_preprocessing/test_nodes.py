from cardio_again.pipelines.data_preprocessing.nodes import _replace_zero_values_for_columns, _separate_male_female, _replace_int_with_float, _divide_age_interval, _calculate_median_for_columns
import pandas as pd

def test_separate_male_female(heart_df):
    """
    Test function for the `_separate_male_female` function.

    This function takes a DataFrame containing records of individuals with a 'Sex' column,
    calls the `_separate_male_female` function to separate male and female records,
    and then verifies the correctness of the separation.

    Args:
        heart_df (pandas.DataFrame): DataFrame containing records of individuals with a 'Sex' column.

    Raises:
        AssertionError: If the total number of records is not preserved after separation,
                        or if any record in the male DataFrame is not male,
                        or if any record in the female DataFrame is not female.
    """
    # Call the function to separate male and female records
    male_df, female_df = _separate_male_female(heart_df)

    # Verify that the total number of records is preserved
    assert male_df.shape[0] + female_df.shape[0] == heart_df.shape[0]

    # Verify that all records in the male DataFrame are male
    assert all(male_df["Sex"] == "M")

    # Verify that all records in the female DataFrame are female
    assert all(female_df["Sex"] == "F")

def test_replace_int_with_float(heart_df):
    """
    Test function for the `_replace_int_with_float` function.

    This function tests whether integer columns in a DataFrame are replaced with float columns.

    Args:
        heart_df (pandas.DataFrame): DataFrame containing columns to be checked for integer-to-float replacement.

    Raises:
        AssertionError: If any column in the DataFrame is not replaced with float numbers.
    """

    # Get the columns that were originally of integer type
    int_columns = heart_df.select_dtypes(include='int64').columns

    # Call the function to replace integer columns with float columns
    df_float = _replace_int_with_float(heart_df)

    # Verify that only the integer columns were replaced with float columns
    for column in int_columns:
        assert df_float[column].dtype == 'float64'


def test_divide_age_interval(heart_df, params):
    """
    Test function for the `_divide_age_interval` function.

    This function tests whether the age interval is correctly divided into the specified number of subintervals.

    Args:
        heart_df (pandas.DataFrame): DataFrame containing age data to be divided into subintervals.
        params (dict): Dictionary containing parameters for dividing the age interval, including:
            - num_intervals (int): The number of subintervals to divide the age range into.

    Raises:
        AssertionError: If the returned subintervals do not match the expected subintervals.
    """
    # Define the expected subintervals
    expected_subintervals = [(28, 34), (35, 41), (42, 48), (49, 55), (56, 62), (63, 69), (70, 77)]

    # Call the function to divide the age interval
    subintervals = _divide_age_interval(heart_df, params["num_intervals"])

    # Check if the returned subintervals match the expected subintervals
    assert subintervals == expected_subintervals


def test_calculate_median_for_columns():

    # Built-in DataFrame for testing
    test_df = pd.DataFrame({
        "Age": [25, 30, 35, 40, 45, 50, 55],
        "Weight": [70, 75, 80, 85, 90, 95, 100],
        "Height": [160, 165, 170, 175, 180, 185, 190]
    })

    columns = ["Weight", "Height"]
    age_range = (30, 50)

    # Calculate median values using the function with the built-in DataFrame
    test_result = _calculate_median_for_columns(test_df, columns, age_range)

    # Expected median values for test DataFrame
    expected_test_result = pd.Series({"Weight": 85.0, "Height": 175.0})

    # Verify the result
    assert test_result.equals(expected_test_result)

def test_replace_zero_values_for_columns():
    # Test DataFrame with zero values
    test_df = pd.DataFrame({
        "Age": [25, 30, 35, 40, 45, 50, 55],
        "Weight": [70, 75, 0, 85, 90, 0, 100],
        "Height": [160, 165, 170, 0, 180, 185, 0]
    })

    # Median values calculated for the specified columns and age range
    median_values = pd.Series({"Weight": 85.0, "Height": 175.0})

    columns = ["Weight", "Height"]
    age_range = (30, 50)

    # Run the function with the test DataFrame and median values
    result_df = _replace_zero_values_for_columns(test_df.copy(), columns, age_range, median_values)
    print(result_df)

    # Expected DataFrame after replacing zero values
    expected_result_df = pd.DataFrame({
        "Age": [25, 30, 35, 40, 45, 50, 55],
        "Weight": [70, 75, 85, 85, 90, 85, 100],
        "Height": [160, 165, 170, 175, 180, 185, 0]
    })

    # Verify the result
    pd.testing.assert_frame_equal(result_df, expected_result_df)