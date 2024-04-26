from cardio_again.pipelines.data_preprocessing.nodes import input_data_females, input_data_males, _process_age_intervals_for_columns, _replace_zero_values_for_columns, _separate_male_female, _replace_int_with_float, _divide_age_interval, _calculate_median_for_columns
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
    #age_range = (30, 50)
    age_range = (30, 45)

    # Calculate median values using the function with the built-in DataFrame
    test_result = _calculate_median_for_columns(test_df, columns, age_range)

    # Expected median values for test DataFrame
    #expected_test_result = pd.Series({"Weight": 85.0, "Height": 175.0})

    # Expected median values for test DataFrame
    expected_test_result = pd.Series({"Weight": 82.5, "Height": 172.5})

    # Verify the result
    assert test_result.equals(expected_test_result)

def test_replace_zero_values_for_columns():
    # Test DataFrame with zero values
    test_df = pd.DataFrame({
        "Age": [25, 30, 35, 40, 45, 50, 55],
        "Weight": [70, 75, 0, 85, 90, 0, 100],
        "Height": [160, 165, 170, 0, 180, 185, 0]
    })

    # Test 1
    # # Median values calculated for the specified columns and age range
    # median_values = pd.Series({"Weight": 85.0, "Height": 175.0})

    # columns = ["Weight", "Height"]
    # age_range = (30, 50)

    # Test 2
    # Median values calculated for the specified columns and age range
    median_values = pd.Series({"Weight": 85.0, "Height": 170.0})

    columns = ["Weight", "Height"]
    age_range = (25, 55)

    # Run the function with the test DataFrame and median values
    result_df = _replace_zero_values_for_columns(test_df.copy(), columns, age_range, median_values)
    print(result_df)

    # Expected test 1
    # # Expected DataFrame after replacing zero values
    # expected_result_df = pd.DataFrame({
    #     "Age": [25, 30, 35, 40, 45, 50, 55],
    #     "Weight": [70, 75, 85, 85, 90, 85, 100],
    #     "Height": [160, 165, 170, 175, 180, 185, 0]
    # })

    # Expected test 2
    # Expected DataFrame after replacing zero values
    expected_result_df = pd.DataFrame({
        "Age": [25, 30, 35, 40, 45, 50, 55],
        "Weight": [70, 75, 85, 85, 90, 85, 100],
        "Height": [160, 165, 170, 170, 180, 185, 170]
    })

    # Verify the result
    pd.testing.assert_frame_equal(result_df, expected_result_df)


def test_process_age_intervals_for_columns():
    # Test DataFrame with zero values
    test_df = pd.DataFrame({
        "Age": [25, 30, 35, 40, 45, 50, 55],
        "Weight": [70.0, 75.0, 0.0, 85.0, 90.0, 0.0, 100.0],
        "Height": [160.0, 165.0, 170.0, 0.0, 180.0, 185.0, 0.0]
    })

    columns = ["Weight", "Height"]
    age_intervals = [(25, 35), (36, 60)]

    # Run the function with the test DataFrame and age intervals
    result_df = _process_age_intervals_for_columns(test_df.copy(), columns, age_intervals)
    print(result_df)

    # Expected DataFrame after processing age intervals
    expected_result_df = pd.DataFrame({
        "Age": [25, 30, 35, 40, 45, 50, 55],
        "Weight": [70.0, 75.0, 70.0, 85.0, 90.0, 87.5, 100.0],
        "Height": [160.0, 165.0, 170.0, 90.0, 180.0, 185.0, 90.0]
    })

    # Verify the result
    pd.testing.assert_frame_equal(result_df, expected_result_df)


def test_input_data_males():

       # Create a test DataFrame
    data = pd.DataFrame({
        'Sex': ['M', 'M', 'M', 'F', 'M',"M"],
        'Age': [25, 30, 40, 35, 28, 45],
        'Column_A': [10, 0, 20, 0, 15, 30],
        'Column_B': [10, 15, 0, 25, 0, 35]
    })
    # Call the function
    num_intervals = 2
    columns_strange_zeros = ['Column_A', 'Column_B']
    result = input_data_males(data, num_intervals, columns_strange_zeros).reset_index(drop=True)
    print(result)
    
    # Expected DataFrame after processing
    expected_result = pd.DataFrame({
        'Sex': ['M', 'M', 'M', 'M', 'M'],
        'Age': [25.0, 30.0, 40.0, 28.0, 45.0],
        'Column_A': [10.0, 10.0, 20.0, 15.0, 30.0],  # Zeros replaced with median for male age groups
        'Column_B': [10.0, 15.0, 17.5, 10.0, 35.0]   # Zeros replaced with median for male age groups
    })
    print(expected_result)
    
    # Assert that the result matches the expected DataFrame
    pd.testing.assert_frame_equal(result, expected_result)



def test_input_data_females():

    # Create a test DataFrame
    data = pd.DataFrame({
        'Sex': ['F', 'F', 'F', 'M', 'F',"F"],
        'Age': [25, 30, 40, 35, 28, 45],
        'Column_A': [10, 0, 20, 0, 15, 30],
        'Column_B': [12, 15, 0, 25, 0, 35]
    })
    # Call the function
    num_intervals = 2
    columns_strange_zeros = ['Column_A', 'Column_B']
    result = input_data_females(data, num_intervals, columns_strange_zeros).reset_index(drop=True)
    print(result)
    
    # Expected DataFrame after processing
    expected_result = pd.DataFrame({
        'Sex': ['F', 'F', 'F','F',"F"],
        'Age': [25.0, 30.0, 40.0, 28.0, 45.0],
        'Column_A': [10.0, 10.0, 20.0, 15.0, 30.0],  # Zeros replaced with median for male age groups
        'Column_B': [12.0, 15.0, 17.5, 12.0, 35.0]   # Zeros replaced with median for male age groups
    })
    print(expected_result)
    
    # Assert that the result matches the expected DataFrame
    pd.testing.assert_frame_equal(result, expected_result)