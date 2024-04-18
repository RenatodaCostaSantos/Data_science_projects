from kedro.framework.session import KedroSession
from cardio_again.pipelines.data_preprocessing.nodes import _separate_male_female, _replace_int_with_float, _divide_age_interval


def test_separate_male_female(heart_df):
    """
    Test function for the `_separate_male_female` function.

    This function loads a DataFrame containing records of individuals with a 'Sex' column,
    calls the `_separate_male_female` function to separate male and female records,
    and then verifies the correctness of the separation.

    Args:
        kedro_session: A Kedro session object used to access the Kedro project context
                       and load datasets.

    Raises:
        AssertionError: If the total number of records is not preserved after separation,
                        or if any record in the male DataFrame is not male,
                        or if any record in the female DataFrame is not female.
    """
    # # Load DataFrame from the catalog
    # df = kedro_session.load_context().catalog.load("heart")

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
        kedro_session: A Kedro session object used to access the Kedro project context
                       and load datasets.

    Raises:
        AssertionError: If any column in the DataFrame is not replaced with float numbers.
    """

    # Get the columns that were originally of integer type
    int_columns = heart_df.select_dtypes(include='int64').columns

    # Call the function to replace integer columns with float columns
    df_float = _replace_int_with_float(heart_df)

    # Verify that only the integer columns were replaced with float columns
    for column in int_columns:
        print(df_float[column].dtype)
        assert df_float[column].dtype == 'float64'


# def test_divide_age_interval(kedro_session, heart_df):
#         # Define the expected subintervals
#         expected_subintervals = [(28, 34), (35, 41), (42, 48), (49, 55), (56, 62), (63, 69), (70, 77)]

#         # Call the function to divide the age interval
#         subintervals = _divide_age_interval(heart_df, kedro_session.load_context.params("num_intervals"))

#         # Check if the returned subintervals match the expected subintervals
#         assert subintervals == expected_subintervals