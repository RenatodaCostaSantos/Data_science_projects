from kedro.framework.session import KedroSession
from cardio_again.pipelines.data_preprocessing.nodes import _separate_male_female
from pathlib import Path
import pytest

from kedro.framework.startup import bootstrap_project

@pytest.fixture()
def kedro_session():
    """Set up a Kedro session for testing."""
    # If you are creating a session outside of a Kedro project (i.e. not using
    # `kedro run` or `kedro jupyter`), you need to run `bootstrap_project` to
    # let Kedro find your configuration.
    bootstrap_project(Path("/home/kunumi/projetos/Data_science_projects/Data_science_projects/cardio_project"))

    with KedroSession.create() as session:
        return session



def test_separate_male_female(kedro_session):
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
    # Load DataFrame from the catalog
    df = kedro_session.load_context().catalog.load("heart")

    # Call the function to separate male and female records
    male_df, female_df = _separate_male_female(df)

    # Verify that the total number of records is preserved
    assert male_df.shape[0] + female_df.shape[0] == df.shape[0]

    # Verify that all records in the male DataFrame are male
    assert all(male_df["Sex"] == "M")

    # Verify that all records in the female DataFrame are female
    assert all(female_df["Sex"] == "F")
