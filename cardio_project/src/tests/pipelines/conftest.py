from kedro.framework.session import KedroSession
from pathlib import Path
from kedro.framework.startup import bootstrap_project

import pytest

@pytest.fixture(scope="session")
def kedro_session():
    """Set up a Kedro session for testing."""
    # If you are creating a session outside of a Kedro project (i.e. not using
    # `kedro run` or `kedro jupyter`), you need to run `bootstrap_project` to
    # let Kedro find your configuration.
    bootstrap_project(Path("/home/kunumi/projetos/Data_science_projects/Data_science_projects/cardio_project"))

    with KedroSession.create() as session:
        return session
    

@pytest.fixture(scope="session")
def heart_df(kedro_session):

    # Load DataFrame from the catalog
    df = kedro_session.load_context().catalog.load("heart")

    return df

@pytest.fixture(scope="session")
def params(kedro_session):

    # Load DataFrame from the catalog
    params = kedro_session.load_context().params

    return params