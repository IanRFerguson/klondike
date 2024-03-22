from contextlib import contextmanager
from typing import Optional, Union

from google.cloud import bigquery

from polar_bare.pbear.generic_db import PolarBareDB

##########


class BigQuery(PolarBareDB):
    """
    TODO - Fill me in
    """

    def __init__(
        self,
        app_creds: Optional[Union[str, dict]] = None,
        gcp_project: Optional[str] = None,
        timeout: int = 60,
        scopes: dict = {
            "scopes": [
                "https://www.googleapis.com/auth/bigquery",
            ]
        },
        google_environment_variable: str = "GOOGLE_APPLICATION_CREDENTIALS",
    ):
        self.app_creds = app_creds
        self.__setup_google_app_creds(
            app_creds=app_creds, env_variable=google_environment_variable
        )
        self.gcp_project = gcp_project
        self.timeout = timeout
        self.scopes = scopes

    def __setup_google_app_creds(self, app_creds: Union[str, dict], env_variable: str):
        """
        TODO - Fill me in
        """

        pass

    @contextmanager
    def connection(self):
        pass
