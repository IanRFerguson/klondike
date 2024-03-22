import json
import os
import tempfile
from contextlib import contextmanager
from typing import Optional, Union

from google.cloud import bigquery
from google.cloud.bigquery import dbapi

from polar_bare.pbear.generic_db import PolarBareDB

##########

SCOPES = (
    {
        "scopes": [
            "https://www.googleapis.com/auth/bigquery",
        ]
    },
)


class PolarBigQuery(PolarBareDB):
    """
    Establish and authenticate a connection to a BigQuery warehouse
    """

    def __init__(
        self,
        app_creds: Optional[Union[str, dict]] = None,
        gcp_project: Optional[str] = None,
        timeout: int = 60,
        client_options: dict = SCOPES,
        google_environment_variable: str = "GOOGLE_APPLICATION_CREDENTIALS",
    ):
        self.app_creds = app_creds
        self.gcp_project = gcp_project
        self.timeout = timeout
        self.client_options = client_options

        self._client = None
        self.dialect = "bigquery"
        self._dbapi = dbapi

        self.__setup_google_app_creds(
            app_creds=app_creds, env_variable=google_environment_variable
        )

    def __setup_google_app_creds(self, app_creds: Union[str, dict], env_variable: str):
        """
        Sets runtime environment variablefor Google SDK
        """

        if isinstance(app_creds, dict):
            creds = json.dumps(app_creds)
            tempp_file = tempfile.TemporaryFile(suffix=".json")

            with open(tempp_file.name, "w") as f:
                f.write(creds)

        elif isinstance(app_creds, str):
            creds = app_creds

        os.environ[env_variable] = creds

    @property
    def client(self):
        """
        Instantiate BigQuery client
        """

        if not self._client:
            self._client = bigquery.Client(
                project=self.project,
                location=self.location,
                client_options=self.client_options,
            )
        return self._client

    @contextmanager
    def connection(self):
        """
        TODO - Fill me in
        """

        conn_ = self._dbapi.connect(self.client)

        try:
            yield conn_
        finally:
            conn_.close()
