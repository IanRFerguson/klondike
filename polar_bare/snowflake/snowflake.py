from contextlib import contextmanager
from typing import Optional

import snowflake.connector

from polar_bare.pbear.generic_db import PolarBareDB

##########


class PolarSnowflake(PolarBareDB):
    """
    Establish and authenticate a connection to a Snowflake warehouse
    """

    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        account: Optional[str] = None,
        session_parameters: dict = {},
    ):
        self.__username = username
        self.__password = password
        self.account = account
        self.session_parameters = session_parameters

    @contextmanager
    def connection(self):
        """
        TODO - Fill this in
        """

        connection = snowflake.connector.connect(
            user=self.__username,
            password=self.__password,
            account=self.account,
            session_parameters=self.session_parameters,
        )

        try:
            yield connection
        except snowflake.connector.Error:
            connection.rollback()
            raise
        else:
            connection.commit()
        finally:
            connection.close()
