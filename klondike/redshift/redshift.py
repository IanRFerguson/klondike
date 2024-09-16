from klondike.postgres.postgres import PostgresConnector
from typing import Optional

#########


class RedshiftConnector(PostgresConnector):
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        default_env_prefix: str = "REDSHIFT",
    ):
        super().__init__(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            default_env_prefix=default_env_prefix,
        )
