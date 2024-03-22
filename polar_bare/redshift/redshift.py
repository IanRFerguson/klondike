from polar_bare.postgres.postgres import PolarPostgres

##########


class PolarRedshift(PolarPostgres):
    """
    Establish and authenticate a connection to a Redshift cluster

    Args:
        host: Required if `PG_HOST` absent in runtime environment
        db: Required if `PG_DBNAME` absent in runtime environment
        port: Required if `PG_PORT` absent in runtime environment
        username: Required if `PG_USERNAME` absent in runtime environment
        password: Required if `PG_PASSWORD` absent in runtime environment
        timeout: Defines connection timeout tolerance (default=60s)
    """

    def __init__(self, host, port, db, username, password, timeout):
        super().__init__(
            host=host,
            db=db,
            port=port,
            username=username,
            password=password,
            timeout=timeout,
        )

        self.dialect = "redshift"
