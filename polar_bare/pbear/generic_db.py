import pickle
import tempfile
from typing import Optional

import petl
from polars import DataFrame

##########


class PolarBareDB:
    """
    Abstract class with shared utilities across all database instances
    """

    def __init__(self):
        self.__tempfiles = []

    def query(
        self, sql: str, parameters: Optional[list] = None, batch_size: int = 250_000
    ) -> Optional[DataFrame]:
        """
        Leverages internal connection to query against Postgres instance

        Args:
            sql: String-formatted SQL query
            parameters: Optional parameters to avoid SQL injection
            batch_size:

        Returns:
            Polars DataFrame of query results
        """

        with self.connection() as conn_:
            with conn_.cursor() as cursor:
                cursor.execute(query=sql, params=parameters)

                # If no results, return without raising exeption
                if not cursor.description:
                    return None

                ###

                temp_ = tempfile.TemporaryFile()
                temp_file = temp_.name
                self.__tempfiles.append(temp_.name)

                with open(temp_file, "wb") as f:
                    header_ = [x[0] for x in cursor.description]
                    pickle.dump(header_, f)

                    while True:
                        batch = cursor.fetchmany(size=batch_size)

                        if not batch:
                            break

                        for row in batch:
                            pickle.dump(list(row), f)

                df = DataFrame(petl.frompickle(temp_file))

                return df