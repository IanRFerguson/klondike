import json
import os
import tempfile
import unittest

##########


class KlondikeTestCase(unittest.TestCase):
    def setUp(self):
        self._temp_directory = tempfile.TemporaryDirectory()

        self._credentials_path = os.path.join(
            self._temp_directory.name, "service_account.json"
        )

        self._service_account = {
            "type": "foo",
            "project_id": "bar",
            "private_key_id": "biz",
            "private_key": "bap",
            "client_email": "bim",
            "client_id": "top",
            "auth_uri": "hat",
            "token_uri": "tap",
            "auth_provider_x509_cert_url": "dance",
            "client_x509_cert_url": "good",
            "universe_domain": "stuff",
        }

        with open(self._credentials_path, "w") as f:
            json.dump(self._service_account, f)

    def tearDown(self):
        self._temp_directory.cleanup()
