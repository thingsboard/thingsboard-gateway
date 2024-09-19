from tests.base_test import BaseTest


class BaseUnitTest(BaseTest):

    @classmethod
    def setUpClass(cls):
        pass

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()