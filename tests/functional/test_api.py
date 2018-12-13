from datetime import datetime
import unittest
import redis
import subprocess

from tests import helper

from api import api
from api import store
from api import scoring


TEST_PORT = 9006
redis_config = {
    "host": "localhost",
    "port": TEST_PORT,
    "db": 0
}


class MockStore():
    def __init__(self):
        self.data_store = {}

    def cache_get(self, key):
        return self.data_store.get(key, None)

    def cache_set(self, key, value, ttl_sec=None):
        self.data_store[key] = str(value)

    def get(self, key):
        return self.data_store.get(key, None)

    def set(self, key, value):
        self.data_store[key] = str(value)


def expected_score(arguments):
    birthday = arguments.get("birthday", None)
    if birthday:
        birthday = datetime.strptime(birthday, "%d.%m.%Y")
    phone = arguments.get("phone", None)
    if phone:
        phone = str(phone)
    return scoring.get_score(MockStore(),
                             phone,
                             arguments.get("email", None),
                             birthday,
                             arguments.get("gender", None),
                             arguments.get("first_name", None),
                             arguments.get("last_name", None))


class TestSuiteApiValidRequests(unittest.TestCase):

    def setUp(self):
        self.context = {}
        self.headers = {}
        self.store = MockStore()

    def get_response(self, request):
        return api.method_handler({"body": request, "headers": self.headers},
                                  self.context, self.store)

    def fill_db_interests(self):
        self.store.set("i:0", '["poker", "alcohol"]')
        self.store.set("i:1", '["yoga", "theater"]')
        self.store.set("i:2", '["pole dance", "zumba"]')
        self.store.set("i:3", '["football", "beer"]')

    def test_empty_request(self):
        _, code = self.get_response({})
        self.assertEqual(api.INVALID_REQUEST, code)

    def test_ok_score_admin_request(self):
        arguments = {"phone": "79175002040", "email": "stupnikov@otus.ru"}
        request = {"account": "horns&hoofs", "login": "admin",
                   "method": "online_score", "arguments": arguments}
        helper.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.OK, code)
        score = response.get("score")
        self.assertEqual(score, 42)

    @helper.cases([
        {"phone": "79175002040", "email": "stupnikov@otus.ru"},
        {"phone": 79175002040, "email": "stupnikov@otus.ru"},
        {"gender": 1, "birthday": "01.01.2000", "first_name": "a",
         "last_name": "b"},
        {"gender": 0, "birthday": "01.01.2000"},
        {"gender": 2, "birthday": "01.01.2000"},
        {"first_name": "a", "last_name": "b"},
        {"first_name": "a", "last_name": "b", "email": "stupnikov@otus.ru"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": 1,
         "birthday": "01.01.2000", "first_name": "a", "last_name": "b"},
    ])
    def test_ok_score_request(self, arguments):
        request = {"account": "horns&hoofs", "login": "h&f",
                   "method": "online_score", "arguments": arguments}
        helper.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.OK, code)
        score = response.get("score")
        self.assertTrue(isinstance(score, (int, float)) and score >= 0)
        self.assertEqual(score, expected_score(arguments))
        self.assertEqual(sorted(self.context["has"]), sorted(arguments.keys()))

    @helper.cases([
        {"client_ids": [1, 2, 3], "date": datetime.today().strftime("%d.%m.%Y")},
        {"client_ids": [1, 2], "date": "19.07.2017"},
        {"client_ids": [0]},
    ])
    def test_ok_interests_request_ok_get(self, arguments):
        self.fill_db_interests()
        request = {"account": "horns&hoofs", "login": "h&f",
                   "method": "clients_interests", "arguments": arguments}
        helper.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.OK, code)
        self.assertEqual(len(arguments["client_ids"]), len(response))
        self.assertTrue(all(v and isinstance(v, list) and all(isinstance(i, str) for i in v)
                        for v in response.values()))
        self.assertEqual(self.context.get("nclients"), len(arguments["client_ids"]))


class TestSuiteApiInvalidRequests(unittest.TestCase):

    def setUp(self):
        self.context = {}
        self.headers = {}
        self.store = MockStore()

    def get_response(self, request):
        return api.method_handler({"body": request, "headers": self.headers},
                                  self.context, self.store)

    @helper.cases([
        {"account": "horns&hoofs", "login": "h&f", "method": "online_score"},
        {"account": "horns&hoofs", "login": "h&f", "arguments": {}},
        {"account": "horns&hoofs", "method": "online_score", "arguments": {}},
    ])
    def test_invalid_method_request(self, request):
        helper.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.INVALID_REQUEST, code)
        self.assertTrue(len(response), request)

    @helper.cases([
        {},
        {"phone": "79175002040"},
        {"phone": "89175002040", "email": "stupnikov@otus.ru"},
        {"phone": "79175002040", "email": "stupnikovotus.ru"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru",
         "gender": -1},
        {"phone": "79175002040", "email": "stupnikov@otus.ru",
         "gender": "1"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru",
         "gender": 1, "birthday": "01.01.1890"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru",
         "gender": 1, "birthday": "XXX"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru",
         "gender": 1, "birthday": "01.01.2000", "first_name": 1},
        {"phone": "79175002040", "email": "stupnikov@otus.ru",
         "gender": 1, "birthday": "01.01.2000",
         "first_name": "s", "last_name": 2},
        {"phone": "79175002040", "birthday": "01.01.2000",
         "first_name": "s"},
        {"email": "stupnikov@otus.ru", "gender": 1, "last_name": 2},
    ])
    def test_invalid_score_request(self, arguments):
        request = {"account": "horns&hoofs", "login": "h&f",
                   "method": "online_score", "arguments": arguments}
        helper.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.INVALID_REQUEST, code)
        self.assertTrue(len(response), arguments)

    @helper.cases([
        {},
        {"date": "20.07.2017"},
        {"client_ids": [], "date": "20.07.2017"},
        {"client_ids": {1: 2}, "date": "20.07.2017"},
        {"client_ids": ["1", "2"], "date": "20.07.2017"},
        {"client_ids": [1, 2], "date": "XXX"},
    ])
    def test_invalid_interests_request(self, arguments):
        request = {"account": "horns&hoofs", "login": "h&f",
                   "method": "clients_interests", "arguments": arguments}
        helper.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.INVALID_REQUEST, code)
        self.assertTrue(len(response))


if __name__ == "__main__":
    unittest.main()
