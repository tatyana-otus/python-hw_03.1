import hashlib
import datetime
import functools
import unittest
import redis
import subprocess
import json

import api
import store

FAKE_PORT = 9876
TEST_PORT = 9006
redis_up_config = {
    "host": "localhost",
    "port": TEST_PORT,
    "db": 0
}

redis_down_config = {
    "host": "localhost",
    "port": FAKE_PORT,
    "db": 0
}


def cases(cases):
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args):
            for c in cases:
                new_args = args + (c if isinstance(c, tuple) else (c,))
                f(*new_args)
        return wrapper
    return decorator


class BaseFieldTest(unittest.TestCase):

    def test_field_1(self):
        f = api.Field(required=False, nullable=False)
        self.assertEqual(f.valid(None), True, 'value = None: required=False')
        self.assertEqual(f.valid(""), False, 'value = "": nullable=False')

    def test_field_2(self):
        f = api.Field(required=True, nullable=False)
        self.assertEqual(f.valid(None), False, 'value = None : required=True')
        self.assertEqual(f.valid(""), False, 'value = "" null_value : nullable=False')

    def test_field_3(self):
        f = api.Field(required=False, nullable=True)
        self.assertEqual(f.valid(None), True, 'value = None:  required=False')
        self.assertEqual(f.valid(""), True, 'value = "" null_value : nullable=True')

    def test_field_4(self):
        f = api.Field(required=True, nullable=True)
        self.assertEqual(f.valid(None), False, 'value = None : required=True')
        self.assertEqual(f.valid(""), True, 'value = "" null_value : nullable=True')

    @cases(["111", 111, {}, []])
    def test_field_5(self, arg):
        null_values = ["111", 111, {}, []]
        f = api.Field(null_values, required=True, nullable=False)
        self.assertEqual(f.valid(arg), False, "{} is null_value : nullable=False".format(arg))

    @cases(["111", 111, {}, []])
    def test_field_6(self, arg):
        null_values = ["111", 111, {}, []]
        f = api.Field(null_values, required=True, nullable=True)
        self.assertEqual(f.valid(arg), True, "{} is null_value : nullable=True".format(arg))


class CharFieldTest(unittest.TestCase):

    def test_charfield_1(self):
        f = api.CharField(required=False, nullable=False)
        self.assertEqual(f.valid(None), True, 'value = None: required=False')
        f = api.CharField(required=True, nullable=True)
        self.assertEqual(f.valid(""), True, 'value = "": nullable=True')

    @cases(["123", "[123]", "\x00xfsdf-0s09#$%#@*,.]!~{"])
    def test_charfield_2(self, arg):
        f = api.CharField(required=True, nullable=False)
        self.assertEqual(f.valid(arg), True, "{} is CharField".format(arg))

    @cases([123, [123], 34.89, {"ewe": "qeewr"}])
    def test_charfield_3(self, arg):
        f = api.CharField(required=False, nullable=True)
        self.assertEqual(f.valid(arg), False, "{} is not CharField".format(arg))


class ArgumentsTest(unittest.TestCase):

    def test_argumentsfield_1(self):
        f = api.ArgumentsField(required=False, nullable=False)
        self.assertEqual(f.valid(None), True, 'value = None: required=False')
        f = api.ArgumentsField(required=True, nullable=True)
        self.assertEqual(f.valid({}), True, 'value = {}: nullable=True')

    @cases([{1: 1}, {"1": 1}, {"sdfsf": "dsfs"},
            {"sdfsf": [1, 33], 232: {1: 3, 4: 5}}])
    def test_argumentsfield_2(self, arg):
        f = api.ArgumentsField(required=True, nullable=False)
        self.assertEqual(f.valid(arg), True, "{} is ArgumentsField".format(arg))

    @cases([23, "dsfsdf", [2, 4, 5], 45.6])
    def test_argumentsfield_3(self, arg):
        f = api.ArgumentsField(required=False, nullable=True)
        self.assertEqual(f.valid(arg), False, "{} is not ArgumentsField".format(arg))


class EmailFieldTest(unittest.TestCase):

    def test_emailfield_1(self):
        f = api.EmailField(required=False, nullable=False)
        self.assertEqual(f.valid(None), True, 'value = None: required=False')
        f = api.EmailField(required=True, nullable=True)
        self.assertEqual(f.valid(""), True, 'value = "": nullable=True')

    @cases(["@", "roberthawkins@yahoo.com"])
    def test_emailfield_2(self, arg):
        f = api.EmailField(required=True, nullable=False)
        self.assertEqual(f.valid("@"), True, "{} is EmailField".format(arg))

    @cases(["kins.yahoo.com", "123"])
    def test_emailfield_3(self, arg):
        f = api.EmailField(required=False, nullable=True)
        self.assertEqual(f.valid(arg), False, "{} is not EmailField".format(arg))


class PhoneFieldTest(unittest.TestCase):

    def test_phonefield_1(self):
        f = api.PhoneField(required=False, nullable=False)
        self.assertEqual(f.valid(None), True, 'value = None: required=False')
        f = api.PhoneField(required=True, nullable=True)
        self.assertEqual(f.valid(""), True, 'value = "": nullable=True')
        self.assertEqual(f.valid(0), True, 'value = 0: nullable=True')

    @cases(["71234567890", 71234567890, 70000000000, "77777777777"])
    def test_phonefield_2(self, arg):
        f = api.PhoneField(required=True, nullable=False)
        self.assertEqual(f.valid(arg), True, "{} is valid PhoneField".format(arg))

    @cases([81234567890, "81234567890", 712345678900, "712345678900", 712345678,
            "712345678", "+71234567890", "7123456789W", [71234567890], {71234567890}])
    def test_phonefield_3(self, arg):
        f = api.PhoneField(required=True, nullable=False)
        self.assertEqual(f.valid(arg), False, "{} is not valid PhoneField".format(arg))


class DateFieldTest(unittest.TestCase):

    def test_datafield_1(self):
        f = api.DateField(required=False, nullable=False)
        self.assertEqual(f.valid(None), True, 'value = None: required=False')
        f = api.DateField(required=True, nullable=True)
        self.assertEqual(f.valid(""), True, 'value = "": nullable=True')

    @cases(["31.12.1234", "1.1.0001", "31.12.9999", "01.12.9999", "29.02.2000"])
    def test_datafield_2(self, arg):
        f = api.DateField(required=True, nullable=False)
        self.assertEqual(f.valid(arg), True, "{} is valid DateField".format(arg))

    @cases([12122012, "31.06.2018", "10.10.145", "18-11-2018", "18/11/2018",
            "1a.11.2018", "01.02.06.2018", "01.09"])
    def test_datafield_3(self, arg):
        f = api.DateField(required=True, nullable=False)
        self.assertEqual(f.valid(arg), False, "{} is not valid DateField".format(arg))


class BirthDayFieldTest(unittest.TestCase):

    def test_birthdayfield_1(self):
        f = api.BirthDayField(required=False, nullable=False)
        self.assertEqual(f.valid(None), True, 'value = None: required=False')
        f = api.BirthDayField(required=True, nullable=True)
        self.assertEqual(f.valid(""), True, 'value = "": nullable=True')

    def test_birthdayfield_2(self):
        f = api.BirthDayField(required=True, nullable=False)
        self.assertEqual(f.valid("08.08.1980"), True)

    def test_birthdayfield_2(self):
        f = api.BirthDayField(required=True, nullable=False)
        self.assertEqual(f.valid("08.08.1880"), False)
        self.assertEqual(f.valid("08.08.2080"), False)


class GenderField(unittest.TestCase):

    def test_genderfield_1(self):
        f = api.GenderField(required=False, nullable=True)
        self.assertEqual(f.valid(None), True, 'value = None: required=False')

    @cases([0, 1, 2])
    def test_genderfield_2(self, arg):
        f = api.GenderField(required=True, nullable=False)
        self.assertEqual(f.valid(arg), True, "{} is valid GenderField".format(arg))

    @cases([3, -1, 11, "1", [1], 1.0])
    def test_genderfield_3(self, arg):
        f = api.GenderField(required=True, nullable=False)
        self.assertEqual(f.valid(arg), False, "{} is not valid GenderField".format(arg))


class ClientIDsField(unittest.TestCase):

    def test_clientidsfield_1(self):
        f = api.ClientIDsField(required=False, nullable=False)
        self.assertEqual(f.valid(None), True, 'value = None: required=False')
        f = api.ClientIDsField(required=True, nullable=True)
        self.assertEqual(f.valid([]), True, 'value = "": nullable=True')

    @cases([[0, 1], [1], [1, 9, 67, 1, 1, 6], [0, 0, 0]])
    def test_clientidsfield_2(self, arg):
        f = api.ClientIDsField(required=True, nullable=False)
        self.assertEqual(f.valid(arg), True, "{} is valid ClientIDsField".format(arg))

    @cases([[0, -1], [0, "1"], {0, 1}, 1, "0 1 2"])
    def test_clientidsfield_3(self, arg):
        f = api.ClientIDsField(required=True, nullable=False)
        self.assertEqual(f.valid(arg), False, "{} is not valid ClientIDsField".format(arg))


class TestSuiteStoreOk(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.redis_process = subprocess.Popen(['redis-server', '--port',
                                             str(redis_up_config["port"])],
                                             stdout=subprocess.DEVNULL)
        connect = False
        r = redis.StrictRedis(**redis_up_config)
        while connect is False:
            try:
                connect = r.ping()
            except redis.exceptions.ConnectionError as e:
                pass

    @classmethod
    def tearDownClass(cls):
        cls.redis_process.terminate()
        cls.redis_process.wait()

    def setUp(self):
        self.context = {}
        self.headers = {}
        self.store = store.Store(**redis_up_config)

    def tearDown(self):
        r = redis.StrictRedis(**redis_up_config)
        r.flushdb()

    def get_response(self, request):
        return api.method_handler({"body": request, "headers": self.headers}, self.context, self.store)

    def set_valid_auth(self, request):
        if request.get("login") == api.ADMIN_LOGIN:
            d = datetime.datetime.now().strftime("%Y%m%d%H") + api.ADMIN_SALT
            request["token"] = hashlib.sha512(d.encode('utf-8')).hexdigest()
        else:
            msg = request.get("account", "") + request.get("login", "") + api.SALT
            request["token"] = hashlib.sha512(msg.encode('utf-8')).hexdigest()

    def get_score_key(self, arguments):
        first_name = arguments.get("first_name")
        last_name = arguments.get("last_name")
        phone = str(arguments.get("phone", ""))
        birthday = arguments.get("birthday")
        if birthday:
            birthday = datetime.datetime.strptime(birthday, "%d.%m.%Y")
        key_parts = [
            first_name or "",
            last_name or "",
            phone or "",
            birthday.strftime("%Y%m%d") if birthday is not None else "",
        ]
        return "uid:" + hashlib.md5("".join(key_parts).encode('utf-8')).hexdigest()

    def calculate_score(self, arguments):
        score = 0
        if arguments.get("phone"):
            score += 1.5
        if arguments.get("email"):
            score += 1.5
        if arguments.get("birthday") and arguments.get("gender"):
            score += 1.5
        if arguments.get("first_name") and arguments.get("last_name"):
            score += 0.5
        return score

    def save_score_key_value(self, key, value):
        r = redis.StrictRedis(**redis_up_config)
        r.set(key, value)

    def fill_db_interests(self):
        r = redis.StrictRedis(**redis_up_config)
        r.set("i:0", '["poker", "alcohol"]')
        r.set("i:1", '["yoga", "theater"]')
        r.set("i:2", '["pole dance", "zumba"]')
        r.set("i:3", '["football", "beer"]')

    def fill_db_interests_wrong_format(self):
        r = redis.StrictRedis(**redis_up_config)
        r.set("i:0", 'poker", "alcohol"')

    def test_empty_request(self):
        _, code = self.get_response({})
        self.assertEqual(api.INVALID_REQUEST, code)

    def test_ok_score_admin_request(self):
        arguments = {"phone": "79175002040", "email": "stupnikov@otus.ru"}
        request = {"account": "horns&hoofs", "login": "admin", "method": "online_score", "arguments": arguments}
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.OK, code)
        score = response.get("score")
        self.assertEqual(score, 42)

    @cases([
        {"phone": "79175002040", "email": "stupnikov@otus.ru"},
        {"phone": 79175002040, "email": "stupnikov@otus.ru"},
        {"gender": 1, "birthday": "01.01.2000", "first_name": "a", "last_name": "b"},
        {"gender": 0, "birthday": "01.01.2000"},
        {"gender": 2, "birthday": "01.01.2000"},
        {"first_name": "a", "last_name": "b"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": 1, "birthday": "01.01.2000",
         "first_name": "a", "last_name": "b"},
    ])
    def test_ok_score_request_fail_cache_get(self, arguments):
        request = {"account": "horns&hoofs", "login": "h&f", "method": "online_score", "arguments": arguments}
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.OK, code, arguments)
        score = response.get("score")
        self.assertTrue(isinstance(score, (int, float)) and score >= 0, arguments)
        self.assertEqual(score, self.calculate_score(arguments), arguments)
        self.assertEqual(sorted(self.context["has"]), sorted(arguments.keys()))

    @cases([
        {"phone": "79175002040", "email": "stupnikov@otus.ru"},
        {"phone": 79175002040, "email": "stupnikov@otus.ru"},
        {"gender": 1, "birthday": "01.01.2000", "first_name": "a", "last_name": "b"},
        {"gender": 0, "birthday": "01.01.2000"},
        {"gender": 2, "birthday": "01.01.2000"},
        {"first_name": "a", "last_name": "b"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": 1, "birthday": "01.01.2000",
         "first_name": "a", "last_name": "b"},
    ])
    def test_ok_score_request_ok_cache_get(self, arguments):
        request = {"account": "horns&hoofs", "login": "h&f", "method": "online_score", "arguments": arguments}
        key = self.get_score_key(arguments)
        self.save_score_key_value(key, 123.987)
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.OK, code, arguments)
        score = response.get("score")
        self.assertEqual(score, 123.987, arguments)
        self.assertEqual(sorted(self.context["has"]), sorted(arguments.keys()))

    @cases([
        {"phone": "79175002040", "email": "stupnikov@otus.ru"}
    ])
    def test_ok_score_request_wrong_db_format(self, arguments):
        request = {"account": "horns&hoofs", "login": "h&f", "method": "online_score", "arguments": arguments}
        key = self.get_score_key(arguments)
        self.save_score_key_value(key, "one")

        self.set_valid_auth(request)
        with self.assertRaises(ValueError):
            self.get_response(request)

    @cases([
        {"client_ids": [1, 2, 3], "date": datetime.datetime.today().strftime("%d.%m.%Y")},
        {"client_ids": [1, 2], "date": "19.07.2017"},
        {"client_ids": [0]},
    ])
    def test_ok_interests_request_ok_get(self, arguments):
        self.fill_db_interests()
        request = {"account": "horns&hoofs", "login": "h&f", "method": "clients_interests", "arguments": arguments}
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.OK, code, arguments)
        self.assertEqual(len(arguments["client_ids"]), len(response), arguments)
        self.assertTrue(all(v and isinstance(v, list) and all(isinstance(i, str) for i in v)
                        for v in response.values()), arguments)
        self.assertEqual(self.context.get("nclients"), len(arguments["client_ids"]),
                         arguments)

    @cases([
        {"client_ids": [1, 2, 3], "date": datetime.datetime.today().strftime("%d.%m.%Y")},
        {"client_ids": [1, 2], "date": "19.07.2017"},
        {"client_ids": [0]},
    ])
    def test_ok_interests_request_empty_db(self, arguments):
        request = {"account": "horns&hoofs", "login": "h&f", "method": "clients_interests", "arguments": arguments}
        self.set_valid_auth(request)
        with self.assertRaises(ValueError):
            self.get_response(request)

    @cases([
         {"client_ids": [0]},
    ])
    def test_ok_interests_wrong_db_format(self, arguments):
        self.fill_db_interests_wrong_format()
        request = {"account": "horns&hoofs", "login": "h&f", "method": "clients_interests", "arguments": arguments}
        self.set_valid_auth(request)
        with self.assertRaises(json.decoder.JSONDecodeError):
            self.get_response(request)


class TestSuiteStoreDown(unittest.TestCase):

    def setUp(self):
        self.context = {}
        self.headers = {}
        self.store = store.Store(**redis_down_config)

    def get_response(self, request):
        return api.method_handler({"body": request, "headers": self.headers}, self.context, self.store)

    def set_valid_auth(self, request):
        if request.get("login") == api.ADMIN_LOGIN:
            d = datetime.datetime.now().strftime("%Y%m%d%H") + api.ADMIN_SALT
            request["token"] = hashlib.sha512(d.encode('utf-8')).hexdigest()
        else:
            msg = request.get("account", "") + request.get("login", "") + api.SALT
            request["token"] = hashlib.sha512(msg.encode('utf-8')).hexdigest()

    def test_empty_request(self):
        _, code = self.get_response({})
        self.assertEqual(api.INVALID_REQUEST, code)

    def test_ok_score_admin_request(self):
        arguments = {"phone": "79175002040", "email": "stupnikov@otus.ru"}
        request = {"account": "horns&hoofs", "login": "admin", "method": "online_score", "arguments": arguments}
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.OK, code)
        score = response.get("score")
        self.assertEqual(score, 42)

    @cases([
        {"client_ids": [1, 2, 3], "date": datetime.datetime.today().strftime("%d.%m.%Y")},
        {"client_ids": [1, 2], "date": "19.07.2017"},
        {"client_ids": [0]},
    ])
    def test_ok_interests_request(self, arguments):
        request = {"account": "horns&hoofs", "login": "h&f", "method": "clients_interests", "arguments": arguments}
        self.set_valid_auth(request)
        with self.assertRaises(ConnectionError):
            self.get_response(request)

    @cases([
        {"phone": "79175002040", "email": "stupnikov@otus.ru"},
        {"phone": 79175002040, "email": "stupnikov@otus.ru"},
        {"gender": 1, "birthday": "01.01.2000", "first_name": "a", "last_name": "b"},
        {"gender": 0, "birthday": "01.01.2000"},
        {"gender": 2, "birthday": "01.01.2000"},
        {"first_name": "a", "last_name": "b"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": 1, "birthday": "01.01.2000",
         "first_name": "a", "last_name": "b"},
    ])
    def test_ok_score_request(self, arguments):
        request = {"account": "horns&hoofs", "login": "h&f", "method": "online_score", "arguments": arguments}
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.OK, code, arguments)
        score = response.get("score")
        self.assertTrue(isinstance(score, (int, float)) and score >= 0, arguments)
        self.assertEqual(sorted(self.context["has"]), sorted(arguments.keys()))

    @cases([
        {"account": "horns&hoofs", "login": "h&f", "method": "online_score"},
        {"account": "horns&hoofs", "login": "h&f", "arguments": {}},
        {"account": "horns&hoofs", "method": "online_score", "arguments": {}},
    ])
    def test_invalid_method_request(self, request):
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.INVALID_REQUEST, code, request)
        self.assertTrue(len(response), request)

    @cases([
        {},
        {"phone": "79175002040"},
        {"phone": "89175002040", "email": "stupnikov@otus.ru"},
        {"phone": "79175002040", "email": "stupnikovotus.ru"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": -1},
        {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": "1"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": 1, "birthday": "01.01.1890"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": 1, "birthday": "XXX"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": 1, "birthday": "01.01.2000", "first_name": 1},
        {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": 1, "birthday": "01.01.2000",
         "first_name": "s", "last_name": 2},
        {"phone": "79175002040", "birthday": "01.01.2000", "first_name": "s"},
        {"email": "stupnikov@otus.ru", "gender": 1, "last_name": 2},
    ])
    def test_invalid_score_request(self, arguments):
        request = {"account": "horns&hoofs", "login": "h&f", "method": "online_score", "arguments": arguments}
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.INVALID_REQUEST, code, arguments)
        self.assertTrue(len(response), arguments)

    @cases([
        {},
        {"date": "20.07.2017"},
        {"client_ids": [], "date": "20.07.2017"},
        {"client_ids": {1: 2}, "date": "20.07.2017"},
        {"client_ids": ["1", "2"], "date": "20.07.2017"},
        {"client_ids": [1, 2], "date": "XXX"},
    ])
    def test_invalid_interests_request(self, arguments):
        request = {"account": "horns&hoofs", "login": "h&f", "method": "clients_interests", "arguments": arguments}
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.INVALID_REQUEST, code, arguments)
        self.assertTrue(len(response))

if __name__ == "__main__":
    unittest.main()
