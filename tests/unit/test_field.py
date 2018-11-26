import datetime
import functools
import unittest
from datetime import datetime

from api import api


def cases(cases):
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args):
            for c in cases:
                new_args = args + (c if isinstance(c, tuple) else (c,))
                try:
                    f(*new_args)
                except AssertionError as e:
                    raise AssertionError("{}: {} (test case: {})".format(e, f.__name__, c))
        return wrapper
    return decorator


class BaseFieldTest(unittest.TestCase):

    def test_field_required_flase_nulllabe_false(self):
        f = api.Field(required=False, nullable=False)
        self.assertEqual(f.valid(None), True, 'value = None: required=False')
        self.assertEqual(f.valid(""), False, 'value = "": nullable=False')

    def test_field_required_true_nulllabe_false(self):
        f = api.Field(required=True, nullable=False)
        self.assertEqual(f.valid(None), False, 'value = None : required=True')
        self.assertEqual(f.valid(""), False, 'value = "" null_value : nullable=False')

    def test_field_required_flase_nulllabe_true(self):
        f = api.Field(required=False, nullable=True)
        self.assertEqual(f.valid(None), True, 'value = None:  required=False')
        self.assertEqual(f.valid(""), True, 'value = "" null_value : nullable=True')

    def test_field_required_true_nulllabe_true(self):
        f = api.Field(required=True, nullable=True)
        self.assertEqual(f.valid(None), False, 'value = None : required=True')
        self.assertEqual(f.valid(""), True, 'value = "" null_value : nullable=True')

    @cases(["111", 111, {}, []])
    def test_field_null_value_nullable_false(self, arg):
        null_values = ["111", 111, {}, []]
        f = api.Field(null_values, required=True, nullable=False)
        self.assertEqual(f.valid(arg), False)

    @cases(["111", 111, {}, []])
    def test_field_null_value_nullable_true(self, arg):
        null_values = ["111", 111, {}, []]
        f = api.Field(null_values, required=True, nullable=True)
        self.assertEqual(f.valid(arg), True)


class CharFieldTest(unittest.TestCase):

    def test_charfield_empty_nullable(self):
        f = api.CharField(required=False, nullable=False)
        self.assertEqual(f.valid(None), True, 'value = None: required=False')
        f = api.CharField(required=True, nullable=True)
        self.assertEqual(f.valid(""), True, 'value = "": nullable=True')

    @cases(["123", "[123]", "\x00xfsdf-0s09#$%#@*,.]!~{"])
    def test_charfield_valid(self, arg):
        f = api.CharField(required=True, nullable=False)
        self.assertEqual(f.valid(arg), True)

    @cases([123, [123], 34.89, {"ewe": "qeewr"}])
    def test_charfield_invalid(self, arg):
        f = api.CharField(required=False, nullable=True)
        self.assertEqual(f.valid(arg), False)
        with self.assertRaises(api.ValidationError):
            f.validate(arg)


class ArgumentsFieldTest(unittest.TestCase):

    def test_argumentsfield_empty_nullable(self):
        f = api.ArgumentsField(required=False, nullable=False)
        self.assertEqual(f.valid(None), True, 'value = None: required=False')
        f = api.ArgumentsField(required=True, nullable=True)
        self.assertEqual(f.valid({}), True, 'value = {}: nullable=True')

    @cases([{1: 1}, {"1": 1}, {"sdfsf": "dsfs"},
            {"sdfsf": [1, 33], 232: {1: 3, 4: 5}}])
    def test_argumentsfield_valid(self, arg):
        f = api.ArgumentsField(required=True, nullable=False)
        self.assertEqual(f.valid(arg), True)

    @cases([23, "dsfsdf", [2, 4, 5], 45.6])
    def test_argumentsfield_invalid(self, arg):
        f = api.ArgumentsField(required=False, nullable=True)
        self.assertEqual(f.valid(arg), False)
        with self.assertRaises(api.ValidationError):
            f.validate(arg)


class EmailFieldTest(unittest.TestCase):

    def test_emailfield_empty_nullable(self):
        f = api.EmailField(required=False, nullable=False)
        self.assertEqual(f.valid(None), True, 'value = None: required=False')
        f = api.EmailField(required=True, nullable=True)
        self.assertEqual(f.valid(""), True, 'value = "": nullable=True')

    @cases(["@", "roberthawkins@yahoo.com"])
    def test_emailfield_valid(self, arg):
        f = api.EmailField(required=True, nullable=False)
        self.assertEqual(f.valid(arg), True)

    @cases(["kins.yahoo.com", "123"])
    def test_emailfield_invalid(self, arg):
        f = api.EmailField(required=False, nullable=True)
        self.assertEqual(f.valid(arg), False)
        with self.assertRaises(api.ValidationError):
            f.validate(arg)


class PhoneFieldTest(unittest.TestCase):

    def test_phonefield_empty_nullable(self):
        f = api.PhoneField(required=False, nullable=False)
        self.assertEqual(f.valid(None), True, 'value = None: required=False')
        f = api.PhoneField(required=True, nullable=True)
        self.assertEqual(f.valid(""), True, 'value = "": nullable=True')
        self.assertEqual(f.valid(0), True, 'value = 0: nullable=True')

    @cases(["71234567890", 71234567890, 70000000000, "77777777777"])
    def test_phonefield_valid(self, arg):
        f = api.PhoneField(required=True, nullable=False)
        self.assertEqual(f.valid(arg), True)

    @cases([81234567890, "81234567890", 712345678900, "712345678900", 712345678,
            "712345678", "+71234567890", "7123456789W", [71234567890], {71234567890}])
    def test_phonefield_invalid(self, arg):
        f = api.PhoneField(required=True, nullable=False)
        self.assertEqual(f.valid(arg), False)
        with self.assertRaises(api.ValidationError):
            f.validate(arg)


class DateFieldTest(unittest.TestCase):

    def test_datafield_empty_nullable(self):
        f = api.DateField(required=False, nullable=False)
        self.assertEqual(f.valid(None), True, 'value = None: required=False')
        f = api.DateField(required=True, nullable=True)
        self.assertEqual(f.valid(""), True, 'value = "": nullable=True')

    @cases(["31.12.1234", "1.1.0001", "31.12.9999", "01.12.9999", "29.02.2000",
            datetime.today().strftime("%d.%m.%Y")])
    def test_datafield_valid(self, arg):
        f = api.DateField(required=True, nullable=False)
        self.assertEqual(f.valid(arg), True)

    @cases([12122012, "31.06.2018", "10.10.145", "18-11-2018", "18/11/2018",
            "1a.11.2018", "01.02.06.2018", "01.09"])
    def test_datafield_invalid(self, arg):
        f = api.DateField(required=True, nullable=False)
        self.assertEqual(f.valid(arg), False)
        with self.assertRaises(api.ValidationError):
            f.validate(arg)


class BirthDayFieldTest(unittest.TestCase):

    def test_birthdayfield_empty_nullable(self):
        f = api.BirthDayField(required=False, nullable=False)
        self.assertEqual(f.valid(None), True, 'value = None: required=False')
        f = api.BirthDayField(required=True, nullable=True)
        self.assertEqual(f.valid(""), True, 'value = "": nullable=True')

    @cases(["08.08.1980", datetime.today().strftime("%d.%m.%Y")])
    def test_birthdayfield_valid(self, arg):
        f = api.BirthDayField(required=True, nullable=False)
        self.assertEqual(f.valid(arg), True)

    @cases(["08.08.1880", "08.08.2080"])
    def test_birthdayfield_invalid(self, arg):
        f = api.BirthDayField(required=True, nullable=False)
        self.assertEqual(f.valid(arg), False)
        with self.assertRaises(api.ValidationError):
            f.validate(arg)


class GenderFieldTest(unittest.TestCase):

    def test_genderfield_empty_nullable(self):
        f = api.GenderField(required=False, nullable=True)
        self.assertEqual(f.valid(None), True, 'value = None: required=False')

    @cases([0, 1, 2])
    def test_genderfield_valid(self, arg):
        f = api.GenderField(required=True, nullable=False)
        self.assertEqual(f.valid(arg), True)

    @cases([3, -1, 11, "1", [1], 1.0])
    def test_genderfield_invalid(self, arg):
        f = api.GenderField(required=True, nullable=False)
        self.assertEqual(f.valid(arg), False)
        with self.assertRaises(api.ValidationError):
            f.validate(arg)


class ClientIDsField(unittest.TestCase):

    def test_clientidsfield_empty_nullable(self):
        f = api.ClientIDsField(required=False, nullable=False)
        self.assertEqual(f.valid(None), True, 'value = None: required=False')
        f = api.ClientIDsField(required=True, nullable=True)
        self.assertEqual(f.valid([]), True, 'value = "": nullable=True')

    @cases([[0, 1], [1], [1, 9, 67, 1, 1, 6], [0, 0, 0]])
    def test_clientidsfield_valid(self, arg):
        f = api.ClientIDsField(required=True, nullable=False)
        self.assertEqual(f.valid(arg), True)

    @cases([[0, -1], [0, "1"], {0, 1}, 1, "0 1 2"])
    def test_clientidsfield_invalid(self, arg):
        f = api.ClientIDsField(required=True, nullable=False)
        self.assertEqual(f.valid(arg), False)
        with self.assertRaises(api.ValidationError):
            f.validate(arg)


if __name__ == "__main__":
    unittest.main()
