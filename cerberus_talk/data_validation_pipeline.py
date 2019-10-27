from collections import defaultdict

import time
from cerberus import Validator
from cerberus.errors import SchemaErrorHandler, ValidationError, ErrorDefinition, BasicErrorHandler
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, TimestampType, DecimalType

# Cerberus validation schema. The schema doesn't contain the "user" field
# and since we don't allow unknown fields, this error will be caught and returned by Cerberus
schema = {
    'visit_id': {
        'type': 'string'
    },
    'event_time': {
        'type': 'datetime'
    },
    'user_id': {
        'type': 'integer'
    },
    'source': {
        'type': 'dict',
        'schema': {
            'site': {'type': 'string', 'required': True},
            'api_version': {'type': 'string', 'required': True}
        }
    },
    'technical': {
        'type': 'dict',
        'schema': {
            'browser': {'type': 'string', 'required': True},
            'lang': {'type': 'string', 'required': True, 'allowed': ['fr', 'pl', 'en', 'de']},
            # you don't need to create a custom validator for network_exists. You can use 'allowed'
            # but for the demo is useful to show the custom validator feature
            'network': {'type': 'string', 'required': True,
                        #'networkexists': set("adsl", "fiber_optic", "3g")
                        }, # TODO: I removed  "4g" just to see the failures
            'device': {
                'type': 'dict',
                'schema': {
                    'type': {'type': 'string', 'required': True},
                    'version': {'type': 'string', 'required': True}
                }
            }
        }
    }
}

UNKNOWN_NETWORK = ErrorDefinition(333, 'network_exists')

class ExtendedValidator(Validator):
    def _validate_networkexists(self, allowed_values, field, value):
        # /home/bartosz/.virtualenvs/cerberus_talk/lib/python3.5/site-packages/cerberus/validator.py:1609: UserWarning: No validation schema is defined for the arguments of rule 'networkexists'
        #  "'%s'" % method_name.split('_', 2)[-1]
        if value not in allowed_values:
            #print('Got error for the network >> {}'.format(ErrorDefinition(0x9F, 'network_exists')))
            # other solution: self._error(field, 'not recognized network')
            # ^- but for that the error code will be 0, accordingly to the CUSTOM ErrorDefinition from cerberus.errors
            self._error(field, UNKNOWN_NETWORK, {})


class ErrorCodesHandler(SchemaErrorHandler):
#    messages = SchemaErrorHandler.messages.copy()
#    messages[333] = "unallowed_network"

    def __call__(self, validation_errors):
        def concat_path(document_path):
            return '.'.join(document_path)

        output_errors = {}
        for error in validation_errors:
            # some validators are group validators
            # dict validator is one of them
            if error.is_group_error:
                for child_error in error.child_errors:
                    output_errors[concat_path(child_error.document_path)] = child_error.code
            else:
                output_errors[concat_path(error.document_path)] = error.code
        return output_errors
data1 = {"user_id": 450081, "visit_id": "8f6ca65f40bf47bd863e6dfaa23ea985", "technical": {"device": {"version": "Samsung Galaxy Tab S3", "type": None}, "lang": "en", "browser": "Mozilla Firefox 52", "os": "Android 8.1", "network": '4g'}, "user": {"latitude": 30.5818, "longitude": -154.4656, "ip": "8395:9313:9b9e:4e62:26dd:7688:785:4a97"}, "source": {"site": "mysite.com", "api_version": "v3"}, "event_time": "2019-10-22T08:21:19+00:00", "page": {"previous": None, "current": "article category 14-5"}}
data2 = {"user_id": 969491, "visit_id": "9a952bd082ac43679bc1cd8b8958a155", "technical": {"device": {"version": "Apple iPhone XS", "type": {"name": "smartphone"}}, "lang": None, "browser": {"language": "en", "name": "Mozilla Firefox 53"}, "os": "iOS 10", "network": "adsl"}, "user": {"latitude": -70.5207, "longitude": -166.6422, "ip": "bc1c:bed4:bc9f:72ef:13c0:777a:6747:5e17"}, "source": {"site": "partner2.com", "api_version": "v1"}, "event_time": "2019-10-22T08:21:19+00:00", "page": {"previous": None, "current": "article category 24-15"}}
"""
validator = ExtendedValidator(error_handler=ErrorCodesHandler()) #error_handler=ErrorCodesHandler())
validation_result = validator.validate(data1, schema)
for error_code, error_field in validator.errors.items():
    print('Got {}={}'.format(error_code, error_field))
#print('Got error code={} schema={} rule={} constraint={}'.format(error.code, error.schema_path, error.rule, error.constraint))
"""


def check_for_errors(rows):
    validator = ExtendedValidator(schema, error_handler=ErrorCodesHandler())

    def default_dictionary():
        return defaultdict(int)

    errors = defaultdict(default_dictionary)
    for row in rows:
        validator.clear_caches()
        # I wish I could work directly on Row class but for now simply convert it recursively to a dictionary
        #start = round(time.monotonic() * 1000)
        # TODO: I'm testing here the version with normalize to False
        # TODO: check what the normalize is doing
        # TODO: time with normalize=False, 4 cores=3 minutes which is far better than 6 minutes with normalization turned on
        # TODO: time with normalize=False, 4 cores, validator.clear_caches called every time=2 minutes  <== check if clear_caches has an impact?
        validation_result = validator.validate(row.asDict(recursive=True), normalize=False)
        #end = round(time.monotonic() * 1000)
        #print("Executed#1 within {} ms".format((end - start)))
        # By default Cerberus returns a textual representation of the errors, like:
        # {'technical': [{'device': [{'version': ['null value not allowed']}]}], 'user': ['unknown field']}
        # It's hard to use because some errors can have different "codes" because they will store
        # the invalid value in the message, like here: cerberus.errors.BasicErrorHandler#messages
        if not validation_result:
            # An error is the instance of ValidationError, for example:
            # ValidationError @ 0x7f12eab8c320 ( document_path=('user',),schema_path=(),code=0x3,constraint=None,value={'latitude': Decimal('-67'), 'ip': '28.123.52.211', 'longitude': Decimal('-46')},info=() )
            #print('Result={}/{}'.format(validator.errors, row))
            for error_field, error_code in validator.errors.items():
                # If you use a default handler, it will return a collection of strings for validator.errors
                # like {'technical': [{'browser': ['null value not allowed'], 'device': [{'version': ['null value not allowed']}]}], 'source': [{'site': ['null value not allowed']}],
                errors[error_field][error_code] += 1
        #end = round(time.monotonic() * 1000)
        #print("Executed#2 within {} ms".format((end - start)))

    result = [(k, dict(v)) for k, v in errors.items()]
    print('result={}'.format(result))
    return [(k, dict(v)) for k, v in errors.items()]

# test on 3 cores, no .asDict but a hardcoded data1 ==> 8 minutes!
# test on 3 cores, optimal version ==> 9 minutes for 99MB!

# let's suppose that this schema comes from our data catalog and it's up to date
# but it can be different from the validation schema since both pipelines (crawler and validator)
# are separated. We could include them and, in that a case, implement crawling and validation
# from a Kinesis or Kafka where the record is a JSON. It would be much harder to implement in batch
dataframe_schema = StructType(
    fields=[
        StructField("visit_id", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("user_id", IntegerType(), False),
        StructField("user", StructType(
            fields=[StructField("ip", StringType(), True),
                    StructField("latitude", DecimalType(), True), StructField("longitude", DecimalType(), True)]
        ), False),
        StructField("source", StructType(
            fields=[StructField("site", StringType(), True), StructField("api_version", StringType(), True)]
        ), False),
        StructField("technical", StructType(
            fields=[StructField("browser", StringType(), True), StructField("network", StringType(), True),
                    StructField("lang", StringType(), True),
                    StructField("device", StructType(
                        fields=[StructField("type", StringType(), True), StructField("version", StringType(), True)]
                    ), True)]
        ), False)
    ]
)


def sum_errors_number(errors_count_1, errors_count_2):
    merged_dict = {dict_key: errors_count_1.get(dict_key, 0) + errors_count_2.get(dict_key, 0) for dict_key in
                   set(errors_count_1) | set(errors_count_2)}
    return merged_dict

spark = SparkSession.builder.master("local[4]")\
    .appName("Python Spark SQL data validation with Cerberus").getOrCreate()

errors_distribution = spark.read.json("/tmp/sessions/input/2019/10/22/08/d3c226d2-68db-45be-8a1b-8326aa59816e.json", schema=dataframe_schema, lineSep='\n')\
    .rdd\
    .mapPartitions(check_for_errors)\
    .reduceByKey(sum_errors_number)\
    .collectAsMap()


print('errors_distribution={}'.format(errors_distribution))
# TODO: see how to get extra fields from the validator
"""
Notes:
I don't use DataFrame --> it has a schema and if you want to know the schema mismatches, like extra fields
and so far, it can not be enough!  <==== I was wrong. I can use a DataFrame! The assumption here is that 
I know the schema of my data but that schema is not necessarily the same as the schema used in the validation
part. Eventually, if I discover a new field, the validation schema can be updated. 

I can't use spark.read.json("/tmp/sessions/input/2019/10/22/07", lineSep='\n')****.toJSON()**** because
Cerberus works on the dictionaries. So converting a DataFrame into a JSON string is useless.

recursive=True is important!

you can handle unknown fields with the flag
"""

#errors_distribution.show()

"""
    .rdd\
    .mapPartitions(find_errors)\
    .reduceByKey(sum_errors_number).collectAsMap()
    """
