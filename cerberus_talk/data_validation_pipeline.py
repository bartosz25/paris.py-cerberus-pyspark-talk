from collections import defaultdict

from cerberus import Validator
from cerberus.errors import SchemaErrorHandler, ErrorDefinition
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
            # Here I removed the "4g" from the set just to see the failures produced by the
            # custom validation rule
            'network': {'type': 'string', 'required': True, 'network_exists': set(["adsl", "fiber_optic", "3g"])},
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
    def _validate_network_exists(self, allowed_values, field, value):
        # /home/bartosz/.virtualenvs/cerberus_talk/lib/python3.5/site-packages/cerberus/validator.py:1609: UserWarning: No validation schema is defined for the arguments of rule 'networkexists'
        #  "'%s'" % method_name.split('_', 2)[-1]
        if value not in allowed_values:
            #print('Got error for the network >> {}'.format(ErrorDefinition(0x9F, 'network_exists')))
            # other solution: self._error(field, 'not recognized network')
            # ^- but for that the error code will be 0, accordingly to the CUSTOM ErrorDefinition from cerberus.errors
            self._error(field, UNKNOWN_NETWORK, {})


class ErrorCodesHandler(SchemaErrorHandler):

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


def check_for_errors(rows):
    validator = ExtendedValidator(schema, error_handler=ErrorCodesHandler())

    def default_dictionary():
        return defaultdict(int)

    errors = defaultdict(default_dictionary)
    for row in rows:
        validator.clear_caches()
        # Note#1: I wish I could work directly on Row class but for now simply convert it recursively to a dictionary
        # Note#2: I observed that disabling normalization, which is normal if you don't have any normalization
        #         rule in your input schema, accelerates the processing time by factor of 2 (6 minutes --> 3 minutes)!
        # Note#3: calling `recursive=True` is important here because otherwise any nested object won't be validated
        #         correctly.
        validation_result = validator.validate(row.asDict(recursive=True), normalize=False)
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

    return [(k, dict(v)) for k, v in errors.items()]

# let's suppose that this schema comes from our data catalog and it's up to date
# but it can be different from the validation schema since both pipelines (crawler and validator)
# are separated. We could include them and, in that a case, implement crawling and validation
# from a Kinesis or Kafka where the record is a JSON. It would be much harder to implement in batch
# Note: I'm using here a PySpark SQL which involves an existence of the input schema. You could use here
#       RDD abstraction directly as well. I chosen PySpark SQL though because it has a richer and easier API,
#       at least for the input part.
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

# Set your file to validate here
input_file = "/home/bartosz/tmp/test_generator/2019/08/11/09/9c67a3e6-5df4-49a4-b36d-65483297d7b0.json"

# I can't use spark.read.json("/tmp/sessions/input/2019/10/22/07", lineSep='\n')****.toJSON()**** because
# Cerberus works on the dictionaries. So converting a DataFrame into a JSON string is useless.
errors_distribution = spark.read.json(input_file, schema=dataframe_schema, lineSep='\n')\
    .rdd\
    .mapPartitions(check_for_errors)\
    .reduceByKey(sum_errors_number)\
    .collectAsMap()

print('Got errors {}'.format(errors_distribution))