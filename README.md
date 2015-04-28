# Clearcare DynamoDB common configuration

<!-- [![](https://ci.solanolabs.com:443/Clearcare/cc_dynamodb/badges/170035.png?badge_token=b6bcffb6661e4901c3046ae459eda6a3e0f2fce9)](https://ci.solanolabs.com:443/Clearcare/cc_payment_service/suites/170035) -->

Example usage:

```python
import random
from cc_dynamodb import cc_dynamodb

cc_dynamodb.set_config(
    aws_access_key_id='<KEY>',
    aws_secret_access_key='<SECRET>',
    namespace='dev_')

table = cc_dynamodb.get_table('employment_screening_reports')
    # Returns the boto Table object
    # after figuring out the DynamoDB table name (via namespace)
    # and loading the schema and indexes from the config.
table.scan()  # Table uses boto dynamodb2 interface
```

# TODO:

* Update tables to match indexes/throughput, if they already exist
* Easier configuration setup (especially for testing)
* Allow external repos to mock without needingo install `moto`
