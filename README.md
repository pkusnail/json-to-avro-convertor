# json-to-avro-convertor
Avro schema auto adaptor for json data, which can be used to convert json formatted data to avro format or parquet format without pre-defined avro schema.

## Advantage
* Convert json to avro or parquet smoothly without pre-defined schema
* Better support the online schema change ,because it generates avro schema according to the json data, we can reduce the mannual operation.

## Disadvantage
* Field type not precisely matched,use default types. For example, 1.0 can be float or double ,the default type is double.
* Avro map type NOT supported, because it looks the same with avro record,but the key and key value pairs are variant.


# Test Case
The test case shows how to convert json to avro, and it is the same for parquet.
If you want to use it as a jar lib, use command "mvn assembly:assembly"

# Field Type Conversion Table
avro |example |json | json-to-avro-convertor
---|---|---|---
null | null | null| null
boolean | TRUE| boolean| boolean 
int,long | 1 | integer| long
float,double | 1.1| number| double
bytes | "\u00ff" | string | string
string| "foo" |string| string
record | {"a":1}| object| record 
enum | "FOO" | string| string
array | [1,2]| array | array
map | {"a":1} | object | unsupported!
fixed | "\u00ff" | string | string 

