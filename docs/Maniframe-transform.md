# Cobol to Avro Converter

Description
-----------
Cobol to Avro Converter accepts the Cobol Copybook and converts it into the Avro schema. Generated Avro schema
is used to convert the records in the Cobol data file into Apache Avro format.


Properties
----------

**copybook:** The Cobol copybook source code

**codeFormat:** Code format associated with the copybook source code

**charset:** The EBCDIC Charset used to read the data

**rdw:** Specifies whether the Cobol record starts with Record Descriptor Word

**fieldName:** Name of the field containing Cobol records in the form of array of bytes