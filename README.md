# Mainframe Reader Plugin

[![cm-available](https://cdap-users.herokuapp.com/assets/cm-available.svg)](https://docs.cdap.io/cdap/current/en/integrations/cask-market.html)
![cdap-batch-source](https://cdap-users.herokuapp.com/assets/cdap-batch-source.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Join CDAP community](https://cdap-users.herokuapp.com/badge.svg?t=wrangler)](https://cdap-users.herokuapp.com?t=1)

# Introduction

Mainframe repo has collection of plugins that support reading and transforming mainframe generated data files. 

## COBOL Files
COBOL files are ASCII or EBCDIC fixed-width files that can contain text and/or binary data. 
You have COBOL copybook files, which describe the structure of the data, and COBOL data files, 
which contain the actual data. If your COBOL files come from a mainframe, they are in EBCDIC format. 
If your COBOL files come from a Windows machine, they are in ASCII format.

## Copybook
COBOL copybook, contains only the fields and datatypes used in the COBOL file. The plugin can directly 
import COBOL copybooks (.cpy files) as definitions for generating the target schema. Schema definition
is based on analyzing entire copybook including REDEFINES and OCCURS. Schema can be simple or complex.

## Type Mapping

| COBOL Type | PICTURE string | Example | Target Type |
|------------|----------------|---------|-------------|
| Alphabetic | A | PIC A(20) | String |
| AlphaNumeric | X (Combination of A,X, & 9) | PIC X(12) | String |
| Numeric | COMP-5 or BINARY | PIC S9 BINARY/PIC S999999 BINARY/PIC S9999999999 BINARY | Short/Integer/Long |
| Numeric | DISPLAY, COMP-3, PACKED-DECIMAL and ARITH(extend) | PIC S9(19) through S9(31) | byte[] |
| Numeric | DISPLAY, COMP-3, PACKED-DECIMAL and ARITH(extend) | S9(19) through S9(31), 9(19) through 9(31) | byte[] |
| DBCS | G, B, or N with DISPLAY-1 | PIC G(10) | String |
| National | PIC N(8) | | String |  

## Character Encodings 
The list of available character sets is determined by the Java Runtime Environment (JRE). Most of the time the 
JRE will have the character set you need. However, if you are using EBCDIC, the default character sets that come 
with the JRE do not include the EBCDIC character sets. There is no single character set for EBCDIC. Rather, 
there are EBCDIC character sets for different locales. For example, the English EBCDIC encoding is called IBM037 
or CP037. When referencing the links below that describe the character sets, the EBCDIC character sets generally 
included in those are identified as IBM, but there are many IBM character sets on the list that are not actually EBCDIC.

If your character set is not present, it's likely part of the extended characters sets that are not automatically 
installed into your Java Runtime Environment (JRE). These links list the supported character sets for JRE 5 or JRE 6. 
To install the extended character set, get the charsets.jar file, which is an option in the Java installation, 
and place it in the lib directory of your JRE. See your system administrator if you need help with this.

If the character set is not present in any of the lists, then it is invalid and needs to be changed to a value that 
is on the list.

# Prerequisites
CDAP version 6.1.x or higher. 
  
# Build
You get started with Mainframe  plugins by building directly from the latest source code. 

    $ git clone https://github.com/data-integrations/mainframe.git
    $ cd mainframe
    $ mvn clean package

After the build completes, you will have a JAR for the plugin under each ``target/`` directory.

# Deploy
You can deploy a plugin using the CDAP CLI::

  > load artifact target/mainframe-&lt;version&gt;.jar config-file target/mainframe-&lt;version&gt;.json

You can build without running tests: ``mvn clean install -DskipTests``

# License and Trademarks

Copyright © 2015-2020 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the 
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
either express or implied. See the License for the specific language governing permissions 
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.

.. |(Hydrator)| image:: http://cask.co/wp-content/uploads/hydrator_logo_cdap1.png
