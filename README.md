# Mainframe Reader Plugin

[![cm-available](https://cdap-users.herokuapp.com/assets/cm-available.svg)](https://docs.cdap.io/cdap/current/en/integrations/cask-market.html)
![cdap-batch-source](https://cdap-users.herokuapp.com/assets/cdap-batch-source.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Join CDAP community](https://cdap-users.herokuapp.com/badge.svg?t=wrangler)](https://cdap-users.herokuapp.com?t=1)

Introduction
============

Mainframe Reader plugin is a source plugin for Hydrator that can read EBCDIC data format. It uses JRecord and Cobol copy book input format to read mainframe data. 
To use the plugin the binary mainframe file and the copybook is needed. Various EBCDIC CCSIDs (Coded Character Set Identifiers) are supported:
 - EBCDIC US (CCSID 037)
 - EBCDIC Germany (CCSID 237)
 - EBCDIC-Arabic (CCSID 420)
 - EBCDIC-Denmark, Norway (CCSID 277)
 - EBCDIC-France (CCSID 297)
 - EBCDIC-Greece (CCSID 875)
 - EBCDIC-International (CCSID 500)
 - EBCDIC-Italy (CCSID 280)
 - EBCDIC-Russia (CCSID 410)
 - EBCDIC-Spain (CCSID 283)
 - EBCDIC-Thailand (CCSID 838)
 - EBCDIC-Turkey (CCSID 322)


Getting Started
===============

Prerequisites
-------------
CDAP version 6.1.x or higher. 
  
Building Plugins
----------------
You get started with Mainframe reader plugins by building directly from the latest source code. However, you need to 
download the JRecord and cb2xml libraries from [sourceforge](https://sourceforge.net/projects/jrecord/) and place them 
the ``repo`` directory before building the plugin. This is because these libraries are not yet available through the 
public maven repository at Maven Central. The plugin is currently tested with version 0.90.2 of the JRecord library, so 
please download that version. 

    $ git clone https://github.com/data-integrations/mainframe-reader.git
    $ cd mainframe-reader
    <Download and place the JRecord libraries in the repo/ directory>
    $ mvn clean package

After the build completes, you will have a JAR for the plugin under each ``target/`` directory.

Deploying Plugins
-----------------
You can deploy a plugin using the CDAP CLI::

  > load artifact <target/plugin-jar> config-file <resources/plugin-config>

  > load artifact target/mainframe-reader-<version>.jar \
         config-file target/mainframe-reader-<version>.json

You can build without running tests: ``mvn clean install -DskipTests``

Mailing Lists
-------------
CDAP User Group and Development Discussions:

- `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`__

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from 
users, release announcements, and any other discussions that we think will be helpful 
to the users.

IRC Channel
-----------
CDAP IRC Channel: #cdap on irc.freenode.net


License and Trademarks
======================

Copyright © 2015-2019 Cask Data, Inc.

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
