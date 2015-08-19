SocialSensor -- The project for use of Social Media as a sensor for detection and prediction of various latent phenomena .

Copyright (C) 2015, Scott Sanner (ssanner@gmail.com) and
                    Zahra Iman (zahra.iman87@gmail.com) and Mohamed Reda BOUADJENEK <rbouadjenek@gmail.com> and Dan Nguyen <nguyenk4@onid.oregonstate.edu>


General Information
===================

LICENSE.txt:  GPLv3 license information for SocialSensorProject source and alternate
              license information for redistibuted 3rd party software

Using the code
===================
1. Change settings in config.properties file in src/main/resources/config folder
2. Main function in preprocess.spark.Preprocess builds necessary tables from raw JSON files
3. Main function in sensoreval.spark.LagTimeAnalyzer computes the lagTimes for 4 different groups A, B, C, and D which will generate the output as PARQUET files
4. Main function in postprocess.spark.PostProcessParquet reads each parquet file, converts them to CSV files, merges all iterations for each group and generates the formula to use for the final output in Excel

Dependencies
------------
json-simple: https://code.google.com/p/json-simple/
JSON.simple is a simple Java toolkit for JSON. You can use JSON.simple to encode or decode JSON text.

Twitter-text Repository: https://github.com/twitter/twitter-text
This repo is a collection of libraries and conformance tests to standardize parsing of tweet text

The other necessary libaries are added to pom.xml as maven dependencies

USAGE
-----
To be added


COPYRIGHT
---------
See LICENCE


DOCUMENTATION
-------------
Javadoc will be available soon


INSTALLATION
------------
See INSTALL


DOWNLOAD
--------
To download the latest version
git: github.com/rishirdua/twitter-lucene

AUTHORS
-------
Scott Sanner <Scott.Sanner@nicta.com.au>
Zahra Iman (zahra.iman87@gmail.com)
Mohamed Reda BOUADJENEK <rbouadjenek@gmail.com>
Dan Nguyen <nguyenk4@onid.oregonstate.edu>

To submit bugs, feature requests, submit patches
Email: Zahra Iman<imanz@onid.orst.edu>

Mailing list:
To get announcements, or join the user or dev community in other forms you need to wait now.


LEGAL
-----
See LICENCE


REFERENCES
----------
Will update if/when we get a paper

MAINTAINERS
-----------
See MAINTAIN
