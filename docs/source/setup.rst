.. _setup:

=====
Setup
=====

``Shortname for the data warehouse``

Default: DWH

Ideally a ~3 letter code.

``Google API Key filename``

Required

You'll need to use the Google API Console to create a key file. Once you have it, put it in your root directory, call it whatever you like, and pass the file name in here.

``The Google Account you will use to manage the DWH``

Required

The setup module will create your Google Sheets under the API user name. To be able to access them via the Google web interface you need to share them with your Google account (i.e. your email address)

``Admin Postgres DB username``
``Admin Postgres DB password``

Required

To create the required databases, BETL must be able to connect to the ``postgres`` database of an already-running Postgres server.

You can run Postgres locally.

``Temp data path``

Default: tmp_data

Where the tempoary data (csv files) are stored.

``Source data path``

Default: src_data

Where BETL expects to find any local source data

``Reports path``

Default: reports

Where BETL will save reporting output

``Logs path``

Default: logs

Where BETL will save logging output

``Control DB - host name``
``Control DB - database name``
``Control DB - username``
``Control DB - password``

Default: localhost, ctl_db, <same as the admin u/n & p/w>

The control database is used by BETL to manage and audit executions of your pipeline

``Schema description - ETL Google Sheets title``
``Schema descriptions - TRG Google Sheets title``

Kimbal data pipelines require carefully controlled schemas. You need to define the schemas in two separate Google Sheets documents.

- ETL schemas: intermediary datalayers that the data will work its way through on route to the target data layers (you will always need a source data layer (which can be auto-populated), any additional ETL data layers are optional)
- TRG schemas: target datalayers from which the end users and analytical applications will read

``ETL DB - host name``
``ETL DB - database name``
``ETL DB - username``
``ETL DB - password``

Default: localhost, ctl_db, <same as the admin u/n & p/w>

The ETL database is where the data is stored while it is transformed

``TRG DB - host name``
``TRG DB - database name``
``TRG DB - username``
``TRG DB - password``

Default: localhost, ctl_db, <same as the admin u/n & p/w>

The TRG database is the data warehouse itself: this is the database that holds your target model and where users access the data

``Default Rows - Google Sheets title``

This spreadsheet holds default rows for each of your dimensions

``Master Data Mappings - Google Sheets title``

if you have any master data mappings to apply, this is the Google Sheet that holds the mappings. There is a MDM method in the DataFlow class which, when you first run it, will populate the "left hand side" of the mapping automatically. You only need to create and name the worksheets.

``Create your directories? (Y/N)``

Creates (and will overwrite too) the necessary directories in your root

``Create your .gitignore file? (Y/N)``

Does what it says on the tin. Adds the necessary entries to ignore logs and data etc.

``Create your appConfig.ini file? (Y/N)``

Sets you up with a standard configuration

``Create your main.py script? (Y/N)``

Again, a standard configuration

``Create an example dataflow script? (Y/N)``

It doesn't do anything useful, but it can be helpful to understand how to get started writing your own pipeline functions

``Create three Postgres databases: Ctrl, ETL and Target? (Y/N)``
``Create two empty schema description Google Sheets: ETL and TRG? (Y/N)``
``Create an empty MDM (master data mapping) Google Sheet: (Y/N)``
``Create an empty default rows Google Sheet: (Y/N)``

Create the databases and spreadsheets that we have given the config details for above
