.. toctree::
   :hidden:
   :titlesonly:

   schedule-config

===================================================================
BETL: A Python framework, for robust, Kimball-esque data pipelining
===================================================================

--------
Overview
--------

[Placeholder]

-----------------
Quick Start Guide
-----------------

Configuring BETL
================

BETL requires considerable configuration in order to automate as much of your pipeline application as possible.

This config comes from the following sources:

- App config: static configuration, which betl loads from a config file
- Schedule config: dynamic configuration, which your main script will pass to betl on init
- Parameters: run-time parameters, which you specify on the command line

- Data:
  - Schema descriptions
  - Master data mappings
  - Default rows

The rest of this quick start guide takes you through these config setups.

Databases
=========

You need to create three Postgres databases:

- Control DB: This is BETL's config and logging database
- ETL DB: This is the database that will act as the persistent storage for your data pipelining. It will only be accessed by your pipeline application.
- Target DB: This is the database that will hold your data warehouse. This is written to by your data pipeline application and read from by end users / analytical applications

``sudo -u <user> createdb <dwhId>_etl``
``sudo -u <user> createdb <dwhId>_trg``

Schema Description Google Sheets
================================

Kimbal data pipelines require carefully controlled schemas. You need to define the schemas in two separate Google Sheets documents.

- ETL schemas: intermediary datalayers that the data will work its way through on route to the target data layers (you will always need a source data layer (which can be auto-populated), any additional ETL data layers are optional)
- TRG schemas: target datalayers from which the end users and analytical applications will read

Create these Google Sheets, and use the Google API Console to get an API Key file. Share the Google Sheets with your api account.

MDM and Default Rows
====================

*Optional*

- MDM (Master Data Mappings): if you have any master data mappings to apply, create a separate Google Sheet to hold these
- Default Rows: if you would like to add default rows to your dimensions, create a separate Google Sheet to hold these

Source Systems
==============

BETL currently supports the following source system datastore types:

- Postgres
- SQLLite
- Deliminated text files
- Google Sheets
- Excel (xlsx)


Starting your data pipeline app: main.py
========================================

Create a new directory (this will be your data pipeline application), and create a ``main.py`` script in the root.

This script sets up your scheduleConfig, initialises a Betl instance, and runs your job

For detailed documentation of the scheduleConfig object: ?? ::

  from betl import Betl
  import sys

  scheduleConfig = {}

  betl = Betl(appConfigFile='./appConfig.ini',
              scheduleConfig=scheduleConfig,
              runTimeParams=sys.argv)

  betl.run()


App Config
==========

 Create an ``appConfig.ini`` file in your root::

   [ctrl]

     DWH_ID = <short code to identify the data warehouse>
     TMP_DATA_PATH = tmp_data/
     REPORTS_PATH = reports/
     LOG_PATH = logs/

     [[ctl_db]]

       HOST =
       DBNAME =
       USER =
       PASSWORD =

   [data]

     [[schema_descs]]

       GSHEETS_API_URL = 'https://spreadsheets.google.com/feeds'
       GSHEETS_API_KEY_FILE = '<filename of your API key file>'
       ETL_FILENAME = ''
       TRG_FILENAME = ''

     [[dwh_dbs]]

       [[[ETL]]]

         HOST =
         DBNAME =
         USER =
         PASSWORD =

       [[[TRG]]]

         HOST =
         DBNAME =
         USER =
         PASSWORD =

     [[default_rows]]

       GSHEETS_API_URL = 'https://spreadsheets.google.com/feeds'
       GSHEETS_API_KEY_FILE = '<filename of your API key file>'
       FILENAME = ''

     [[mdm]]

       TYPE = GSHEET
       GSHEETS_API_URL = 'https://spreadsheets.google.com/feeds'
       GSHEETS_API_KEY_FILE = '<filename of your API key file>'
       FILENAME = ''

     [[src_sys]]

       [[[SQLLITE_EXAMPLE]]]
         TYPE = SQLITE
         PATH = 'src_data/'
         FILENAME = ''

       [[[FILE_SYSTEM_EXAMPLE]]]
         TYPE = FILESYSTEM
         PATH = 'src_data/'
         FILE_EXT = '.csv'
         DELIMITER = ','
         QUOTECHAR = '"'

       [[[GSHEET_EXAMPLE]]]
         TYPE = GSHEET
         GOOGLE_SHEETS_API_URL = 'https://spreadsheets.google.com/feeds'
         GOOGLE_SHEETS_API_KEY_FILE = '<filename of your API key file>'
         FILENAME = ''

       [[[EXCEL_EXAMPLE]]]
         TYPE = EXCEL
         PATH = 'src_data/'
         FILENAME = ''
         FILE_EXT = '.xlsx'

Schedule Config
===============

In ``main.py``, replace your scheduleConfig object with the following::

  scheduleConfig = {

      # Control whether default transformations should be run
      'DEFAULT_EXTRACT': True,
      'DEFAULT_TRANSFORM': True,
      'DEFAULT_LOAD': True,
      'DEFAULT_SUMMARISE': True,

      # Data BETL can generate itself
      'DEFAULT_DM_DATE': True,
      'DEFAULT_DM_AUDIT': True,

      # Define tables to exclude from default processing
      'SRC_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXT': [],
      'TRG_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD': [],

      # Here you define the bespoke parts of your data pipeline.
      # Pass in your app's functions to the following four lists,
      # in the order you want them executed
      'EXTRACT_DFS': [],
      'TRANSFORM_DFS': [],
      'LOAD_DFS': [],
      'SUMMARISE_DFS': []
  }

BETL Setup
==========

A few bits need to be setup before BETL can run (e.g. the Control database). So execute your application's main.py with the following parameters::

  python main.py setup

Auto-populate your source data layer schema
============================================

The first thing BETL does is extract all the data from your source systems and saves it in the source datalayer (in the ETL database).

To do this, BETL needs the source datalayer schema defined. You can define this yourself, and thus choose which tables/columns to extract.

Alternatively, BETL can auto-populate the source datalayer schema description by copying the schema of the source system(s). To do this, execute your application's main.py with the following parameters::

  python main.py readsrc

Build your physical schema
==========================
::
  python main.py rebuildall

Running Your Pipeline
=====================

Execute main.py with the following parameters::

  python main.py bulk run

Default Links
=============

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
