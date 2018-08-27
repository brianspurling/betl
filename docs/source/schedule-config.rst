.. _schedule-config:

===============
Schedule Config
===============

You pass a schedule config object into the ``betl.init`` function. This is a standard Python dictionary.

A standard schedule config looks like this::

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
      'EXT_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXT': [],
      'BSE_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD': [],

      # Here you define the bespoke parts of your data pipeline.
      # Pass in your app's functions to the following four lists,
      # in the order you want them executed
      'EXTRACT_DATAFLOWS': [],
      'TRANSFORM_DATAFLOWS': [],
      'LOAD_DATAFLOWS': [],
      'SUMMARISE_DATAFLOWS': []
  }

DEFAULT_EXTRACT
---------------

NB. Delta loads not working in current version

Using BETL's default extract can save you a lot of time.

Extracts vary depending whether you're running a bulk or delta load.

A bulk extract will extract all the source data; a delta extract will only extract data this is new or has changed since the last load.

Delta extracts are determined using full table comparisons. You should have specified the natural key of each source table in your source schema description spreadsheet.

If a new natural key is found in the source, it's considered a new row. If an already-loaded natural key is found with one or more of the other columns changed, it's considered an updated row. If an already-loaded natural key is not found, it's considered a deleted row.

DEFAULT_TRANSFORM
-----------------

The transform stage is the most bespoke and therefore is the most reliant on your own application code. However, one thing that can be automated is the processing of the audit dimension's natural keys on the fact tables.

DEFAULT_LOAD
------------

NB. Delta loads not working in current version

Like the extract stage, the load stage can be easily automated. To have your transformed datasets picked up the default load, call dfl.prepForLoad(). This will place a CSV file in the LOD datalayer named the same as the target table.

The default load picks up each of these files and loads them into the target model (bulk or delta). It loads the dimensions first, immediately updating the SK mappings after load. Then it loads the facts, looking up the SKs from the just-updated mappings.

Throughout, it deals carefully with indexes to improve performance.

DEFAULT_SUMMARISE
-----------------

The summarise is another mainly bespoke stage, however if you finish your bespoke code by writing files that match exactly the summary table names, BETL will auto load them for you and deal with the indexes to maximise performance.

DEFAULT_DM_DATE
---------------

DEFAULT_DM_AUDIT
----------------

EXT_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXT
--------------------------------------

BSE_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD
---------------------------------------

EXTRACT_DATAFLOWS
-----------------

TRANSFORM_DATAFLOWS
-------------------

LOAD_DATAFLOWS
--------------

SUMMARISE_DATAFLOWS
-------------------
