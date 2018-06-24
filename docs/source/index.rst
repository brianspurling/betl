.. toctree::
   :hidden:
   :titlesonly:

   setup
   schedule-config
   command-line


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
- Data: schema descriptions, master data mappings, default rows

By FAR the easist way to setup all the config is through BETL's built-in setup module. In advance you only need to get a Google API Key file and set up a Postgres database (localhost is fine). Then...

Create the root directory for your data pipeline repo, then run::

  $ python
  >>> import betl
  >>> betl.setup()

... and follow the instructions (:ref:`more help here <setup>`)

Configuring Source Systems
==========================

After setting up BETL, you need to tell it where your source data is.

Editing the newly created ``appConfig.ini`` file and replace the template source system configs with your actual source system(s) config.

BETL currently supports the following source system datastore types:

- Postgres
- SQLLite
- Deliminated text files
- Google Sheets
- Excel (xlsx)

Extracting your source data
===========================

As instructed at the end of ``betl.setup()``, you are now one command away from having your source data extracted into your ETL database and your SRC schema descriptions defined in a Google Sheet.

To get there you just need to run::

  $ python main.py readsrc rebuildall bulk run

This does three things:

  - ``readsrc``   auto populates your SRC schema description spreadsheet with an exact copy of your source system(s) schema(s)
  - ``rebuildall``   creates your physical schemas (i.e. database tables) in your ETL and TRG postgres databases (of course, you haven't defined much of your DWH schemas yet, so this is really just creating the SRC data layer tables, and a couple of default target dimensions)
  - ``bulk run``   executes your data pipeline.  Without any bespoke code yet, all this will do is extract the source data from the source system(s) and load it into your ETL database (and create a couple of default target dimensions)

When you run it again, you just need the last bit::

  $ python main.py bulk run

Check out the full list of commands in the help (:ref:`more details here <command-line>`)::

  $ python main.py help

Adding functions to your data pipeline
======================================

Check out the ``main.py`` script that was created by ``betl.setup()``. It contains a ``scheduleConfig`` object (full documentation :ref:`here <schedule-config>`).

This scheduleConfig object contains four lists:

  - EXTRACT_DATAFLOWS
  - TRANSFORM_DATAFLOWS
  - LOAD_DATAFLOWS
  - SUMMARISE_DATAFLOWS

Add your pipeline functions to these lists and BETL will execute them in sequence.

Each of your data pipeline functions must take a single argument: an instance of the ``Betl()`` class that we instantiate at the end of ``main.py``.

So scheudleConfig looks like this::

  scheduleConfig = {

      ...

      'EXTRACT_DATAFLOWS': [],
      'TRANSFORM_DATAFLOWS': [
        examplePipelineFunction
      ],
      'LOAD_DATAFLOWS': [],
      'SUMMARISE_DATAFLOWS': []
  }

And ``examplePipelineFunction`` looks like this::

  def examplePipelineFunction(betl):

    # Your pipeline code here

Manipulating data in your pipeline functions
============================================

To benefit from the full power of the BETL framework, all data manipulation within your pipeline functions should be performed via a ``DataFlow()`` object.

You start by telling the dataflow object what data to read from disk. Then you transform it. Then you write it back to disk. The next dataflow can pick up the output of the previous dataflow, and so on.

Thus the common pattern for a pipeline functions is::

  def examplePipelineFunction(betl):

      # Instantiate an instance of the DataFlow class
      dfl = betl.DataFlow(desc='An example pipeline function')

      # Read some data into the DataFlow object
      dfl.read(tableName='src_example_src_data', dataLayer='SRC')

      # Perform some transformations on the data, via the DataFlow object
      dfl.renameColumns(
          dataset='src_example_src_data',
          columns={'col_to_rename': 'new_column_name'},
          desc='Rename the column')

      # Write the databack to disk (so it is available for the next DataFlow to read
      dfl.write(
          dataset='src_example_src_data',
          targetTableName='example_staging_data',
          dataLayerID='STG')

Each "step" (each DataFlow method called) must be given a description that is unique within the dataflow. And each dataflow must be given a description that is unique across the whole pipeline. This allows us to compare stats across executions.
