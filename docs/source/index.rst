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

By FAR the easist way to setup all the config is through BETL's built-in setup module. In advance you only need to:

 - get a Google API Key file
 - set up a Postgres database (localhost is fine)
 - create the root directory for your data pipeline repo and cd into it

Then run::

  $ python
  >>> import betl
  >>> betl.setupBetl()

And follow the instructions (:ref:`more help here <setup>`)

Configuring Source Systems
==========================

After completing the setup process you'll need to tell BETL where your source data is.

Edit the newly created ``appConfig.ini`` file and replace the template source system configs with your actual source system(s) config.

BETL currently supports the following source system datastore types:

- Postgres
- SQLLite
- Deliminated text files
- Google Sheets
- Excel (xlsx)

Extracting your source data
===========================

You are now one command away from having your source data extracted into your ETL database and your EXT schema descriptions defined in a Google Sheet.

To get there you need to run::

  $ python main.py readsrc rebuildall bulk run

This does three things:

  - ``readsrc``   auto populates your EXT schema description spreadsheet with an exact copy of your source system(s) schema(s)
  - ``rebuildall``   creates your physical schemas (i.e. database tables) in your ETL and TRG postgres databases (of course, you haven't defined much of your DWH schemas yet, so this is really just creating the EXT datalayer, and a couple of default target dimensions in your BSE datalayer)
  - ``bulk run``   executes your data pipeline.  Without any bespoke code yet, all this will do is extract the source data from the source system(s) and load it into your ETL database (and create a couple of default target dimensions)

Note, when you run your extract again you just need the last bit::

  $ python main.py bulk run

Command Line Arguments
======================

You can control quite a lot of what BETL does in each execution from the command line.

Check out the full list of command line arguments in the help (:ref:`more details here <command-line>`)::

  $ python main.py help

Adding functions to your data pipeline
======================================

So far our pipeline doesn't do much - it just extracts our source data and creates a default date dimension. The next step is to start adding bepsoke data pipeline functions: i.e. the actual code that will transform your data.

Take a look at the ``main.py`` script that was created by ``betl.setupBetl()``. It contains a ``scheduleConfig`` object (full documentation :ref:`here <schedule-config>`).

This scheduleConfig object contains four lists:

  - EXTRACT_DATAFLOWS
  - TRANSFORM_DATAFLOWS
  - LOAD_DATAFLOWS
  - SUMMARISE_DATAFLOWS

Add your pipeline functions to these lists and BETL will execute them in sequence.

Each of your data pipeline functions must take a single argument: an instance of the ``Pipeline()`` class that we instantiate at the end of ``main.py``.

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

You can instantiate a new DataFlow object from the Betl object passed into you pipeline function.

Then you tell the DataFlow object what data to read from disk. Then you transform it. Then you write it back to disk.

The next dataflow can pick up the output of the previous dataflow, and so on.

Thus the common pattern for a pipeline functions is::

  def examplePipelineFunction(betl):

      # Instantiate an instance of the DataFlow class
      dfl = betl.DataFlow(desc='An example pipeline function')

      # Read some data into the DataFlow object
      dfl.read(tableName='example_src_data', dataLayer='EXT')

      # Perform some transformations on the data, via the DataFlow object
      dfl.renameColumns(
          dataset='example_src_data',
          columns={'col_to_rename': 'new_column_name'},
          desc='Rename the column')

      # You can have as many steps as you like...
      dfl.do_some_more_stuff_to_the_data()
      # ...

      # Write the databack to disk (so it is available for the next DataFlow to read
      dfl.write(
          dataset='example_src_data',
          targetTableName='example_staging_data',
          dataLayerID='TRN')

Each "step" (each DataFlow method called) must be given a description that is unique within the dataflow. And each dataflow must be given a description that is unique across the whole pipeline. This allows us to compare stats across executions.

Approach
========

Some thoughts on how to approach building a robust data pipeline:

  - Build the dimensions first, then go back to the source data and build the facts
  - But: always have a CLN layer that preceeds both - that way you don't end up repeating baisc basic cleaning logic
  - In the absence of a built in testing framework, write SQL scripts to compare EXT to TRG as best as possible (temporarily pipe EXT tables into BSE to make this easier, if necessary).
  - Again, though, you don't want to have to repeat cleaning logic. So if you have a CLN datset (you should do), test the EXT->CLN step, then test the CLN->BSE step
