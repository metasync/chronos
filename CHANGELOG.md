# Change log

## [0.1.11] - 2023-08-16

  * Supported fully qualified table name - database.schema.table

## [0.1.8] - 2023-04-12

  * Enforced offset limit without order by when retrieving pending archive transactions

## [0.1.7] - 2023-03-30

  * Solved query performance issue in retrieving pending archivable datasets

## [0.1.6] - 2023-03-13

  * Supported optional custom database schema for Chronos related system tables
    * Database schema support depends on the actual databases are used

## [0.1.5] - 2023-03-08

  * Deprecated Dockerfiles and Gemfile to build Chronos image
  * Shorten index name for archive transaction tables

## [0.1.3] - 2023-03-07

  * Properly set chronos_archive_transaction table name for purger
  * Corrected chronos version
  * Updated citrine version
  * Revised gitignore

## [0.1.2] - 2023-03-07

  * Revised docker files to build dev / release images
  * Added Gemfile.lcok for docker build
  * Added image push in Makefile

## [0.1.0] - 2023-03-04

  * Initial release
  * Supported archive and purge
  * Used gem Sequel for datbase integration
    * MySQL and MS SQL Server are tested
