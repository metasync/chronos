# Change log

## [0.1.0] - 2023-03-04

  * Initial release
  * Supported archive and purge
  * Used gem Sequel for datbase integration
    * MySQL and MS SQL Server are tested

## [0.1.2] - 2023-03-07

  * Revised docker files to build dev / release images
  * Added Gemfile.lcok for docker build
  * Added image push in Makefile

## [0.1.3] - 2023-03-07

  * Properly set chronos_archive_transaction table name for purger
  * Corrected chronos version
  * Updated citrine version
  * Revised gitignore

## [0.1.4] - 2023-03-08

  * Deprecated Dockerfiles and Gemfile to build Chronos image