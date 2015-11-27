# Bricolage Release Note

## version 5.12.5

- [fix] Ruby 2.1 does not have Etc.uname, use uname command instead.

## version 5.12.4

- [new] Supports loading from encrypted S3 data source.
- [new] New job class "createview".
- [new] Now "create" and "sql" job class support "grant" parameter.

## version 5.12.3

- [new] my-migrate job class supports sql-file parameter for export.

## version 5.12.2

- [new] td-export job class supports .sql.job file

## version 5.12.1

- [fix] ensure unlocking VACUUM lock, also when VACUUM statement was failed.

## version 5.12.0

- [new] Introduces subsystem-wise variable file (SUBSYS/variable.yml)
- [new] Allows providing default options by "defaults" global variable (e.g. enabling "grant" option by default)

## version 5.11.0

- [fix] Supports jobnet which has both a job and a jobnet

## version 5.10.0

- streaming_load: new option --sql-file
