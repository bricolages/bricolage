# Bricolage Release Note

## version 5.17.1

- [fix] --dry-run option did not work for my-import job class.
- [new] AWS access key id & secret key are now optional for S3 data sources (to allow using EC2 instance or ECS task attached IAM role)

## version 5.17.0

- [new] Supports Redshift attached IAM role for COPY and UNLOAD.

## version 5.16.9

- code-level change only: [new] new method Transaction#truncate_and_commit

## version 5.16.8

- code-level change only

## version 5.16.7

- [fix] require 'bricolage/context' wrongly caused NameError.
- [new] PostgresConnection#drop_table_force utilizes DROP TABLE IF EXISTS.

## version 5.16.6

- rebuild-rename, rebuild-drop, my-import, my-migrate, create, createview: Reduces the number of transactions for speed.

## version 5.16.5

- [fix] my-import: mys3dump: Fixes buffer size problem.
- [fix] my-import: mys3dump: Escapes more meta characters (e.g. \n, \r, \v, ...).

## version 5.16.4

- [fix] Adds dependency to rake
- [fix] my-import: Reduces warning log messages.

## version 5.16.3

- [fix] streaming_load: Disables statupdate for log staging table, it is useless.

## version 5.16.2

- [fix] streaming_load: Disables compupdate on COPY.   This *might* cause Assert error on some clusters.

## version 5.16.1

- [fix] Fixes syntax error on ruby 2.1

## version 5.16.0

- [CHANGE][EXPERIMENTAL] streaming_load: Always reuse same temporary log table xxxx_l_wk instead of temporary xxxx_l_tmpNNNN.  This *might* cause Redshift DDL slow down, I try to reduce the number of drop-create.

## version 5.15.2

- [fix] redis-export: remove un-required error check.

## version 5.15.1

- [new] redis-export: make faster using cursor and Redis pipeline.

## version 5.15.0

- [new][EXPERIMENTAL] new job class redis-export.

## version 5.14.0

- [new] streaming_load: Fast log check by temporary load log table.
- [new] streaming_load: Ignores all S3 key-does-not-exist errors; they are caused by S3 eventual consistency.

## version 5.13.1

- [fix] load, streaming_load: "encrypted" load option should not be used for SSE-KMS

## version 5.13.0

- [new] streaming_load: Supports S3 server-side encryption with AWS KMS (Key Management Service).
- Now Bricolage requires Ruby AWS-SDK v2 for AWS signature v4.

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

