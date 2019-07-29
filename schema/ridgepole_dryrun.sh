#! /bin/sh
exec bundle exec ridgepole -f Schemafile -c database.yml --merge --dry-run
