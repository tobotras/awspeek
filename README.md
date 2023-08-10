# awspeek

Run regex search against data. Data storage supported:
  1. local file (plain text or gzipped),
  2. AWS S3 (plain text or gzipped),
  3. AWS RDS (PostgreSQL),
  4. local PostgreSQL.

For AWS auto-discovers all S3 buckets and files, all RDS databases and tables.

## Installation

1. Install JDK, clojure, leiningen.
2. Run "lein uberjar"
3. Set up PostgreSQL server:
    1. Create DB `awspeek`
    2. Initialize DB:
        1. `psql -d postgres < db-init.sql`
        2. `psql -d ximi -U ximi < data-init.sql`

`data-init.sql` contains sample regex set, edit freely.

`testdb-init.sql` contains example test data, load to target PostgreSQL (local or AWS RDS-managed).

## Usage

    $ java -jar target/uberjar/awspeek-0.1.0-SNAPSHOT-standalone.jar [args]

Log is appended to `MATCHES` table.

## Options

      -v, --verbose          0     Verbosity
      -s, --aws-s3                 Process AWS S3 storage
      -r, --aws-rds                Process AWS RDS tables
      -f, --file FILE              Proces local text file
      -d, --dbname DATABASE        PostgreSQL database name
      -h, --host HOSTNAME          PostgreSQL server host
      -p, --port PORT        5432  PostgreSQL server port
      -u, --user USERNAME          PostgreSQL server username
      -w, --password PASS          PostgreSQL server password

## Environment

For AWS access `AWS_PROFILE` variable should be set and refer to existing profile
in `~/.aws/config` and `~/.aws/credentials`. 

### Bugs

It's only proof of concept, so it may format your disk and kill you dog too.

## License

Copyright © 2023 Boris Tobotras

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
