App build for converting Synapse data types to Snowflakes data types

## Usage
 - Create folder data/
 - Copy file with Synapse datatypes to file data/data.csv
 - ```docker-composer run converter```
 - Output file will be located in data/output.csv

## Prerequisites
App expects to have "data.csv" in folder /data. This file will consists all necessary info about Synapse columns. Example of file is "example-data/data.csv".

## Instalation

```docker-compose run composer install```
