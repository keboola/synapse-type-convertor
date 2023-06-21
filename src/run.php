<?php
require_once __DIR__ . '/../vendor/autoload.php';

use  Keboola\SynapseTypesToBasetypes\RowConverter;

const INPUT_FILE = __DIR__ . '/../data/data.csv';
const OUTPUT_FILE = __DIR__ . '/../data/output.csv';

$debug = false;
if (in_array('--debug', $argv)) {
    $debug = true;
}

$csv = new Keboola\Csv\CsvReader(INPUT_FILE);
(new \Symfony\Component\Filesystem\Filesystem())->remove(OUTPUT_FILE);
$outputCsv = new Keboola\Csv\CsvWriter(OUTPUT_FILE);
$outputHeader = array_merge($csv->getHeader(), [
    'snowflake_sql_definition',
    'snowflake_type',
    'snowflake_length'
]);
$outputCsv->writeRow($outputHeader);

foreach ($csv as $index => $row) {
    //skip header
    if ($index == 0) {
        continue;
    }

    $convertedRow = new RowConverter($row);

    if ($convertedRow->getDatatype() == 'sysname') {
        $convertedRow->setType('varchar')->setLength(0);
    }


    try {
        $outputCsv->writeRow(
            array_merge(
                $row,
                [
                    $convertedRow->getSnowflakeDefinition()->getSQLDefinition(),
                    $convertedRow->getSnowflakeDefinition()->getType(),
                    $convertedRow->getSnowflakeDefinition()->getLength()
                ]
            )
        );
    } catch (\Keboola\Datatype\Definition\Exception\InvalidTypeException $e) {
        echo "{$e->getMessage()} for row " . implode(',', $row) . "\n";
    } catch (\Throwable $e) {
        echo "{$e->getMessage()} for row " . implode(',', $row) . "\n";
    }


    if ($debug && $index > 1000) {
        break;
    }
}




