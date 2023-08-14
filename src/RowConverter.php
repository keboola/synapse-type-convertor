<?php

namespace Keboola\SynapseTypesToBasetypes;

use Keboola\Datatype\Definition\Snowflake;
use Keboola\Datatype\Definition\Synapse;

class RowConverter
{
    private array $row;

    private string $database;

    private string $schema;

    private string $table;

    private int $columnOrdinal;

    private string $columnName;

    private string $dataType;

    private int $length;

    private int $precision;

    private int $isNullable;

    private int $isPrimaryKey;

    private int $primaryKeyOrdinal;

    private Synapse $synapseDatatype;

    private int $toWholeNumber;

    private int $scale;

    public function __construct(array $row)
    {
        $this->row = $row;
        $this->database = $this->getNonEmptyData(3, $row);
        $this->schema = $this->getNonEmptyData(5, $row);
        $this->table = $this->getNonEmptyData(6, $row);
        $this->columnOrdinal = (int)$this->getNonEmptyData(7, $row);
        $this->columnName = $this->getNonEmptyData(8, $row);
        $this->toWholeNumber = $this->getNonEmptyData(10, $row);
        $this->dataType = $this->getNonEmptyData(11, $row);
        $this->length = (int)$this->getNonEmptyData(12, $row);
        $this->precision = (int)$this->getNonEmptyData(13, $row);
        $this->scale = (int)$this->getNonEmptyData(14, $row);
        $this->isNullable = (int)$this->getNonEmptyData(15, $row);
        $this->isPrimaryKey = (int)$this->getNonEmptyData(16, $row);
        $this->primaryKeyOrdinal = (int)$this->getNonEmptyData(17, $row);
    }

    public function getDatatype(): string
    {
        return $this->dataType;
    }

    private function setSynapseDatatype(): void
    {
        $this->synapseDatatype = new Synapse($this->dataType);
    }

    public function setType(string $type): RowConverter
    {
        $this->dataType = $type;
        return $this;
    }

    public function setLength(int $length): RowConverter
    {
        $this->length = $length;
        return $this;
    }

    public function getSnowflakeDefinition(): Snowflake
    {
        $this->setSynapseDatatype();
        $def = match (strtoupper($this->synapseDatatype->getType())) {
            //NVARCHAR -> VARCHAR(n) or VARCHAR(MAX)
            Synapse::TYPE_NVARCHAR => function () {
                if ($this->length/2 == Synapse::MAX_LENGTH_NVARCHAR) {
                    return new Snowflake(Snowflake::TYPE_VARCHAR);
                } else if ($this->length <= 0) {
                    return new Snowflake(Snowflake::TYPE_VARCHAR);
                } else {
                    return new Snowflake(Snowflake::TYPE_VARCHAR, ['length' => $this->length / 2]);
                }
            },
            //NVARCHAR -> VARCHAR(n) or VARCHAR(MAX)
            Synapse::TYPE_VARCHAR => function () {
                if ($this->length < 1) {
                    return new Snowflake(Snowflake::TYPE_VARCHAR);
                }
                return new Snowflake(Snowflake::TYPE_VARCHAR, ['length' => $this->length]);
            },
            //float -> float or whole number
            Synapse::TYPE_FLOAT, Synapse::TYPE_REAL => function () {
                if ($this->toWholeNumber) {
                    return new Snowflake(
                        Snowflake::TYPE_NUMBER, [
                            'length' => [
                                'numeric_precision' => '38',
                                'numeric_scale' => '0'
                            ]
                        ]
                    );
                } else {
                    return new Snowflake(
                        Snowflake::TYPE_NUMBER, [
                            'length' => [
                                'numeric_precision' => 38,
                                'numeric_scale' => 18
                            ]
                        ]
                    );
                }
            },
            //numeric
            Synapse::TYPE_NUMERIC, Synapse::TYPE_DECIMAL => function() {
                return new Snowflake(
                    Snowflake::TYPE_NUMBER, [
                        'length' => [
                            'numeric_precision' =>  $this->precision,
                            'numeric_scale' => $this->scale
                        ]
                    ]
                );
            },
            //datatimes
            Synapse::TYPE_DATETIME, Synapse::TYPE_DATETIME2 => function () {
                if ($this->scale == 0) {
                    return new Snowflake(Snowflake::TYPE_DATETIME);
                }
                return new Snowflake(Snowflake::TYPE_DATETIME, ['length' => $this->scale]);
            }
            ,
            Synapse::TYPE_CHAR => function () {
                return new Snowflake(Snowflake::TYPE_CHAR, ['length' => $this->length]);
            }
            ,
            Synapse::TYPE_BIT => function () {
                return new Snowflake(Snowflake::TYPE_BOOLEAN);
            },
            Synapse::TYPE_BINARY => function () {
                return new Snowflake(Snowflake::TYPE_BINARY);
            },
            Synapse::TYPE_MONEY => function () {
                return new Snowflake(
                    Snowflake::TYPE_NUMBER, [
                        'length' => [
                            'numeric_precision' =>  19,
                            'numeric_scale' => 4
                        ]
                    ]
                );
            },
            Synapse::TYPE_TIME => function () {
                return new Snowflake(Snowflake::TYPE_TIME);
            },
            Synapse::TYPE_INT, Synapse::TYPE_BIGINT, Synapse::TYPE_SMALLINT, Synapse::TYPE_TINYINT => function () {
                return new Snowflake(
                    Snowflake::TYPE_NUMBER, [
                        'length' => [
                            'numeric_precision' => 38,
                            'numeric_scale' => 0
                        ]
                    ]
                );
            }
            ,
            default => function () {
                return new Snowflake($this->synapseDatatype->getBasetype());
            }
        };
        return $def();
    }

    public function getRowBaseType(): Synapse
    {
        return $this->synapseDatatype;
    }

    private function getNonEmptyData(int $index, array $row)
    {
        if ($row[$index] == '') {
            throw new \InvalidArgumentException(sprintf("Index %d is empty in row %s", $index, implode(', ', $row)));
        }
        return $row[$index];
    }

}