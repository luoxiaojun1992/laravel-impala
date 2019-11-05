<?php

namespace Lxj\Laravel\Impala\Connections;

use Illuminate\Database\Connection;
use Illuminate\Database\Events\StatementPrepared;
use Lxj\Laravel\Impala\Connectors\ODBCConnector;
use Lxj\Laravel\Impala\Query\Grammars\Grammar as QueryGrammar;
use Lxj\Laravel\Impala\Schema\Grammars\Grammar as SchemaGrammar;

class ODBCConnection extends Connection
{
    /** @var resource */
    protected $odbcConnection;

    protected function reconnectIfMissingConnection()
    {
        if (is_null($this->odbcConnection)) {
            $this->reconnect();
        }
    }

    /**
     * @param resource $odbcConnection
     * @return $this
     */
    public function setODBCConnection($odbcConnection)
    {
        $this->odbcConnection = $odbcConnection;
        return $this;
    }

    public function reconnect()
    {
        $this->disconnect();
        $this->odbcConnection = (new ODBCConnector())->connect($this->config);
    }

    public function disconnect()
    {
        if (is_null($this->odbcConnection)) {
            return;
        }

        odbc_close($this->odbcConnection);
    }

    public function __destruct()
    {
        $this->disconnect();
    }

    public function select($query, $bindings = [], $useReadPdo = true)
    {
        return $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                return [];
            }

            $bindings = $this->prepareBindings($bindings);

            $statement = odbc_prepare($this->odbcConnection, $query);

            $this->afterPrepare($statement);

            if (!odbc_execute($statement, $bindings)) {
                return [];
            }

            $rows = [];
            while ($row = odbc_fetch_array($statement)) {
                $rows[] = $row;
            }

            return $rows;
        });
    }

    public function cursor($query, $bindings = [], $useReadPdo = true)
    {
        return $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                return [];
            }

            $bindings = $this->prepareBindings($bindings);

            $statement = odbc_prepare($this->odbcConnection, $query);

            $this->afterPrepare($statement);

            if (!odbc_execute($statement, $bindings)) {
                return [];
            }

            while (($row = odbc_fetch_array($statement)) !== false) {
                yield $row;
            }
        });
    }

    protected function afterPrepare($statement)
    {
        $this->event(new StatementPrepared(
            $this, $statement
        ));
    }

    protected function getDefaultQueryGrammar()
    {
        return $this->withTablePrefix(new QueryGrammar());
    }

    protected function getDefaultSchemaGrammar()
    {
        return $this->withTablePrefix(new SchemaGrammar());
    }
}
