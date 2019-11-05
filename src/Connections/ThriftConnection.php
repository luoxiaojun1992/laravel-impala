<?php

namespace Lxj\Laravel\Impala\Connections;

use Closure;
use Illuminate\Database\Events\StatementPrepared;
use Illuminate\Database\QueryException;
use Lxj\Laravel\Impala\Query\Grammars\Grammar as QueryGrammar;
use Lxj\Laravel\Impala\Schema\Grammars\Grammar as SchemaGrammar;
use ThriftSQL\Impala;

class ThriftConnection extends \Illuminate\Database\Connection
{
    /** @var Impala */
    protected $impala;

    protected function reconnectIfMissingConnection()
    {
        if (is_null($this->impala)) {
            $this->reconnect();
        }
    }

    /**
     * @param Impala $impala
     * @return $this
     */
    public function setImpala(Impala $impala)
    {
        $this->impala = $impala;
        return $this;
    }

    public function reconnect()
    {
        $this->disconnect();
        $this->impala->connect();
    }

    public function disconnect()
    {
        if (is_null($this->impala)) {
            return;
        }

        $this->impala->disconnect();
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

            $statement = $this->prepareQuery($query, $bindings);
            
            $this->afterPrepare($statement);
            
            return $this->impala->queryAndFetchAll($statement);
        });
    }

    public function cursor($query, $bindings = [], $useReadPdo = true)
    {
        return $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                return new \ArrayIterator();;
            }

            $statement = $this->prepareQuery($query, $bindings);

            $this->afterPrepare($statement);

            return $this->impala->getIterator($statement);
        });
    }

    protected function prepareQuery(
        $query,
        $bindings = []
    ) {
        $bindings = $this->prepareBindings($bindings);

        foreach ($bindings as $bindValue) {
            $query = str_replace_first('?', $bindValue, $query);
        }

        return $query;
    }

    /**
     * Prepare the query bindings for execution.
     *
     * @param  array  $bindings
     * @return array
     */
    public function prepareBindings(array $bindings)
    {
        $bindings = parent::prepareBindings($bindings);

        foreach ($bindings as $key => $value) {
            if (is_string($value)) {
                $bindings[$key] = ('\'' . $value . '\'');
            }
        }

        return $bindings;
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
