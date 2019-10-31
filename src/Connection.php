<?php

namespace Lxj\Laravel\Impala;

use Closure;
use Illuminate\Database\QueryException;
use Lxj\Laravel\Impala\Query\Grammars\Grammar as QueryGrammar;
use Lxj\Laravel\Impala\Schema\Grammars\Grammar as SchemaGrammar;
use ThriftSQL\Impala;

class Connection extends \Illuminate\Database\Connection
{
    /** @var Impala */
    protected $impala;

    public function __construct($pdo, $database = '', $tablePrefix = '', array $config = [])
    {
        parent::__construct($pdo, $database, $tablePrefix, $config);
    }

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
        $this->impala->disconnect();
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

            return $this->impala->queryAndFetchAll($this->prepareQuery($query, $bindings));
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
                $bindings[$key] = '\'' . $value . '\'';
            }
        }

        return $bindings;
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
