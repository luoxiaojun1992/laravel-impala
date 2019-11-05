<?php

namespace Lxj\Laravel\Impala\Connectors;

use Illuminate\Database\Connectors\Connector;
use Illuminate\Database\Connectors\ConnectorInterface;
use ThriftSQL\Impala;
use Ytake\PrestoClient\ClientSession;

class ThriftConnector extends Connector implements ConnectorInterface
{
    public function connect(array $config)
    {
        $impala = new Impala(
            $config['host'],
            $config['port'] ?? 21000,
            $config['username'] ?? null,
            $config['password'] ?? null,
            $config['timeout'] ?? 500000
        );
        $impala->connect();
        return $impala;
    }
}
