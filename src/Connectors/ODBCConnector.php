<?php

namespace Lxj\Laravel\Impala\Connectors;

use Illuminate\Database\Connectors\Connector;
use Illuminate\Database\Connectors\ConnectorInterface;

class ODBCConnector extends Connector implements ConnectorInterface
{
    public function connect(array $config)
    {
        return odbc_connect('impala', '', '');
    }
}
