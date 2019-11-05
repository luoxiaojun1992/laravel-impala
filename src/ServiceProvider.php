<?php

namespace Lxj\Laravel\Impala;

use Lxj\Laravel\Impala\Connections\Connection;
use Lxj\Laravel\Impala\Connections\ODBCConnection;
use Lxj\Laravel\Impala\Connections\ThriftConnection;
use Lxj\Laravel\Impala\Connectors\ODBCConnector;
use Lxj\Laravel\Impala\Connectors\ThriftConnector;
use Lxj\Laravel\Presto\Eloquent\Model;

class ServiceProvider extends \Illuminate\Support\ServiceProvider
{
    /**
     * Bootstrap the application events.
     */
    public function boot()
    {
        Model::setConnectionResolver($this->app['db']);
        Model::setEventDispatcher($this->app['events']);
    }

    /**
     * Register the service provider.
     */
    public function register()
    {
        $this->app->resolving('db', function ($db) {
            $db->extend('impala', function ($config, $name) {
                $config['name'] = $name;
                $prefix = $config['prefix'] ?? '';

                $connector = $config['connector'] ?? 'ODBC';
                if ($connector === 'thrift') {
                    $connection = new ThriftConnection(null, '', $prefix, $config);
                    $connection->setImpala((new ThriftConnector())->connect($config));
                } else {
                    $connection = new ODBCConnection(null, '', $prefix, $config);
                    $connection->setODBCConnection((new ODBCConnector())->connect($config));
                }

                return $connection;
            });
        });
    }

}
