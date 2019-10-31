<?php

namespace Lxj\Laravel\Impala;

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
        // Add database driver.
        $this->app->resolving('db', function ($db) {
            $db->extend('impala', function ($config, $name) {
                $config['name'] = $name;
                $prefix = $config['prefix'] ?? '';
                $connection = new Connection(null, '', $prefix, $config);
                $connection->setImpala((new ThriftConnector())->connect($config));
                return $connection;
            });
        });
    }

}
