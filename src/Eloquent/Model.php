<?php

namespace Lxj\Laravel\Impala\Eloquent;

class Model extends \Illuminate\Database\Eloquent\Model
{
    protected $connection = 'impala';

    public $incrementing = false;
}
