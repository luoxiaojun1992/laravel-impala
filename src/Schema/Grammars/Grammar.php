<?php

namespace Lxj\Laravel\Impala\Schema\Grammars;

class Grammar extends \Illuminate\Database\Schema\Grammars\Grammar
{
    protected function wrapValue($value)
    {
        if ($value !== '*') {
            return '`'.str_replace('`', '``', $value).'`';
        }

        return $value;
    }
}
