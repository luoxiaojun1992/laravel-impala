<?php

namespace Lxj\Laravel\Impala\Query\Grammars;

class Grammar extends \Illuminate\Database\Query\Grammars\Grammar
{
    protected function wrapValue($value)
    {
        if ($value !== '*') {
            return '`'.str_replace('`', '``', $value).'`';
        }

        return $value;
    }
}
