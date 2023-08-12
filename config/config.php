<?php

return [
    'driver' => 'obs',
    'access_id' => env('OBS_ACCESS_KEY_ID'),
    'access_key' => env('OBS_ACCESS_KEY_SECRET'),
    'bucket' => env('OBS_BUCKET'),
    'endpoint' => env('OBS_ENDPOINT'), // OBS 外网节点或自定义外部域名
    'endpoint_internal' => env('OBS_ENDPOINT_INTERNAL'), // 如果为空，则默认使用 endpoint 配置
    'cdnDomain' => env('OBS_DOMAIN'), // 如果不为空，getUrl会判断cdnDomain是否设定来决定返回的url，如果cdnDomain未设置，则使用endpoint来生成url，否则使用cdn
    'ssl' => env('OBS_SSL', false), // true to use 'https://' and false to use 'http://'. default is false,
    'prefix' => env('OBS_PREFIX'), // 路径前缀
    'options' => [],
];
