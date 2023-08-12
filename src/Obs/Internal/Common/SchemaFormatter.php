<?php

namespace Jtz\HuaweiObsStorage\Obs\Internal\Common;

use Carbon\Carbon;

class SchemaFormatter
{
    public static function format(string $fmt, string|int|\DateTimeInterface $value): string|int|\DateTimeInterface
    {
        return match ($fmt) {
            'date-time' => Carbon::parse($value)->format('Y-m-d\TH:i:s\Z'),
            'data-time-http' => Carbon::parse($value)->format('D, d M Y H:i:s \G\M\T'),
            'data-time-middle' => Carbon::parse($value)->format('Y-m-d\T00:00:00\Z'),
            'date' => Carbon::parse($value)->format('Y-m-d'),
            'timestamp' => (int) Carbon::parse($value)->format('U'),
            'boolean-string' => filter_var($value, FILTER_VALIDATE_BOOLEAN) ? 'true' : 'false',
            default => $value,
        };
    }
}
