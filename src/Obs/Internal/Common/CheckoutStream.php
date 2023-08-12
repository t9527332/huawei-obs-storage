<?php

namespace Jtz\HuaweiObsStorage\Obs\Internal\Common;

use GuzzleHttp\Psr7\StreamDecoratorTrait;
use Jtz\HuaweiObsStorage\Obs\ObsException;
use Psr\Http\Message\StreamInterface;

class CheckoutStream implements StreamInterface
{
    use StreamDecoratorTrait;

    private ?int $expectedLength;

    private int $readedCount = 0;

    public function __construct(StreamInterface $stream, ?int $expectedLength)
    {
        $this->stream = $stream;
        $this->expectedLength = $expectedLength;
    }

    public function getContents(): string
    {
        $contents = $this->stream->getContents();
        $length = strlen($contents);
        if ($this->expectedLength !== null && $length !== $this->expectedLength) {
            $this->throwObsException($this->expectedLength, $length);
        }
        return $contents;
    }

    public function read(int $length): string
    {
        $string = $this->stream->read($length);
        if ($this->expectedLength !== null) {
            $this->readedCount += strlen($string);
            if ($this->stream->eof()) {
                if ($this->readedCount !== $this->expectedLength) {
                    $this->throwObsException($this->expectedLength, $this->readedCount);
                }
            }
        }
        return $string;
    }

    public function throwObsException(int $expectedLength, int $reaLength): void
    {
        $obsException = new ObsException('premature end of Content-Length delimiter message body (expected:' . $expectedLength . '; received:' . $reaLength . ')');
        $obsException->setExceptionType('server');
        throw $obsException;
    }
}
