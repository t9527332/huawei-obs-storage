<?php

/**
 * Copyright 2019 Huawei Technologies Co.,Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at.
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

namespace Jtz\HuaweiObsStorage\Obs;

use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;

class ObsException extends \RuntimeException
{
    public const CLIENT = 'client';

    public const SERVER = 'server';

    private Response $response;

    private Request $request;

    private string $requestId;

    private string $exceptionType;

    private string $exceptionCode;

    private string $exceptionMessage = '';

    private string $hostId;

    public function __construct(?string $message = null, ?int $code = null, ?\Throwable $previous = null)
    {
        parent::__construct($message, $code, $previous);
    }

    public function __toString()
    {
        $message = get_class($this) . ': '
            . 'OBS Error Code: ' . $this->getExceptionCode() . ', '
            . 'Status Code: ' . $this->getStatusCode() . ', '
            . 'OBS Error Type: ' . $this->getExceptionType() . ', '
            . 'OBS Error Message: ' . ($this->getExceptionMessage() ? $this->getExceptionMessage() : $this->getMessage());

        // Add the User-Agent if available
        if ($this->request) {
            $message .= ', User-Agent: ' . $this->request->getHeaderLine('User-Agent');
        }
        $message .= "\n";
        return $message;
    }

    public function setExceptionCode(string $exceptionCode)
    {
        $this->exceptionCode = $exceptionCode;
    }

    public function getExceptionCode(): string
    {
        return $this->exceptionCode;
    }

    public function setExceptionMessage(string $exceptionMessage)
    {
        $this->exceptionMessage = $exceptionMessage;
    }

    public function getExceptionMessage(): string
    {
        return $this->exceptionMessage ?: $this->message;
    }

    public function setExceptionType(string $exceptionType)
    {
        $this->exceptionType = $exceptionType;
    }

    public function getExceptionType(): string
    {
        return $this->exceptionType;
    }

    public function setRequestId(string $requestId)
    {
        $this->requestId = $requestId;
    }

    public function getRequestId(): string
    {
        return $this->requestId;
    }

    public function setResponse(Response $response)
    {
        $this->response = $response;
    }

    public function getResponse(): Response
    {
        return $this->response;
    }

    public function setRequest(Request $request)
    {
        $this->request = $request;
    }

    public function getRequest(): Request
    {
        return $this->request;
    }

    public function getStatusCode(): int
    {
        return $this->response?->getStatusCode() ?? -1;
    }

    public function setHostId(string $hostId)
    {
        $this->hostId = $hostId;
    }

    public function getHostId(): string
    {
        return $this->hostId;
    }
}
