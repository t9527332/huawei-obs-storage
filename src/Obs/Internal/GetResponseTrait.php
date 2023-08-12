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

namespace Jtz\HuaweiObsStorage\Obs\Internal;

use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Jtz\HuaweiObsStorage\Obs\Internal\Common\CheckoutStream;
use Jtz\HuaweiObsStorage\Obs\Internal\Common\Model;
use Jtz\HuaweiObsStorage\Obs\Internal\Resource\OBSConstants;
use Jtz\HuaweiObsStorage\Obs\ObsException;
use Psr\Http\Message\StreamInterface;

trait GetResponseTrait
{
    protected $exceptionResponseMode = true;

    protected $chunkSize = 65536;

    protected function isClientError(Response $response)
    {
        return $response->getStatusCode() >= 400 && $response->getStatusCode() < 500;
    }

    protected function parseXmlByType($searchPath, $key, &$value, $xml, $prefix)
    {
        $type = 'string';

        if (isset($value['sentAs'])) {
            $key = $value['sentAs'];
        }

        if ($searchPath === null) {
            $searchPath = '//' . $prefix . $key;
        }

        if (isset($value['type'])) {
            $type = $value['type'];
            if ($type === 'array') {
                $items = $value['items'];
                if (isset($value['wrapper'])) {
                    $paths = explode('/', $searchPath);
                    $size = count($paths);
                    if ($size > 1) {
                        $end = $paths[$size - 1];
                        $paths[$size - 1] = $value['wrapper'];
                        $paths[] = $end;
                        $searchPath = implode('/', $paths) . '/' . $prefix;
                    }
                }

                $array = [];
                if (!isset($value['data']['xmlFlattened'])) {
                    $pkey = isset($items['sentAs']) ? $items['sentAs'] : $items['name'];
                    $_searchPath = $searchPath . '/' . $prefix . $pkey;
                } else {
                    $pkey = $key;
                    $_searchPath = $searchPath;
                }
                if ($result = $xml->xpath($_searchPath)) {
                    if (is_array($result)) {
                        foreach ($result as $subXml) {
                            $subXml = simplexml_load_string($subXml->asXML());
                            $subPrefix = $this->getXpathPrefix($subXml);
                            $array[] = $this->parseXmlByType('//' . $subPrefix . $pkey, $pkey, $items, $subXml, $subPrefix);
                        }
                    }
                }
                return $array;
            } elseif ($type === 'object') {
                $properties = $value['properties'];
                $array = [];
                foreach ($properties as $pkey => $pvalue) {
                    $name = isset($pvalue['sentAs']) ? $pvalue['sentAs'] : $pkey;
                    $array[$pkey] = $this->parseXmlByType($searchPath . '/' . $prefix . $name, $name, $pvalue, $xml, $prefix);
                }
                return $array;
            }
        }

        if ($result = $xml->xpath($searchPath)) {
            if ($type === 'boolean') {
                return ((string) $result[0]) !== 'false';
            } elseif ($type === 'numeric' || $type === 'float') {
                return (float) $result[0];
            } elseif ($type === 'int' || $type === 'integer') {
                return (int) $result[0];
            }
            return (string) $result[0];
        }
        if ($type === 'boolean') {
            return false;
        } elseif ($type === 'numeric' || $type === 'float' || $type === 'int' || $type === 'integer') {
            return null;
        }
        return '';
    }

    protected function parseItems($responseParameters, $model, $response, $body)
    {
        $prefix = '';

        $this->parseCommonHeaders($model, $response);

        $closeBody = false;
        try {
            foreach ($responseParameters as $key => $value) {
                if (isset($value['location'])) {
                    $location = $value['location'];
                    if ($location === 'header') {
                        $name = isset($value['sentAs']) ? $value['sentAs'] : $key;
                        $isSet = false;
                        if (isset($value['type'])) {
                            $type = $value['type'];
                            if ($type === 'object') {
                                $headers = $response->getHeaders();
                                $temp = [];
                                foreach ($headers as $headerName => &$_) {
                                    if (stripos($headerName, $name) === 0) {
                                        $metaKey = rawurldecode(substr($headerName, strlen($name)));
                                        $temp[$metaKey] = rawurldecode($response->getHeaderLine($headerName));
                                    }
                                }
                                $model[$key] = $temp;
                                $isSet = true;
                            } else {
                                if ($response->hasHeader($name)) {
                                    if ($type === 'boolean') {
                                        $model[$key] = $response->getHeaderLine($name) !== 'false';
                                        $isSet = true;
                                    } elseif ($type === 'numeric' || $type === 'float') {
                                        $model[$key] = (float) $response->getHeaderLine($name);
                                        $isSet = true;
                                    } elseif ($type === 'int' || $type === 'integer') {
                                        $model[$key] = (int) $response->getHeaderLine($name);
                                        $isSet = true;
                                    }
                                }
                            }
                        }
                        if (!$isSet) {
                            $model[$key] = rawurldecode($response->getHeaderLine($name));
                        }
                    } elseif ($location === 'xml' && $body !== null) {
                        if (!isset($xml) && ($xml = simplexml_load_string($body->getContents()))) {
                            $prefix = $this->getXpathPrefix($xml);
                        }
                        $closeBody = true;
                        $model[$key] = $this->parseXmlByType(null, $key, $value, $xml, $prefix);
                    } elseif ($location === 'body' && $body !== null) {
                        if (isset($value['type']) && $value['type'] === 'stream') {
                            $model[$key] = $body;
                        } elseif (isset($value['type']) && $value['type'] === 'json') {
                            $jsonBody = trim($body->getContents());
                            if ($jsonBody && ($data = json_decode($jsonBody, true))) {
                                if (is_array($data)) {
                                    $model[$key] = $data;
                                }
                            }
                            $closeBody = true;
                        } else {
                            $model[$key] = $body->getContents();
                            $closeBody = true;
                        }
                    }
                }
            }
        } finally {
            if ($closeBody && $body !== null) {
                $body->close();
            }
        }
    }

    protected function parseResponse(Model $model, Request $request, Response $response, array $requestConfig)
    {
        $statusCode = $response->getStatusCode();
        $expectedLength = $response->getHeaderLine('content-length');
        $responseContentType = $response->getHeaderLine('content-type');

        $expectedLength = is_numeric($expectedLength) ? (int) $expectedLength : null;

        $body = new CheckoutStream($response->getBody(), $expectedLength);

        if ($statusCode >= 300) {
            if ($this->exceptionResponseMode) {
                $obsException = new ObsException();
                $obsException->setRequest($request);
                $obsException->setResponse($response);
                $obsException->setExceptionType($this->isClientError($response) ? 'client' : 'server');
                if ($responseContentType === 'application/json') {
                    $this->parseJsonToException($body, $obsException);
                } else {
                    $this->parseXmlToException($body, $obsException);
                }
                throw $obsException;
            } else {
                $this->parseCommonHeaders($model, $response);
                if ($responseContentType === 'application/json') {
                    $this->parseJsonToModel($body, $model);
                } else {
                    $this->parseXmlToModel($body, $model);
                }
            }
        } else {
            if (!empty($model)) {
                foreach ($model as $key => $value) {
                    if ($key === 'method') {
                        continue;
                    }
                    if (isset($value['type']) && $value['type'] === 'file') {
                        $this->writeFile($value['value'], $body, $expectedLength);
                    }
                    $model[$key] = $value['value'];
                }
            }

            if (isset($requestConfig['responseParameters'])) {
                $responseParameters = $requestConfig['responseParameters'];
                if (isset($responseParameters['type']) && $responseParameters['type'] === 'object') {
                    $responseParameters = $responseParameters['properties'];
                }
                $this->parseItems($responseParameters, $model, $response, $body);
            }
        }

        $model['HttpStatusCode'] = $statusCode;
        $model['Reason'] = $response->getReasonPhrase();
    }

    protected function getXpathPrefix($xml)
    {
        $namespaces = $xml->getDocNamespaces();
        if (isset($namespaces[''])) {
            $xml->registerXPathNamespace('ns', $namespaces['']);
            $prefix = 'ns:';
        } else {
            $prefix = '';
        }
        return $prefix;
    }

    protected function buildException(Request $request, RequestException $exception, $message)
    {
        $response = $exception->hasResponse() ? $exception->getResponse() : null;
        $obsException = new ObsException($message ? $message : $exception->getMessage());
        $obsException->setExceptionType('client');
        $obsException->setRequest($request);
        if ($response) {
            $obsException->setResponse($response);
            $obsException->setExceptionType($this->isClientError($response) ? 'client' : 'server');
            if ($this->isJsonResponse($response)) {
                $this->parseJsonToException($response->getBody(), $obsException);
            } else {
                $this->parseXmlToException($response->getBody(), $obsException);
            }

                                                                        $obsException->setRequestId(data_get($response->getHeader('x-obs-request-id'), '0'));
            if ($obsException->getRequestId() === null) {
                $prefix = 'x-obs-';
                $requestId = $response->getHeaderLine($prefix . 'request-id');
                $obsException->setRequestId($requestId);
            }
        }
        return $obsException;
    }

    protected function parseExceptionAsync(Request $request, RequestException $exception, $message = null)
    {
        return $this->buildException($request, $exception, $message);
    }

    protected function parseException(Model $model, Request $request, RequestException $exception, $message = null)
    {
        $response = $exception->hasResponse() ? $exception->getResponse() : null;
        if ($this->exceptionResponseMode) {
            throw $this->buildException($request, $exception, $message);
        }
        if ($response) {
            $model['HttpStatusCode'] = $response->getStatusCode();
            $model['Reason'] = $response->getReasonPhrase();
            $this->parseXmlToModel($response->getBody(), $model);
        } else {
            $model['HttpStatusCode'] = -1;
            $model['Message'] = $exception->getMessage();
        }
    }

    private function isJsonResponse($response)
    {
        return $response->getHeaderLine('content-type') === 'application/json';
    }

    private function parseCommonHeaders($model, $response)
    {
        foreach (OBSConstants::COMMON_HEADERS as $key => $value) {
            $model[$value] = $response->getHeaderLine($key);
        }
    }

    private function writeFile($filePath, StreamInterface &$body, $contentLength)
    {
        $filePath = iconv('UTF-8', 'GBK', $filePath);
        if (is_string($filePath) && $filePath !== '') {
            $fp = null;
            $dir = dirname($filePath);
            try {
                if (!is_dir($dir)) {
                    mkdir($dir, 0755, true);
                }

                if ($fp = fopen($filePath, 'w')) {
                    while (!$body->eof()) {
                        $str = $body->read($this->chunkSize);
                        fwrite($fp, $str);
                    }
                    fflush($fp);
                }
            } finally {
                if ($fp) {
                    fclose($fp);
                }
                $body->close();
                $body = null;
            }
        }
    }

    private function parseXmlToException($body, $obsException)
    {
        try {
            $xmlErrorBody = trim($body->getContents());
            if ($xmlErrorBody && ($xml = simplexml_load_string($xmlErrorBody))) {
                $prefix = $this->getXpathPrefix($xml);
                if ($tempXml = $xml->xpath('//' . $prefix . 'Code')) {
                    $obsException->setExceptionCode((string) $tempXml[0]);
                }
                if ($tempXml = $xml->xpath('//' . $prefix . 'RequestId')) {
                    $obsException->setRequestId((string) $tempXml[0]);
                }
                if ($tempXml = $xml->xpath('//' . $prefix . 'Message')) {
                    $obsException->setExceptionMessage((string) $tempXml[0]);
                }
                if ($tempXml = $xml->xpath('//' . $prefix . 'HostId')) {
                    $obsException->setHostId((string) $tempXml[0]);
                }
            }
        } finally {
            $body->close();
        }
    }

    private function parseJsonToException($body, $obsException)
    {
        try {
            $jsonErrorBody = trim($body->getContents());
            if ($jsonErrorBody && ($data = json_decode($jsonErrorBody, true))) {
                if (is_array($data)) {
                    if ($data['request_id']) {
                        $obsException->setRequestId((string) $data['request_id']);
                    }
                    if ($data['code']) {
                        $obsException->setExceptionCode((string) $data['code']);
                    }
                    if ($data['message']) {
                        $obsException->setExceptionMessage((string) $data['message']);
                    }
                } elseif (strlen($data)) {
                    $obsException->setExceptionMessage('Invalid response data，since it is not json data');
                }
            }
        } finally {
            $body->close();
        }
    }

    private function parseJsonToModel($body, $model)
    {
        try {
            $jsonErrorBody = trim($body->getContents());
            if ($jsonErrorBody && ($jsonArray = json_decode($jsonErrorBody, true))) {
                if (is_array($jsonArray)) {
                    if ($jsonArray['request_id']) {
                        $model['RequestId'] = (string) $jsonArray['request_id'];
                    }
                    if ($jsonArray['code']) {
                        $model['Code'] = (string) $jsonArray['code'];
                    }
                    if ($jsonArray['message']) {
                        $model['Message'] = (string) $jsonArray['message'];
                    }
                } elseif (strlen($jsonArray)) {
                    $model['Message'] = 'Invalid response data，since it is not json data';
                }
            }
        } finally {
            $body->close();
        }
    }

    private function parseXmlToModel($body, $model)
    {
        try {
            $xmlErrorBody = trim($body->getContents());
            if ($xmlErrorBody && ($xml = simplexml_load_string($xmlErrorBody))) {
                $prefix = $this->getXpathPrefix($xml);
                if ($tempXml = $xml->xpath('//' . $prefix . 'Code')) {
                    $model['Code'] = (string) $tempXml[0];
                }
                if ($tempXml = $xml->xpath('//' . $prefix . 'RequestId')) {
                    $model['RequestId'] = (string) $tempXml[0];
                }

                if ($tempXml = $xml->xpath('//' . $prefix . 'HostId')) {
                    $model['HostId'] = (string) $tempXml[0];
                }
                if ($tempXml = $xml->xpath('//' . $prefix . 'Resource')) {
                    $model['Resource'] = (string) $tempXml[0];
                }

                if ($tempXml = $xml->xpath('//' . $prefix . 'Message')) {
                    $model['Message'] = (string) $tempXml[0];
                }
            }
        } finally {
            $body->close();
        }
    }
}
