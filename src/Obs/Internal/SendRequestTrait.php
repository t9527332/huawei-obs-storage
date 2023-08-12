<?php

namespace Jtz\HuaweiObsStorage\Obs\Internal;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Psr7;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Jtz\HuaweiObsStorage\Obs\Internal\Common\Model;
use Jtz\HuaweiObsStorage\Obs\Internal\Resource\Constants;
use Jtz\HuaweiObsStorage\Obs\Internal\Resource\OBSConstants;
use Jtz\HuaweiObsStorage\Obs\Internal\Resource\OBSRequestResource;
use Jtz\HuaweiObsStorage\Obs\Internal\Signature\DefaultSignature;
use Jtz\HuaweiObsStorage\Obs\ObsClient;
use Jtz\HuaweiObsStorage\Obs\ObsException;
use Psr\Http\Message\StreamInterface;

trait SendRequestTrait
{
    protected string $ak;

    protected string $sk;

    protected ?string $securityToken = null;

    protected string $endpoint = '';

    protected bool $pathStyle = false;

    protected string $region = 'region';

    protected string $signature = 'obs';

    protected bool $sslVerify = false;

    protected int $maxRetryCount = 3;

    protected int $timeout = 0;

    protected int $socketTimeout = 60;

    protected int $connectTimeout = 60;

    protected bool $isCname = false;

    protected ?string $proxy = null;

    protected Client $httpClient;

    public function __call(string $originMethod, array $args): Model|PromiseInterface
    {
        $method = $originMethod;

        $resource = OBSRequestResource::RESOURCE_ARRAY;
        $async = false;
        if (strpos($method, 'Async') === (strlen($method) - 5)) {
            $method = substr($method, 0, strlen($method) - 5);
            $async = true;
        }

        if (isset($resource['aliases'][$method])) {
            $method = $resource['aliases'][$method];
        }

        $method = lcfirst($method);

        $operation = $resource['operations'][$method] ?? null;

        if (!$operation) {
            $obsException = new ObsException('unknow method ' . $originMethod);
            $obsException->setExceptionType('client');
            throw $obsException;
        }

        $start = microtime(true);
        if (!$async) {
            $model = new Model();
            $model->method = $method;
            $params = empty($args) ? [] : $args[0];
            $this->checkMimeType($method, $params);
            $this->doRequest($model, $operation, $params);
            unset($model->method);
            return $model;
        }
        if (empty($args) || !is_callable($callback = $args[count($args) - 1])) {
            $obsException = new ObsException('async method ' . $originMethod . ' must pass a CallbackInterface as param');
            $obsException->setExceptionType('client');
            throw $obsException;
        }
        $params = count($args) === 1 ? [] : $args[0];
        $this->checkMimeType($method, $params);
        $model = new Model();
        $model->method = $method;
        return $this->doRequestAsync($model, $operation, $params, $callback, $start, $originMethod);
    }

    public function createSignedUrl(array $args = [])
    {
        return $this->createCommonSignedUrl($args);
    }

    public function createPostSignature(array $args = [])
    {
        $bucketName = isset($args[ObsClient::OBS_BUCKET]) ? (string) ($args[ObsClient::OBS_BUCKET]) : null;
        $objectKey = isset($args[ObsClient::OBS_KEY]) ? (string) ($args[ObsClient::OBS_KEY]) : null;
        $expires = isset($args[ObsClient::OBS_EXPIRES]) && is_numeric($args[ObsClient::OBS_EXPIRES]) ? (int) ($args[ObsClient::OBS_EXPIRES]) : 300;

        $formParams = [];

        if (isset($args['FormParams']) && is_array($args['FormParams'])) {
            foreach ($args['FormParams'] as $key => $val) {
                $formParams[$key] = $val;
            }
        }

        if (!is_null($this->securityToken) && !isset($formParams[OBSConstants::SECURITY_TOKEN_HEAD])) {
            $formParams[OBSConstants::SECURITY_TOKEN_HEAD] = $this->securityToken;
        }

        $timestamp = time();
        $expires = gmdate('Y-m-d\TH:i:s\Z', $timestamp + $expires);

        if ($bucketName) {
            $formParams['bucket'] = $bucketName;
        }

        if ($objectKey) {
            $formParams['key'] = $objectKey;
        }

        $policy = [];

        $policy[] = '{"expiration":"';
        $policy[] = $expires;
        $policy[] = '", "conditions":[';

        $matchAnyBucket = true;
        $matchAnyKey = true;

        $conditionAllowKeys = ['acl', 'bucket', 'key', 'success_action_redirect', 'redirect', 'success_action_status'];

        foreach ($formParams as $key => $val) {
            if ($key) {
                $key = strtolower((string) $key);

                if ($key === 'bucket') {
                    $matchAnyBucket = false;
                } elseif ($key === 'key') {
                    $matchAnyKey = false;
                }

                if (!in_array($key, Constants::ALLOWED_REQUEST_HTTP_HEADER_METADATA_NAMES) && strpos($key, OBSConstants::HEADER_PREFIX) !== 0 && !in_array($key, $conditionAllowKeys)) {
                    $key = OBSConstants::METADATA_PREFIX . $key;
                }

                $policy[] = '{"';
                $policy[] = $key;
                $policy[] = '":"';
                $policy[] = $val !== null ? (string) ($val) : '';
                $policy[] = '"},';
            }
        }

        if ($matchAnyBucket) {
            $policy[] = '["starts-with", "$bucket", ""],';
        }

        if ($matchAnyKey) {
            $policy[] = '["starts-with", "$key", ""],';
        }

        $policy[] = ']}';

        $originPolicy = implode('', $policy);

        $policy = base64_encode($originPolicy);

        $signatureContent = base64_encode(hash_hmac('sha1', $policy, $this->sk, true));

        $model = new Model();
        $model->OriginPolicy = $originPolicy;
        $model->Policy = $policy;
        $model->Signature = $signatureContent;
        return $model;
    }

    protected function makeRequest(Model $model, array &$operation, array $params, ?string $endpoint = null): Request
    {
        if (is_null($endpoint)) {
            $endpoint = $this->endpoint;
        }
        $signatureInterface = new DefaultSignature($this->ak, $this->sk, $this->pathStyle, $endpoint, $model['method'], $this->signature, $this->securityToken, $this->isCname);
        $authResult = $signatureInterface->doAuth($operation, $params, $model);
        $httpMethod = $authResult['method'];
        $authResult['headers']['User-Agent'] = self::default_user_agent();
        if ($model['method'] === 'putObject') {
            $model['ObjectURL'] = ['value' => $authResult['requestUrl']];
        }
        return new Request($httpMethod, $authResult['requestUrl'], $authResult['headers'], $authResult['body']);
    }

    protected function doRequest(Model $model, array &$operation, array $params, ?string $endpoint = null): void
    {
        $request = $this->makeRequest($model, $operation, $params, $endpoint);
        $this->sendRequest($model, $operation, $params, $request);
    }

    protected function sendRequest(Model $model, array &$operation, array $params, Request $request, int $requestCount = 1): void
    {
        $saveAsStream = false;
        if (isset($operation['stream']) && $operation['stream']) {
            $saveAsStream = isset($params['SaveAsStream']) ? $params['SaveAsStream'] : false;

            if (isset($params['SaveAsFile'])) {
                if ($saveAsStream) {
                    $obsException = new ObsException('SaveAsStream cannot be used with SaveAsFile together');
                    $obsException->setExceptionType('client');
                    throw $obsException;
                }
                $saveAsStream = true;
            }
            if (isset($params['FilePath'])) {
                if ($saveAsStream) {
                    $obsException = new ObsException('SaveAsStream cannot be used with FilePath together');
                    $obsException->setExceptionType('client');
                    throw $obsException;
                }
                $saveAsStream = true;
            }

            if (isset($params['SaveAsFile']) && isset($params['FilePath'])) {
                $obsException = new ObsException('SaveAsFile cannot be used with FilePath together');
                $obsException->setExceptionType('client');
                throw $obsException;
            }
        }

        $promise = $this->httpClient->sendAsync($request, ['stream' => $saveAsStream])->then(
            function (Response $response) use ($model, $operation, $params, $request, $requestCount) {
                $statusCode = $response->getStatusCode();
                $readable = isset($params[ObsClient::OBS_BODY]) && ($params[ObsClient::OBS_BODY] instanceof StreamInterface || is_resource($params[ObsClient::OBS_BODY]));
                if ($statusCode >= 300 && $statusCode < 400 && $statusCode !== 304 && !$readable && $requestCount <= $this->maxRetryCount) {
                    if ($location = $response->getHeaderLine('location')) {
                        $url = parse_url($this->endpoint);
                        $newUrl = parse_url($location);
                        $scheme = (isset($newUrl['scheme']) ? $newUrl['scheme'] : $url['scheme']);
                        $defaultPort = strtolower($scheme) === 'https' ? '443' : '80';
                        $this->doRequest($model, $operation, $params, $scheme . '://' . $newUrl['host'] . ':' . (isset($newUrl['port']) ? $newUrl['port'] : $defaultPort));
                        return;
                    }
                }
                $this->parseResponse($model, $request, $response, $operation);
            },
            function (RequestException $exception) use ($model, $operation, $params, $request, $requestCount) {
                $message = null;
                if ($exception instanceof ConnectException) {
                    if ($requestCount <= $this->maxRetryCount) {
                        $this->sendRequest($model, $operation, $params, $request, $requestCount + 1);
                        return;
                    }
                    $message = 'Exceeded retry limitation, max retry count:' . $this->maxRetryCount . ', error message:' . $exception->getMessage();
                }
                $this->parseException($model, $request, $exception, $message);
            }
        );
        $promise->wait();
    }

    protected function doRequestAsync(Model $model, array &$operation, array $params, callable $callback, int $startAsync, $originMethod, ?string $endpoint = null): PromiseInterface
    {
        $request = $this->makeRequest($model, $operation, $params, $endpoint);
        return $this->sendRequestAsync($model, $operation, $params, $callback, $startAsync, $originMethod, $request);
    }

    protected function sendRequestAsync(Model $model, array &$operation, array $params, callable $callback, int $startAsync, $originMethod, Request $request, int $requestCount = 1): PromiseInterface
    {
        $saveAsStream = false;
        if (isset($operation['stream']) && $operation['stream']) {
            $saveAsStream = isset($params['SaveAsStream']) ? $params['SaveAsStream'] : false;

            if ($saveAsStream) {
                if (isset($params['SaveAsFile'])) {
                    $obsException = new ObsException('SaveAsStream cannot be used with SaveAsFile together');
                    $obsException->setExceptionType('client');
                    throw $obsException;
                }
                if (isset($params['FilePath'])) {
                    $obsException = new ObsException('SaveAsStream cannot be used with FilePath together');
                    $obsException->setExceptionType('client');
                    throw $obsException;
                }
            }

            if (isset($params['SaveAsFile']) && isset($params['FilePath'])) {
                $obsException = new ObsException('SaveAsFile cannot be used with FilePath together');
                $obsException->setExceptionType('client');
                throw $obsException;
            }
        }
        return $this->httpClient->sendAsync($request, ['stream' => $saveAsStream])->then(
            function (Response $response) use ($model, $operation, $params, $callback, $startAsync, $originMethod, $request) {
                $statusCode = $response->getStatusCode();
                $readable = isset($params[ObsClient::OBS_BODY]) && ($params[ObsClient::OBS_BODY] instanceof StreamInterface || is_resource($params[ObsClient::OBS_BODY]));
                if ($statusCode === 307 && !$readable) {
                    if ($location = $response->getHeaderLine('location')) {
                        $url = parse_url($this->endpoint);
                        $newUrl = parse_url($location);
                        $scheme = (isset($newUrl['scheme']) ? $newUrl['scheme'] : $url['scheme']);
                        $defaultPort = strtolower($scheme) === 'https' ? '443' : '80';
                        return $this->doRequestAsync($model, $operation, $params, $callback, $startAsync, $originMethod, $scheme . '://' . $newUrl['host'] .
                            ':' . (isset($newUrl['port']) ? $newUrl['port'] : $defaultPort));
                    }
                }
                $this->parseResponse($model, $request, $response, $operation);
                unset($model['method']);
                $callback(null, $model);
            },
            function (RequestException $exception) use ($model, $operation, $params, $callback, $startAsync, $originMethod, $request, $requestCount) {
                $message = null;
                if ($exception instanceof ConnectException) {
                    if ($requestCount <= $this->maxRetryCount) {
                        return $this->sendRequestAsync($model, $operation, $params, $callback, $startAsync, $originMethod, $request, $requestCount + 1);
                    }
                    $message = 'Exceeded retry limitation, max retry count:' . $this->maxRetryCount . ', error message:' . $exception->getMessage();
                }
                $obsException = $this->parseExceptionAsync($request, $exception, $message);
                $callback($obsException, null);
            }
        );
    }

    private function createCommonSignedUrl(array $args = [])
    {
        if (!isset($args[ObsClient::OBS_METHOD])) {
            $obsException = new ObsException('Method param must be specified, allowed values: GET | PUT | HEAD | POST | DELETE | OPTIONS');
            $obsException->setExceptionType('client');
            throw $obsException;
        }
        $method = (string) $args[ObsClient::OBS_METHOD];
        $bucketName = isset($args[ObsClient::OBS_BUCKET]) ? (string) ($args[ObsClient::OBS_BUCKET]) : null;
        $objectKey = isset($args[ObsClient::OBS_KEY]) ? (string) ($args[ObsClient::OBS_KEY]) : null;
        $specialParam = isset($args['SpecialParam']) ? (string) ($args['SpecialParam']) : null;
        $expires = isset($args[ObsClient::OBS_EXPIRES]) && is_numeric($args[ObsClient::OBS_EXPIRES]) ? (int) ($args[ObsClient::OBS_EXPIRES]) : 300;

        $headers = [];
        if (isset($args['Headers']) && is_array($args['Headers'])) {
            foreach ($args['Headers'] as $key => $val) {
                if (is_string($key) && $key !== '') {
                    $headers[$key] = $val;
                }
            }
        }

        $queryParams = [];
        if (isset($args['QueryParams']) && is_array($args['QueryParams'])) {
            foreach ($args['QueryParams'] as $key => $val) {
                if (is_string($key) && $key !== '') {
                    $queryParams[$key] = $val;
                }
            }
        }

        if (!is_null($this->securityToken) && !isset($queryParams[OBSConstants::SECURITY_TOKEN_HEAD])) {
            $queryParams[OBSConstants::SECURITY_TOKEN_HEAD] = $this->securityToken;
        }

        $sign = new DefaultSignature($this->ak, $this->sk, $this->pathStyle, $this->endpoint, $method, $this->signature, $this->securityToken, $this->isCname);

        $url = parse_url($this->endpoint);
        $host = $url['host'];

        $result = '';

        if ($bucketName) {
            if ($this->pathStyle) {
                $result = '/' . $bucketName;
            } else {
                $host = $this->isCname ? $host : $bucketName . '.' . $host;
            }
        }

        $headers['Host'] = $host;

        if ($objectKey) {
            $objectKey = $sign->urlencodeWithSafe($objectKey);
            $result .= '/' . $objectKey;
        }

        $result .= '?';

        if ($specialParam) {
            $queryParams[$specialParam] = '';
        }

        $queryParams[OBSConstants::TEMPURL_AK_HEAD] = $this->ak;

        if (!is_numeric($expires) || $expires < 0) {
            $expires = 300;
        }
        $expires = ((int) $expires) + ((int) microtime(true));

        $queryParams[ObsClient::OBS_EXPIRES] = (string) $expires;

        $_queryParams = [];

        foreach ($queryParams as $key => $val) {
            $key = $sign->urlencodeWithSafe($key);
            $val = $sign->urlencodeWithSafe($val);
            $_queryParams[$key] = $val;
            $result .= $key;
            if ($val) {
                $result .= '=' . $val;
            }
            $result .= '&';
        }

        $canonicalstring = $sign->makeCanonicalstring($method, $headers, $_queryParams, $bucketName, $objectKey, $expires);
        $signatureContent = base64_encode(hash_hmac('sha1', $canonicalstring, $this->sk, true));

        $result .= 'Signature=' . $sign->urlencodeWithSafe($signatureContent);

        $model = new Model();
        $model->ActualSignedRequestHeaders = $headers;
        $model->SignedUrl = strtolower($url['scheme']) . '://' . $host . (isset($url['port']) && !in_array((int) $url['port'], [443, 80]) ? ':' . $url['port'] : '') . $result;

        return $model;
    }

    private function checkMimeType(string $method, array &$params)
    {
        // fix bug that guzzlehttp lib will add the content-type if not set
        if (($method === 'putObject' || $method === 'initiateMultipartUpload' || $method === 'uploadPart') && (!isset($params[ObsClient::OBS_CONTENT_TYPE]) || $params[ObsClient::OBS_CONTENT_TYPE] === null)) {
            if (isset($params[ObsClient::OBS_KEY])) {
                $params[ObsClient::OBS_CONTENT_TYPE] = Psr7\MimeType::fromFilename($params[ObsClient::OBS_KEY]);
            }

            if ((!isset($params[ObsClient::OBS_CONTENT_TYPE]) || $params[ObsClient::OBS_CONTENT_TYPE] === null) && isset($params['SourceFile'])) {
                $params[ObsClient::OBS_CONTENT_TYPE] = Psr7\MimeType::fromFilename($params['SourceFile']);
            }

            if (!isset($params[ObsClient::OBS_CONTENT_TYPE]) || $params[ObsClient::OBS_CONTENT_TYPE] === null) {
                $params[ObsClient::OBS_CONTENT_TYPE] = 'binary/octet-stream';
            }
        }
    }
}
