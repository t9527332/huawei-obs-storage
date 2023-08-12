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

use GuzzleHttp\Client;
use GuzzleHttp\Handler\CurlHandler;
use GuzzleHttp\Handler\CurlMultiHandler;
use GuzzleHttp\Handler\Proxy;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Promise\Promise;
use Jtz\HuaweiObsStorage\Obs\Internal\Common\Model;
use Jtz\HuaweiObsStorage\Obs\Internal\Common\SdkCurlFactory;
use Jtz\HuaweiObsStorage\Obs\Internal\Common\SdkStreamHandler;

/**
 * @method Model createPostSignature(array $args=[]);
 * @method Model createSignedUrl(array $args=[]);
 * @method Model createBucket(array $args = []);
 * @method Model listBuckets();
 * @method Model deleteBucket(array $args = []);
 * @method Model listObjects(array $args = []);
 * @method Model listVersions(array $args = []);
 * @method Model headBucket(array $args = []);
 * @method Model getBucketMetadata(array $args = []);
 * @method Model getBucketLocation(array $args = []);
 * @method Model getBucketStorageInfo(array $args = []);
 * @method Model setBucketQuota(array $args = []);
 * @method Model getBucketQuota(array $args = []);
 * @method Model setBucketStoragePolicy(array $args = []);
 * @method Model getBucketStoragePolicy(array $args = []);
 * @method Model setBucketAcl(array $args = []);
 * @method Model getBucketAcl(array $args = []);
 * @method Model setBucketLogging(array $args = []);
 * @method Model getBucketLogging(array $args = []);
 * @method Model setBucketPolicy(array $args = []);
 * @method Model getBucketPolicy(array $args = []);
 * @method Model deleteBucketPolicy(array $args = []);
 * @method Model setBucketLifecycle(array $args = []);
 * @method Model getBucketLifecycle(array $args = []);
 * @method Model deleteBucketLifecycle(array $args = []);
 * @method Model setBucketWebsite(array $args = []);
 * @method Model getBucketWebsite(array $args = []);
 * @method Model deleteBucketWebsite(array $args = []);
 * @method Model setBucketVersioning(array $args = []);
 * @method Model getBucketVersioning(array $args = []);
 * @method Model setBucketCors(array $args = []);
 * @method Model getBucketCors(array $args = []);
 * @method Model deleteBucketCors(array $args = []);
 * @method Model setBucketNotification(array $args = []);
 * @method Model getBucketNotification(array $args = []);
 * @method Model setBucketTagging(array $args = []);
 * @method Model getBucketTagging(array $args = []);
 * @method Model deleteBucketTagging(array $args = []);
 * @method Model optionsBucket(array $args = []);
 * @method Model getFetchPolicy(array $args = []);
 * @method Model setFetchPolicy(array $args = []);
 * @method Model deleteFetchPolicy(array $args = []);
 * @method Model setFetchJob(array $args = []);
 * @method Model getFetchJob(array $args = []);
 *
 * @method Model putObject(array $args = []);
 * @method Model getObject(array $args = []);
 * @method Model copyObject(array $args = []);
 * @method Model deleteObject(array $args = []);
 * @method Model deleteObjects(array $args = []);
 * @method Model getObjectMetadata(array $args = []);
 * @method Model setObjectAcl(array $args = []);
 * @method Model getObjectAcl(array $args = []);
 * @method Model initiateMultipartUpload(array $args = []);
 * @method Model uploadPart(array $args = []);
 * @method Model copyPart(array $args = []);
 * @method Model listParts(array $args = []);
 * @method Model completeMultipartUpload(array $args = []);
 * @method Model abortMultipartUpload(array $args = []);
 * @method Model listMultipartUploads(array $args = []);
 * @method Model optionsObject(array $args = []);
 * @method Model restoreObject(array $args = []);
 *
 * @method Promise createBucketAsync(array $args = [], callable $callback);
 * @method Promise listBucketsAsync(callable $callback);
 * @method Promise deleteBucketAsync(array $args = [], callable $callback);
 * @method Promise listObjectsAsync(array $args = [], callable $callback);
 * @method Promise listVersionsAsync(array $args = [], callable $callback);
 * @method Promise headBucketAsync(array $args = [], callable $callback);
 * @method Promise getBucketMetadataAsync(array $args = [], callable $callback);
 * @method Promise getBucketLocationAsync(array $args = [], callable $callback);
 * @method Promise getBucketStorageInfoAsync(array $args = [], callable $callback);
 * @method Promise setBucketQuotaAsync(array $args = [], callable $callback);
 * @method Promise getBucketQuotaAsync(array $args = [], callable $callback);
 * @method Promise setBucketStoragePolicyAsync(array $args = [], callable $callback);
 * @method Promise getBucketStoragePolicyAsync(array $args = [], callable $callback);
 * @method Promise setBucketAclAsync(array $args = [], callable $callback);
 * @method Promise getBucketAclAsync(array $args = [], callable $callback);
 * @method Promise setBucketLoggingAsync(array $args = [], callable $callback);
 * @method Promise getBucketLoggingAsync(array $args = [], callable $callback);
 * @method Promise setBucketPolicyAsync(array $args = [], callable $callback);
 * @method Promise getBucketPolicyAsync(array $args = [], callable $callback);
 * @method Promise deleteBucketPolicyAsync(array $args = [], callable $callback);
 * @method Promise setBucketLifecycleAsync(array $args = [], callable $callback);
 * @method Promise getBucketLifecycleAsync(array $args = [], callable $callback);
 * @method Promise deleteBucketLifecycleAsync(array $args = [], callable $callback);
 * @method Promise setBucketWebsiteAsync(array $args = [], callable $callback);
 * @method Promise getBucketWebsiteAsync(array $args = [], callable $callback);
 * @method Promise deleteBucketWebsiteAsync(array $args = [], callable $callback);
 * @method Promise setBucketVersioningAsync(array $args = [], callable $callback);
 * @method Promise getBucketVersioningAsync(array $args = [], callable $callback);
 * @method Promise setBucketCorsAsync(array $args = [], callable $callback);
 * @method Promise getBucketCorsAsync(array $args = [], callable $callback);
 * @method Promise deleteBucketCorsAsync(array $args = [], callable $callback);
 * @method Promise setBucketNotificationAsync(array $args = [], callable $callback);
 * @method Promise getBucketNotificationAsync(array $args = [], callable $callback);
 * @method Promise setBucketTaggingAsync(array $args = [], callable $callback);
 * @method Promise getBucketTaggingAsync(array $args = [], callable $callback);
 * @method Promise deleteBucketTaggingAsync(array $args = [], callable $callback);
 * @method Promise optionsBucketAsync(array $args = [], callable $callback);
 *
 * @method Promise putObjectAsync(array $args = [], callable $callback);
 * @method Promise getObjectAsync(array $args = [], callable $callback);
 * @method Promise copyObjectAsync(array $args = [], callable $callback);
 * @method Promise deleteObjectAsync(array $args = [], callable $callback);
 * @method Promise deleteObjectsAsync(array $args = [], callable $callback);
 * @method Promise getObjectMetadataAsync(array $args = [], callable $callback);
 * @method Promise setObjectAclAsync(array $args = [], callable $callback);
 * @method Promise getObjectAclAsync(array $args = [], callable $callback);
 * @method Promise initiateMultipartUploadAsync(array $args = [], callable $callback);
 * @method Promise uploadPartAsync(array $args = [], callable $callback);
 * @method Promise copyPartAsync(array $args = [], callable $callback);
 * @method Promise listPartsAsync(array $args = [], callable $callback);
 * @method Promise completeMultipartUploadAsync(array $args = [], callable $callback);
 * @method Promise abortMultipartUploadAsync(array $args = [], callable $callback);
 * @method Promise listMultipartUploadsAsync(array $args = [], callable $callback);
 * @method Promise optionsObjectAsync(array $args = [], callable $callback);
 * @method Promise restoreObjectAsync(array $args = [], callable $callback);
 * @method Model getFetchPolicyAsync(array $args = [], callable $callback);
 * @method Model setFetchPolicyAsync(array $args = [], callable $callback);
 * @method Model deleteFetchPolicyAsync(array $args = [], callable $callback);
 * @method Model setFetchJobAsync(array $args = [], callable $callback);
 * @method Model getFetchJobAsync(array $args = [], callable $callback);
 */
class ObsClient
{
    use Internal\SendRequestTrait;
    use Internal\GetResponseTrait;

    public const SDK_VERSION = '3.21.9';

    public const AclPrivate = 'private';

    public const AclPublicRead = 'public-read';

    public const AclPublicReadWrite = 'public-read-write';

    public const AclPublicReadDelivered = 'public-read-delivered';

    public const AclPublicReadWriteDelivered = 'public-read-write-delivered';

    public const AclAuthenticatedRead = 'authenticated-read';

    public const AclBucketOwnerRead = 'bucket-owner-read';

    public const AclBucketOwnerFullControl = 'bucket-owner-full-control';

    public const AclLogDeliveryWrite = 'log-delivery-write';

    public const StorageClassStandard = 'STANDARD';

    public const StorageClassWarm = 'WARM';

    public const StorageClassCold = 'COLD';

    public const PermissionRead = 'READ';

    public const PermissionWrite = 'WRITE';

    public const PermissionReadAcp = 'READ_ACP';

    public const PermissionWriteAcp = 'WRITE_ACP';

    public const PermissionFullControl = 'FULL_CONTROL';

    public const AllUsers = 'Everyone';

    public const GroupAllUsers = 'AllUsers';

    public const GroupAuthenticatedUsers = 'AuthenticatedUsers';

    public const GroupLogDelivery = 'LogDelivery';

    public const RestoreTierExpedited = 'Expedited';

    public const RestoreTierStandard = 'Standard';

    public const RestoreTierBulk = 'Bulk';

    public const GranteeGroup = 'Group';

    public const GranteeUser = 'CanonicalUser';

    public const CopyMetadata = 'COPY';

    public const ReplaceMetadata = 'REPLACE';

    public const SignatureV2 = 'v2';

    public const SignatureV4 = 'v4';

    public const SigantureObs = 'obs';

    public const ObjectCreatedAll = 'ObjectCreated:*';

    public const ObjectCreatedPut = 'ObjectCreated:Put';

    public const ObjectCreatedPost = 'ObjectCreated:Post';

    public const ObjectCreatedCopy = 'ObjectCreated:Copy';

    public const ObjectCreatedCompleteMultipartUpload = 'ObjectCreated:CompleteMultipartUpload';

    public const ObjectRemovedAll = 'ObjectRemoved:*';

    public const ObjectRemovedDelete = 'ObjectRemoved:Delete';

    public const ObjectRemovedDeleteMarkerCreated = 'ObjectRemoved:DeleteMarkerCreated';

    // ======

    public const OBS_BUCKET = 'Bucket';

    public const OBS_KEY = 'Key';

    public const OBS_COPY_SOURCE = 'CopySource';

    public const OBS_PREFIX = 'Prefix';

    public const OBS_MARKER = 'Marker';

    public const OBS_MAX_KEYS = 'MaxKeys';

    public const OBS_OBJECTS = 'Objects';

    public const OBS_DELIMITER = 'Delimiter';

    public const OBS_KEY_MARKER = 'KeyMarker';

    public const OBS_VERSION_ID_MARKER = 'VersionIdMarker';

    public const OBS_VERSION_ID = 'VersionId';

    public const OBS_ENCODING_TYPE = 'EncodingType';

    public const OBS_CONTENT_TYPE = 'ContentType';

    public const OBS_BODY = 'Body';

    public const OBS_EXPIRES = 'Expires';

    public const OBS_CONTENT_LENGTH = 'ContentLength';

    public const OBS_POSITION = 'Position';

    public const OBS_ACL = 'ACL';

    public const OBS_METHOD = 'Method';

    public const OBS_HTTP_GET = 'GET';

    public const OBS_HTTP_PUT = 'PUT';

    public const OBS_HTTP_POST = 'POST';

    public const OBS_HTTP_DELETE = 'DELETE';

    public const OBS_HTTP_HEAD = 'HEAD';

    public const OBS_HTTP_OPTIONS = 'OPTIONS';
    // ======

    protected static ?string $defaultAgent = null;

    private array $factorys;

    public function __construct(array $config = [])
    {
        $this->factorys = [];

        $this->ak = (string) $config['key'];
        $this->sk = (string) $config['secret'];

        if (isset($config['security_token'])) {
            $this->securityToken = (string) $config['security_token'];
        }

        if (isset($config['endpoint'])) {
            $this->endpoint = trim((string) $config['endpoint']);
        }

        if ($this->endpoint === '') {
            throw new \RuntimeException('endpoint is not set');
        }

        while ($this->endpoint[strlen($this->endpoint) - 1] === '/') {
            $this->endpoint = substr($this->endpoint, 0, strlen($this->endpoint) - 1);
        }

        if (strpos($this->endpoint, 'http') !== 0) {
            $this->endpoint = 'https://' . $this->endpoint;
        }

        if (isset($config['signature'])) {
            $this->signature = (string) $config['signature'];
        }

        if (isset($config['path_style'])) {
            $this->pathStyle = (bool) $config['path_style'];
        }

        if (isset($config['region'])) {
            $this->region = (string) $config['region'];
        }

        if (isset($config['ssl_verify'])) {
            $this->sslVerify = $config['ssl_verify'];
        } elseif (isset($config['ssl.certificate_authority'])) {
            $this->sslVerify = $config['ssl.certificate_authority'];
        }

        if (isset($config['max_retry_count'])) {
            $this->maxRetryCount = (int) $config['max_retry_count'];
        }

        if (isset($config['timeout'])) {
            $this->timeout = (int) $config['timeout'];
        }

        if (isset($config['socket_timeout'])) {
            $this->socketTimeout = (int) $config['socket_timeout'];
        }

        if (isset($config['connect_timeout'])) {
            $this->connectTimeout = (int) $config['connect_timeout'];
        }

        if (isset($config['chunk_size'])) {
            $this->chunkSize = (int) $config['chunk_size'];
        }

        if (isset($config['exception_response_mode'])) {
            $this->exceptionResponseMode = $config['exception_response_mode'];
        }

        if (isset($config['is_cname'])) {
            $this->isCname = $config['is_cname'];
        }

        if (isset($config['proxy'])) {
            $this->proxy = $config['proxy'];
        }

        $host = parse_url($this->endpoint)['host'];
        if (filter_var($host, FILTER_VALIDATE_IP) !== false) {
            $this->pathStyle = true;
        }

        $handler = self::choose_handler($this);

        $this->httpClient = new Client(
            [
                'timeout' => 0,
                'read_timeout' => $this->socketTimeout,
                'connect_timeout' => $this->connectTimeout,
                'allow_redirects' => false,
                'verify' => $this->sslVerify,
                'expect' => false,
                'proxy' => $this->proxy,
                'handler' => HandlerStack::create($handler),
                'curl' => [
                    CURLOPT_BUFFERSIZE => $this->chunkSize,
                ],
            ]
        );
    }

    public function __destruct()
    {
        $this->close();
    }

    public function refresh(string $key, string $secret, ?string $security_token = null)
    {
        $this->ak = (string) $key;
        $this->sk = (string) $secret;
        if ($security_token) {
            $this->securityToken = $security_token;
        }
    }

    /**
     * Factory method to create a new Obs client using an array of configuration options.
     *
     * @param array $config Client configuration data
     */
    public static function factory(array $config = []): self
    {
        return new ObsClient($config);
    }

    public function close(): void
    {
        if ($this->factorys) {
            foreach ($this->factorys as $factory) {
                $factory->close();
            }
        }
    }

    /**
     * Get the default User-Agent string to use with Guzzle.
     */
    private static function default_user_agent(): string
    {
        if (is_null(self::$defaultAgent)) {
            self::$defaultAgent = 'obs-sdk-php/' . self::SDK_VERSION;
        }

        return self::$defaultAgent;
    }

    private static function choose_handler($obsclient)
    {
        $handler = null;
        if (function_exists('curl_multi_exec') && function_exists('curl_exec')) {
            $f1 = new SdkCurlFactory(50);
            $f2 = new SdkCurlFactory(3);
            $obsclient->factorys[] = $f1;
            $obsclient->factorys[] = $f2;
            $handler = Proxy::wrapSync(new CurlMultiHandler(['handle_factory' => $f1]), new CurlHandler(['handle_factory' => $f2]));
        } elseif (function_exists('curl_exec')) {
            $f = new SdkCurlFactory(3);
            $obsclient->factorys[] = $f;
            $handler = new CurlHandler(['handle_factory' => $f]);
        } elseif (function_exists('curl_multi_exec')) {
            $f1 = new SdkCurlFactory(50);
            $f = new SdkCurlFactory(50);
            $obsclient->factorys[] = $f;
            $handler = new CurlMultiHandler(['handle_factory' => $f1]);
        }

        if (ini_get('allow_url_fopen')) {
            $handler = $handler
                ? Proxy::wrapStreaming($handler, new SdkStreamHandler())
                : new SdkStreamHandler();
        } elseif (!$handler) {
            throw new \RuntimeException('GuzzleHttp requires cURL, the allow_url_fopen ini setting, or a custom HTTP handler.');
        }

        return $handler;
    }
}
