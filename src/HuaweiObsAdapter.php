<?php

namespace Jtz\HuaweiObsStorage;

use DateTimeInterface;
use Generator;
use League\Flysystem\FilesystemAdapter;
use Carbon\Carbon;
use Illuminate\Support\Arr;
use League\Flysystem\Config;
use League\Flysystem\DirectoryAttributes;
use League\Flysystem\FileAttributes;
use League\Flysystem\PathPrefixer;
use League\Flysystem\StorageAttributes;
use League\Flysystem\UnableToCheckDirectoryExistence;
use League\Flysystem\UnableToCheckExistence;
use League\Flysystem\UnableToCopyFile;
use League\Flysystem\UnableToCreateDirectory;
use League\Flysystem\UnableToDeleteFile;
use League\Flysystem\UnableToGeneratePublicUrl;
use League\Flysystem\UnableToGenerateTemporaryUrl;
use League\Flysystem\UnableToListContents;
use League\Flysystem\UnableToMoveFile;
use League\Flysystem\UnableToReadFile;
use League\Flysystem\UnableToRetrieveMetadata;
use League\Flysystem\UnableToSetVisibility;
use League\Flysystem\UnableToWriteFile;
use League\MimeTypeDetection\FinfoMimeTypeDetector;
use League\MimeTypeDetection\MimeTypeDetector;
use Jtz\HuaweiObsStorage\Contracts\PortableVisibilityConverter;
use Jtz\HuaweiObsStorage\Contracts\VisibilityConverter;
use Jtz\HuaweiObsStorage\Obs\ObsClient;
use Jtz\HuaweiObsStorage\Obs\ObsException;
use Throwable;

class HuaweiObsAdapter implements FilesystemAdapter
{
    /**
     * @var array<int, string>
     */
    protected const META_OPTIONS = [
        'CacheControl',
        'Expires',
        'SseKms',
        'MetadataDirective',
        'ACL',
        'ContentType',
        'ContentDisposition',
        'ContentLanguage',
        'ContentEncoding',
    ];

    /**
     * @var array<int, string>
     */
    private const EXTRA_METADATA_FIELDS = [
        'StorageClass',
        'ETag',
        'VersionId',
        'Metadata',
    ];

    // Huawei OBS Client ObsClient
    protected ObsClient $client;

    // bucket name
    protected string $bucket;

    protected string $hostname;

    protected bool $ssl;

    protected bool $isCname;

    protected string $epInternal;

    // 配置
    protected array $options = [
        'Multipart' => 128,
    ];

    private MimeTypeDetector $mimeTypeDetector;

    /**
     * The Flysystem PathPrefixes instance.
     *
     * @var PathPrefixer
     */
    protected PathPrefixer $prefixes;

    private VisibilityConverter $visibility;

    private string $domain;

    /**
     * HuaWeiOssAdapter constructor.
     */
    public function __construct(
        ObsClient $client,
        string $bucket,
        string $hostname,
        bool $ssl,
        bool $isCname,
        string $epInternal,
        string $prefix = '',
        ?VisibilityConverter $visibility = null,
        ?MimeTypeDetector $mimeTypeDetector = null,
        array $options = []
    ) {
        $this->client = $client;
        $this->bucket = $bucket;
        $this->hostname = $hostname;
        $this->ssl = $ssl;
        $this->isCname = $isCname;
        $this->epInternal = $epInternal;
        $this->prefixes = new PathPrefixer($prefix);
        $this->visibility = $visibility ?: new PortableVisibilityConverter();
        $this->mimeTypeDetector = $mimeTypeDetector ?: new FinfoMimeTypeDetector();
        $this->options = array_merge($this->options, $options);
        $this->domain = $this->isCname ? $this->hostname : $this->bucket . '.' . $this->hostname;
    }

    /**
     * 判断文件是否存在.
     * @throws UnableToCheckExistence
     */
    public function fileExists(string $path): bool
    {
        try {
            $options = [
                ObsClient::OBS_BUCKET => $this->bucket,
                ObsClient::OBS_KEY => $this->prefixes->prefixPath($path),
            ];

            $objectMetadata = $this->client->getObjectMetadata($options + $this->options);
            if(((int) $objectMetadata->HttpStatusCode) === 200) {
                return true;
            } else if(((int) $objectMetadata->HttpStatusCode) === 404){
                return false;
            }
            return   false;
        } catch (ObsException $exception) {
            $status = (int) $exception->getResponse()->getStatusCode();
            if (($status / 100) == 2 || $status === 404) {
                return $status === 200;
            }
            throw UnableToCheckExistence::forLocation($path, $exception);
        } catch (Throwable $exception) {
            throw UnableToCheckExistence::forLocation($path, $exception);
        }
    }

    /**
     * 判断文件夹是否存在.
     * @throws UnableToCheckDirectoryExistence
     */
    public function directoryExists($path): bool
    {
        try {
            $options = [
                ObsClient::OBS_BUCKET => $this->bucket,
                ObsClient::OBS_DELIMITER => '/',
                ObsClient::OBS_MARKER => '',
                ObsClient::OBS_MAX_KEYS => 1,
                ObsClient::OBS_PREFIX => $this->prefixes->prefixDirectoryPath($path),
            ];
            $listObjectInfo = $this->client->listObjects($options + $this->options);

            return !empty($listObjectInfo->Contents) || !empty($listObjectInfo->CommonPrefixes);
        } catch (Throwable $exception) {
            throw UnableToCheckDirectoryExistence::forLocation($path, $exception);
        }
    }

    /**
     * 写入文本.
     * @throws UnableToWriteFile
     */
    public function write(string $path, string $contents, Config $config): void
    {
        $this->upload($path, $contents, $config);
    }

    /**
     * 写入流.
     * @throws UnableToWriteFile
     */
    public function writeStream(string $path, $contents, Config $config): void
    {
        $this->upload($path, $contents, $config);
    }

    /**
     * 文本读取文件.
     * @throws UnableToReadFile
     */
    public function read(string $path): string
    {
        try {
            $options = [
                ObsClient::OBS_BUCKET => $this->bucket,
                ObsClient::OBS_KEY => $this->prefixes->prefixPath($path),
            ];

            $getObject = $this->client->getObject($options + $this->options);
            return $getObject->Body->getContents();
        } catch (ObsException $exception) {
            throw UnableToReadFile::fromLocation($path, $exception->getExceptionMessage(), $exception);
        } catch (Throwable $e) {
            throw UnableToReadFile::fromLocation($path, '', $e);
        }
    }

    /**
     * 流读取文件.
     * @throws UnableToReadFile
     */
    public function readStream(string $path)
    {
        try {
            $options = [
                ObsClient::OBS_BUCKET => $this->bucket,
                ObsClient::OBS_KEY => $this->prefixes->prefixPath($path),
            ];

            $getObject = $this->client->getObject($options + $this->options);
            return $getObject->Body->detach();
        } catch (ObsException $exception) {
            throw UnableToReadFile::fromLocation($path, $exception->getExceptionMessage(), $exception);
        } catch (Throwable $e) {
            throw UnableToReadFile::fromLocation($path, '', $e);
        }
    }

    /**
     * 删除文件.
     * @throws UnableToDeleteFile
     */
    public function delete(string $path): void
    {
        try {
            $options = [
                ObsClient::OBS_BUCKET => $this->bucket,
                ObsClient::OBS_KEY => $this->prefixes->prefixPath($path),
            ];

            $this->client->deleteObject($options + $this->options);
        } catch (ObsException $exception) {
            throw UnableToDeleteFile::atLocation($path, $exception->getExceptionMessage(), $exception);
        } catch (Throwable $exception) {
            throw UnableToDeleteFile::atLocation($path, '', $exception);
        }
    }

    /**
     * 删除目录.
     * @throws UnableToDeleteFile
     */
    public function deleteDirectory(string $path): void
    {
        try {
            $dirname = ltrim(rtrim($this->prefixes->prefixPath($path), '\\/') . '/', '\\/');

            $objects = $this->retrievePaginatedListing([
                ObsClient::OBS_MAX_KEYS => 1000,
                // ObsClient::OBS_DELIMITER => '/',
                ObsClient::OBS_MARKER => '',
                ObsClient::OBS_PREFIX => $dirname,
            ]);
            $delete = [];
            foreach ($objects as $object) {
                array_unshift($delete, [ObsClient::OBS_KEY => $object['Key'] ?? $object['Prefix']]);
            }
            array_push($delete, [ObsClient::OBS_KEY => $dirname]);

            $options = [
                ObsClient::OBS_BUCKET => $this->bucket,
                ObsClient::OBS_OBJECTS => $delete,
            ];

            $this->client->deleteObjects($options + $this->options);
        } catch (ObsException $exception) {
            throw UnableToDeleteFile::atLocation($path, $exception->getExceptionMessage(), $exception);
        } catch (Throwable $exception) {
            throw UnableToDeleteFile::atLocation($path, '', $exception);
        }
    }

    /**
     * 创建目录.
     * @throws UnableToCreateDirectory
     */
    public function createDirectory(string $path, Config $config): void
    {
        try {
            $this->client->putObject([
                    ObsClient::OBS_BUCKET => $this->bucket,
                    ObsClient::OBS_KEY => $this->prefixes->prefixPath($path) . '/',
                ] + $this->options + $this->getOptionsFromConfig($config));
        } catch (ObsException $exception) {
            throw UnableToCreateDirectory::atLocation($path, $exception->getExceptionMessage(), $exception);
        } catch (Throwable $exception) {
            throw UnableToCreateDirectory::atLocation($path, 'Unknown', $exception);
        }
    }

    /**
     * 设置权限.
     * @throws UnableToSetVisibility
     */
    public function setVisibility(string $path, string $visibility): void
    {
        try {
            $this->client->setObjectAcl([
                    ObsClient::OBS_BUCKET => $this->bucket,
                    ObsClient::OBS_KEY => $this->prefixes->prefixPath($path),
                    ObsClient::OBS_ACL => $this->visibility->visibilityToAcl($visibility),
                ] + $this->options);
        } catch (ObsException $exception) {
            throw UnableToSetVisibility::atLocation($path, $exception->getExceptionMessage(), $exception);
        } catch (Throwable $exception) {
            throw UnableToSetVisibility::atLocation($path, '', $exception);
        }
    }

    /**
     * 获取权限.
     * @throws UnableToRetrieveMetadata
     */
    public function visibility(string $path): FileAttributes
    {
        try {
            $acl = $this->client->getObjectAcl([
                    ObsClient::OBS_BUCKET => $this->bucket,
                    ObsClient::OBS_KEY => $this->prefixes->prefixPath($path),
                ] + $this->options);
        } catch (ObsException $exception) {
            throw UnableToRetrieveMetadata::visibility($path, $exception->getExceptionMessage(), $exception);
        } catch (Throwable $exception) {
            throw UnableToRetrieveMetadata::visibility($path, '', $exception);
        }

        $visibility = $this->visibility->aclToVisibility(!empty(Arr::where($acl->Grants, fn ($item) => strtolower(Arr::get($item, 'Grantee.URI', '')) === 'everyone' && strtolower(Arr::get($item, 'Permission', '')) === 'read')) ? 'READ' : 'PRIVATE');

        return new FileAttributes(path: $path, visibility: $visibility);
    }

    /**
     * 获取mimeType.
     * @throws UnableToRetrieveMetadata
     */
    public function mimeType(string $path): FileAttributes
    {
        $attributes = $this->fetchFileMetadata($path, StorageAttributes::ATTRIBUTE_MIME_TYPE);

        if ($attributes->mimeType() === null) {
            throw UnableToRetrieveMetadata::mimeType($path);
        }

        return $attributes;
    }

    /**
     * 获取lastModified.
     * @throws UnableToRetrieveMetadata
     */
    public function lastModified(string $path): FileAttributes
    {
        $attributes = $this->fetchFileMetadata($path, StorageAttributes::ATTRIBUTE_LAST_MODIFIED);

        if ($attributes->lastModified() === null) {
            throw UnableToRetrieveMetadata::lastModified($path);
        }

        return $attributes;
    }

    /**
     * 获取fileSize.
     * @throws UnableToRetrieveMetadata
     */
    public function fileSize(string $path): FileAttributes
    {
        $attributes = $this->fetchFileMetadata($path, StorageAttributes::ATTRIBUTE_FILE_SIZE);

        if ($attributes->fileSize() === null) {
            throw UnableToRetrieveMetadata::fileSize($path);
        }

        return $attributes;
    }

    /**
     * 枚举列表.
     * @throws UnableToWriteFile
     */
    public function listContents(string $path, bool $deep): iterable
    {
        $prefix = trim($this->prefixes->prefixPath($path), '\\/');
        $prefix = empty($prefix) ? '' : $prefix . '/';

        $options = [
            ObsClient::OBS_MAX_KEYS => 1000,
            ObsClient::OBS_MARKER => '',
            ObsClient::OBS_PREFIX => $prefix,
        ];

        if ($deep === false) {
            $options[ObsClient::OBS_DELIMITER] = '/';
        }
        $listing = $this->retrievePaginatedListing($options);

        try {
            foreach ($listing as $item) {
                yield $this->mapObsObjectMetadata((array) $item);
            }
        } catch (Throwable $exception) {
            throw UnableToListContents::atLocation($path, $deep, $exception);
        }
    }

    /**
     * 移动文件.
     * @throws UnableToCreateDirectory
     */
    public function move(string $from, string $to, Config $config): void
    {
        try {
            $this->copy($from, $to, $config);
            $this->delete($from);
        } catch (Throwable $exception) {
            throw UnableToMoveFile::fromLocationTo($from, $to, $exception);
        }
    }

    /**
     * copy文件.
     * @throws UnableToCopyFile
     */
    public function copy(string $from, string $to, Config $config): void
    {
        try {
            $visibility = $this->visibility($from)->visibility();
        } catch (Throwable $exception) {
            throw UnableToCopyFile::fromLocationTo($from, $to, $exception);
        }

        $options = $this->getOptions([ObsClient::OBS_ACL => $this->visibility->visibilityToAcl($visibility)] + $this->options, $config);

        try {
            $this->client->copyObject([
                    ObsClient::OBS_BUCKET => $this->bucket,
                    ObsClient::OBS_COPY_SOURCE => $this->bucket . '/' . $this->prefixes->prefixPath($from),
                    ObsClient::OBS_KEY => $this->prefixes->prefixPath($to),
                ] + $options);
        } catch (Throwable $exception) {
            throw UnableToCopyFile::fromLocationTo($from, $to, $exception);
        }
    }

    /**
     * appendFile并行桶才能有，测试没通过.
     * @throws UnableToWriteFile
     */
    public function appendFile(string $path, string $file, int $position, Config $config): void
    {
        try {
            $key = $this->prefixes->prefixPath($path);
            $options = $this->getOptions($this->options, $config);
            $shouldDetermineMimetype = $file !== '' && !array_key_exists(ObsClient::OBS_CONTENT_TYPE, $options);

            if ($shouldDetermineMimetype && $mimeType = $this->mimeTypeDetector->detectMimeType($key, $file)) {
                $options[ObsClient::OBS_CONTENT_TYPE] = $mimeType;
            }

            $this->client->appendFile([
                    ObsClient::OBS_BUCKET => $this->bucket,
                    ObsClient::OBS_KEY => $key,
                    ObsClient::OBS_BODY => $file,
                    ObsClient::OBS_POSITION => $position,
                ] + $options);
        } catch (ObsException $exception) {
            throw UnableToWriteFile::atLocation($path, $exception->getExceptionMessage(), $exception);
        } catch (Throwable $exception) {
            throw UnableToWriteFile::atLocation($path, 'Unknown', $exception);
        }
    }

    /**
     * appendObject.
     * @throws UnableToWriteFile
     */
    public function appendObject(string $path, string $content, int $position, Config $config): void
    {
        try {
            $this->client->appendObject([
                    ObsClient::OBS_BUCKET => $this->bucket,
                    ObsClient::OBS_KEY => $this->prefixes->prefixPath($path),
                    ObsClient::OBS_BODY => $content,
                    ObsClient::OBS_POSITION => $position,
                ] + $this->getOptions($this->options, $config));
        } catch (ObsException $exception) {
            throw UnableToWriteFile::atLocation($path, $exception->getExceptionMessage(), $exception);
        } catch (Throwable $exception) {
            throw UnableToWriteFile::atLocation($path, 'Unknown', $exception);
        }
    }

    /**
     * 获取公开地址.
     * @throws UnableToGeneratePublicUrl
     */
    public function getUrl(string $path): string
    {
        try {
            return ($this->ssl ? 'https://' : 'http://') . $this->domain . '/' . ltrim($path, '\\/');
        } catch (Throwable $exception) {
            throw UnableToGeneratePublicUrl::dueToError($path, $exception);
        }
    }

    /**
     * 获取临时地址.
     */
    public function getTemporaryUrl(string $path, DateTimeInterface $expiration, array $options = []): string
    {
        try {
            $url = $this->client->createSignedUrl([
                    ObsClient::OBS_BUCKET => $this->bucket,
                    ObsClient::OBS_KEY => $this->prefixes->prefixPath($path),
                    ObsClient::OBS_EXPIRES => Carbon::now()->diffInSeconds(Carbon::parse($expiration)),
                    ObsClient::OBS_METHOD => $options[ObsClient::OBS_METHOD] ?? ObsClient::OBS_HTTP_GET,
                ] + $options + $this->options);
            if ($this->epInternal == $this->hostname) {
                return $url->SignedUrl;
            }
            return preg_replace(sprintf('/%s/', preg_quote($this->bucket . '.' . $this->epInternal)), $this->domain, $url->SignedUrl, 1);
        } catch (Throwable $exception) {
            throw UnableToGenerateTemporaryUrl::dueToError($path, $exception);
        }
    }

    /**
     * Get options for an OBS call. done.
     */
    protected function getOptions(array $options = [], Config $config = null): array
    {
        $options = array_merge($this->options, $options);

        if (!is_null($config)) {
            $options = array_merge($options, $this->getOptionsFromConfig($config));
        }

        foreach ([ObsClient::OBS_CONTENT_TYPE, ObsClient::OBS_CONTENT_LENGTH] as $key) {
            if ($value = $config->get($key)) {
                $options[$key] = $value;
            }
        }

        return $options;
    }

    /**
     * Retrieve options from a Config instance. done.
     */
    protected function getOptionsFromConfig(Config $config): array
    {
        $options = [];

        foreach (static::META_OPTIONS as $option) {
            $value = $config->get($option, '__NOT_SET__');

            if ($value !== '__NOT_SET__') {
                $options[$option] = $value;
            }
        }

        if ($visibility = $config->get(Config::OPTION_VISIBILITY)) {
            // For local reference
            // $options['visibility'] = $visibility;
            // For external reference
            $options[ObsClient::OBS_ACL] = $this->visibility->visibilityToAcl($visibility);
        }

        if ($mimetype = $config->get('mimetype')) {
            // For local reference
            // $options['mimetype'] = $mimetype;
            // For external reference
            $options[ObsClient::OBS_CONTENT_TYPE] = $mimetype;
        }

        return $options;
    }


    private function retrievePaginatedListing(array $options, bool $recursive = false): Generator
    {
        while (true) {
            $options = [
                    ObsClient::OBS_BUCKET => $this->bucket,
                ] + $options;

            $listObjectInfo = $this->client->listObjects($options + $this->options);
            $options[ObsClient::OBS_MARKER] = $listObjectInfo->NextMarker;

            foreach ($listObjectInfo->CommonPrefixes ?? [] as $object) {
                yield [
                    'Prefix' => $object['Prefix'],
                ];
                if ($recursive) {
                    yield from $this->retrievePaginatedListing([
                            ObsClient::OBS_MARKER => '',
                            ObsClient::OBS_PREFIX => $object['Prefix'],
                        ] + $options + $this->options, $recursive);
                }
            }

            foreach ($listObjectInfo->Contents as $object) {
                if ($object['Key'] === $options[ObsClient::OBS_PREFIX] && $object['Size'] === 0) {
                    continue;
                }
                if (substr($object['Key'], -1) === '/') {
                    yield [
                        'Prefix' => $object['Key'],
                    ];
                } else {
                    yield [
                        'Key' => $object['Key'],
                        'LastModified' => $object['LastModified'],
                        'ETag' => $object['ETag'],
                        'Type' => $object['Type'],
                        'ContentLength' => $object['Size'],
                        'StorageClass' => $object['StorageClass'],
                    ];
                }
            }

            // 没有更多结果了
            if ($listObjectInfo->IsTruncated !== true) {
                break;
            }
        }
    }


    private function fetchFileMetadata(string $path, string $type): FileAttributes
    {
        try {
            $objectMeta = $this->client->getObjectMetadata([
                    ObsClient::OBS_BUCKET => $this->bucket,
                    ObsClient::OBS_KEY => $this->prefixes->prefixPath($path),
                ] + $this->options);
        } catch (ObsException $exception) {
            throw UnableToRetrieveMetadata::create($path, $type, $exception->getExceptionMessage(), $exception);
        } catch (Throwable $exception) {
            throw UnableToRetrieveMetadata::create($path, $type, '', $exception);
        }

        $attributes = $this->mapObsObjectMetadata($objectMeta->toArray(), $path);

        if (!$attributes instanceof FileAttributes) {
            throw UnableToRetrieveMetadata::create($path, $type);
        }

        return $attributes;
    }

    private function mapObsObjectMetadata(array $metadata, string $path = null): StorageAttributes
    {
        if ($path === null) {
            $path = $this->prefixes->stripPrefix($metadata['Key'] ?? $metadata['Prefix']);
        }

        $path = $path ?: '/'; // 修复根目录

        if (substr($path, -1) === '/') {
            return new DirectoryAttributes(rtrim($path, '\\/'));
        }

        $mimetype = $metadata['ContentType'] ?? null;
        $fileSize = $metadata['ContentLength'] ?? null;
        $fileSize = $fileSize === null ? null : (int) $fileSize;
        $dateTime = $metadata['LastModified'] ?? null;
        $lastModified = !is_null($dateTime) ? Carbon::parse($dateTime)->getTimeStamp() : null;

        return new FileAttributes(
            path: $path,
            fileSize: $fileSize,
            lastModified: $lastModified,
            mimeType: $mimetype,
            extraMetadata: $this->extractExtraMetadata($metadata)
        );
    }


    private function extractExtraMetadata(array $metadata): array
    {
        $extracted = [];

        foreach (static::EXTRA_METADATA_FIELDS as $field) {
            if (isset($metadata[$field]) && $metadata[$field] !== '') {
                $extracted[$field] = $metadata[$field];
            }
        }

        return $extracted;
    }

    /**
     * @param string|resource $body
     * @throws UnableToWriteFile
     */
    private function upload(string $path, mixed $body, Config $config): void
    {
        $key = $this->prefixes->prefixPath($path);
        $options = $this->getOptions($this->options, $config);

        $shouldDetermineMimetype = $body !== '' && !array_key_exists(ObsClient::OBS_CONTENT_TYPE, $options);

        if ($shouldDetermineMimetype && $mimeType = $this->mimeTypeDetector->detectMimeType($key, $body)) {
            $options[ObsClient::OBS_CONTENT_TYPE] = $mimeType;
        }

        try {
            $this->client->putObject([
                    ObsClient::OBS_BUCKET => $this->bucket,
                    ObsClient::OBS_KEY => $key,
                    ObsClient::OBS_BODY => $body,
                ] + $options);
        } catch (ObsException $exception) {
            throw UnableToWriteFile::atLocation($path, $exception->getExceptionMessage(), $exception);
        } catch (Throwable $exception) {
            throw UnableToWriteFile::atLocation($path, 'Unknown', $exception);
        }
    }
}
