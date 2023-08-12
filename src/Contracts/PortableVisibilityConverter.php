<?php

namespace Jtz\HuaweiObsStorage\Contracts;

use League\Flysystem\Visibility;

class PortableVisibilityConverter implements VisibilityConverter
{
    public const PRIVATE_ACL = 'private';

    public const PUBLIC_READ_ACL = 'public-read';

    public const PUBLIC_READ_WRITE_ACL = 'public-read-write';

    /**
     * @var string
     */
    private $defaultForDirectories;

    public function __construct(string $defaultForDirectories = Visibility::PUBLIC)
    {
        $this->defaultForDirectories = $defaultForDirectories;
    }

    public function visibilityToAcl(string $visibility): string
    {
        if ($visibility === Visibility::PUBLIC) {
            return self::PUBLIC_READ_ACL;
        }

        return self::PRIVATE_ACL;
    }

    public function aclToVisibility(string $acl): string
    {
        return match ($acl) {
            'READ' => Visibility::PUBLIC,
            default => Visibility::PRIVATE
        };
    }

    public function defaultForDirectories(): string
    {
        return $this->defaultForDirectories;
    }
}
