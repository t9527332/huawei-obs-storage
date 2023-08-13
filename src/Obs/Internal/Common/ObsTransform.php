<?php

namespace Jtz\HuaweiObsStorage\Obs\Internal\Common;

use Jtz\HuaweiObsStorage\Obs\ObsClient;

class ObsTransform implements ITransform
{
    private static ?ObsTransform $instance = null;

    private static array $search_repalce = ['STANDARD' => ObsClient::StorageClassStandard, 'STANDARD_IA' => ObsClient::StorageClassWarm, 'GLACIER' => ObsClient::StorageClassCold];

    public static function getInstance(): static|self
    {
        if (is_null(self::$instance)) {
            self::$instance = new ObsTransform();
        }
        return self::$instance;
    }

    public function transform(string $sign, string $para): ?string
    {
        return match ($sign) {
            'aclHeader' => $this->transAclHeader($para),
            'aclUri' => $this->transAclGroupUri($para),
            'event' => $this->transNotificationEvent($para),
            'storageClass' => $this->transStorageClass($para),
            default => $para,
        };
    }

    private function transAclHeader(string $para): ?string
    {
        if (
            $para === ObsClient::AclAuthenticatedRead || $para === ObsClient::AclBucketOwnerRead ||
            $para === ObsClient::AclBucketOwnerFullControl || $para === ObsClient::AclLogDeliveryWrite
        ) {
            $para = null;
        }
        return $para;
    }

    private function transAclGroupUri(string $para): string
    {
        if ($para === ObsClient::GroupAllUsers) {
            $para = ObsClient::AllUsers;
        }
        return $para;
    }

    private function transNotificationEvent(string $para): string
    {
        $pos = strpos($para, 's3:');
        if ($pos !== false && $pos === 0) {
            $para = substr($para, 3);
        }
        return $para;
    }

    private function transStorageClass(string $para): string
    {
        return strtr($para, self::$search_repalce);
    }
}
