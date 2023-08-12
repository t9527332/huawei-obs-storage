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

namespace Jtz\HuaweiObsStorage\Obs\Internal\Resource;

class OBSConstants extends Constants
{
    public const FLAG = 'OBS';

    public const METADATA_PREFIX = 'x-obs-meta-';

    public const HEADER_PREFIX = 'x-obs-';

    public const ALTERNATIVE_DATE_HEADER = 'x-obs-date';

    public const SECURITY_TOKEN_HEAD = 'x-obs-security-token';

    public const TEMPURL_AK_HEAD = 'AccessKeyId';

    public const COMMON_HEADERS = [
        'content-length' => 'ContentLength',
        'date' => 'Date',
        'x-obs-request-id' => 'RequestId',
        'x-obs-id-2' => 'Id2',
        'x-reserved' => 'Reserved',
    ];
}
