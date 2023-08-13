# HuaWei-OBS-Storage for Laravel9+

# Installation
`composer require jtz/huawei-obs-storage`

# Configuration
Add the following in `app/filesystems.php`:
```php
'disks'=>[
    ...
    'obs' => [
            'driver' => 'obs',
            'access_id' => env('OBS_ACCESS_KEY_ID'),
            'access_key' => env('OBS_ACCESS_KEY_SECRET'),
            'bucket' => env('OBS_BUCKET'),
            'endpoint' => env('OBS_ENDPOINT'), // OBS 外网节点或自定义外部域名
            'endpoint_internal' => env('OBS_ENDPOINT_INTERNAL'), // 如果为空，则默认使用 endpoint 配置
            'cdnDomain' => env('OBS_DOMAIN'), // 如果不为空，getUrl会判断cdnDomain是否设定来决定返回的url，如果cdnDomain未设置，则使用endpoint来生成url，否则使用cdn
            'ssl' => env('OBS_SSL', false), // true to use 'https://' and false to use 'http://'. default is false,
            'prefix' => env('OBS_PREFIX'), // 路径前缀
            'options' => [],
            'throw' => true,
    ],
    ...
]
```
## Usage
See [Larave doc for Storage](https://laravel.com/docs/9.x/filesystem#custom-filesystems)
Or you can learn here:

> First you must use Storage facade

```php
use Illuminate\Support\Facades\Storage;
```    
> Then You can use all APIs of laravel Storage

```php
Storage::disk('obs'); // if default filesystems driver is obs, you can skip this step

//fetch all files of specified bucket(see upond configuration)
Storage::files($directory);
Storage::allFiles($directory);

Storage::put('path/to/file/file.jpg', $contents); //first parameter is the target file path, second paramter is file content
Storage::putFile('path/to/file/file.jpg', 'local/path/to/local_file.jpg'); // upload file from local path

Storage::get('path/to/file/file.jpg'); // get the file object by path
Storage::exists('path/to/file/file.jpg'); // determine if a given file exists on the storage(OBS)
Storage::size('path/to/file/file.jpg'); // get the file size (Byte)
Storage::lastModified('path/to/file/file.jpg'); // get date of last modification

Storage::directories($directory); // Get all of the directories within a given directory
Storage::allDirectories($directory); // Get all (recursive) of the directories within a given directory

Storage::copy('old/file1.jpg', 'new/file1.jpg');
Storage::move('old/file1.jpg', 'new/file1.jpg');
Storage::rename('path/to/file1.jpg', 'path/to/file2.jpg');

Storage::prepend('file.log', 'Prepended Text'); // Prepend to a file.
Storage::append('file.log', 'Appended Text'); // Append to a file.

Storage::delete('file.jpg');
Storage::delete(['file1.jpg', 'file2.jpg']);

Storage::makeDirectory($directory); // Create a directory.
Storage::deleteDirectory($directory); // Recursively delete a directory.It will delete all files within a given directory, SO Use with caution please.
Storage::url('path/to/img.jpg') // get the file url
Storage::temporaryUrl('path/to/img.jpg', 900) // Get a temporary URL for the file at the given path.
```

## Documentation
More development detail see [Huawei OBS DOC](https://support.huaweicloud.com/api-obs/obs_04_0079.html)
## License
Except for the Obs directory the source code is released under the MIT license. Read the license file for more information.
Obs is Apache License 2.0.
