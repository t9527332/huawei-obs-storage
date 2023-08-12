<?php

namespace Jtz\HuaweiObsStorage\Providers;

use Illuminate\Filesystem\FilesystemAdapter;
use Illuminate\Support\ServiceProvider;
use League\Flysystem\Config;
use League\Flysystem\Filesystem;
use Jtz\HuaweiObsStorage\HuaweiObsAdapter;
use Jtz\HuaweiObsStorage\Obs\ObsClient;

class HuaweiObsServiceProvider extends ServiceProvider
{
    /**
     * Bootstrap the application services.
     */
    public function boot(): void
    {
        $this->setupConfig();

        $this->app->make('filesystem')->extend('obs', function ($app, array $config) {
            $cdnDomain = empty($config['cdnDomain']) ? '' : $config['cdnDomain'];
            $ssl = empty($config['ssl']) ? false : (bool) $config['ssl'];
            $isCname = empty($cdnDomain) ? false : true;

            $hostname = $isCname ? $cdnDomain : $config['endpoint'];

            $epInternal = empty($config['endpoint_internal']) ? $hostname : $config['endpoint_internal']; // 内部节点

            $client = new ObsClient([
                'key' => $config['access_id'],
                'secret' => $config['access_key'],
                'proxy' => $config['proxy'] ?? null,
                'endpoint' => $epInternal,
                'ssl_verify' => $ssl,
                'is_cname' => $isCname ? empty($config['endpoint_internal']) : false,
            ]);

            $adapter = new HuaweiObsAdapter($client, $config['bucket'], $hostname, $ssl, $isCname, $epInternal, $config['prefix'] ?? '', options: $config['options'] ?? []);

            FilesystemAdapter::macro('appendFile', fn (string $path, string $file, int $position = 0, array $config = []) => $adapter->appendFile($path, $file, $position, new Config($config)));
            FilesystemAdapter::macro('appendObject', fn (string $path, string $content, int $position = 0, array $config = []) => $adapter->appendObject($path, $content, $position, new Config($config)));

            return new FilesystemAdapter(new Filesystem($adapter, $config), $adapter, $config);
        });
    }

    /**
     * Register the application services.
     *
     * @return void
     */
    public function register()
    {
    }

    /**
     * Setup the config.
     *
     * @return void
     */
    protected function setupConfig()
    {
        $this->mergeConfigFrom(realpath(__DIR__ . '/../../config/config.php'), 'filesystems.disks.obs');
    }
}
