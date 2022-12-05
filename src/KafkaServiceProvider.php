<?php

namespace Kafka;

use Illuminate\Support\ServiceProvider;
use App\Connectors\KafkaConnector;

class KafkaServiceProvider extends ServiceProvider
{

    public function boot()
    {
        $manager = $this->app["queue"];

        $manager->addConnector('kafka', function () {
            return new KafkaConnector;
        });
    }
}
