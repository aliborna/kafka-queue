<?php

namespace Kafka;

use Illuminate\Queue\Connectors\ConnectorInterface;

class KafkaConnector implements ConnectorInterface
{

        /**
         * Establish a queue connection.
         *
         * @param array $config
         * @return \Illuminate\Contracts\Queue\Queue
         */
        public function connect(array $config)
        {
                $conf = new \RdKafka\Conf();

                $conf->set('bootstrap.servers', $config["bootstrap_servers"]);
                $producer = new \RdKafka\Producer($conf);

                $conf->set('group.id', $config["group_id"]);
                $conf->set('auto.offset.reset', $config["auto_offset_reset"]);
                $conf->set('enable.auto.commit', $config["enable_auto_commit"]);
                $conf->set('auto.commit.interval.ms', $config["auto_commit_interval_ms"]);

                $consumer = new \RdKafka\KafkaConsumer($conf);
                return new KafkaQueue($producer, $consumer);
        }
}
