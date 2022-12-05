<?php

namespace Kafka;

use Illuminate\Queue\Queue;
use Illuminate\Contracts\Queue\Queue as QueueContract;

class KafkaQueue extends Queue implements QueueContract
{
    protected $consumer, $producer;

    public function __construct($producer, $consumer)
    {
        $this->consumer = $consumer;
        $this->producer = $producer;
    }
    /**
     * Get the size of the queue.
     *
     * @param null|string $queue
     * @return int
     */
    public function size($queue = null)
    {
    }

    /**
     * Push a new job onto the queue.
     *
     * @param object|string $job
     * @param mixed $data
     * @param null|string $queue
     * @return mixed
     */
    public function push($job, $data = "", $queue = null)
    {
        $topic = $this->producer->newTopic($queue ?? env('KAFKA_QUEUE'));
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, serialize($job));
        $this->producer->flush(1000);
    }

    /**
     * Push a raw payload onto the queue.
     *
     * @param string $payload
     * @param null|string $queue
     * @param array $options
     * @return mixed
     */
    public function pushRaw($payload, $queue = null, array $options = array())
    {
    }

    /**
     * Push a new job onto the queue after (n) seconds.
     *
     * @param \DateInterval|\DateTimeInterface|int $delay
     * @param object|string $job
     * @param mixed $data
     * @param null|string $queue
     * @return mixed
     */
    public function later($delay, $job, $data = "", $queue = null)
    {
    }

    /**
     * Pop the next job off of the queue.
     *
     * @param null|string $queue
     * @return \Illuminate\Contracts\Queue\Job|null
     */
    public function pop($queue = null)
    {
        $this->consumer->subscribe([$queue]);
        $message = $this->consumer->consume(120 * 1000);
        try {
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $job = unserialize($message->payload);
                    $job->handle();
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    var_dump("'[' . date('H:i:s') . \"][partition {$message->partition}] No more messages; will wait for more [key: '{$message->key}' offset: {$message->offset}]\n");
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    var_dump('[' . date('H:i:s') . "] Timed out \n");
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
                    break;
            }
        } catch(\Exception $e) {
            var_dump($e->getMessage());
        }
    }
}
