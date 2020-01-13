<?php

namespace App\Controller;

use InvalidArgumentException;
use Psr\Log\LoggerInterface;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\TopicPartition;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Annotation\Route;

/**
 * Class MetricsController
 * @package App\Controller
 */
class MetricsController extends AbstractController
{
    /** @var KafkaConsumer */
    public $kafkaConsumer;
    /** @var LoggerInterface */
    private $logger;

    /**
     * MetricsController constructor.
     *
     * @param LoggerInterface $logger
     */
    public function __construct(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }


    /**
     * @param array $options
     */
    private function initConsumer(array $options)
    {
        $conf = new Conf();

        // Callback Erreur
        $conf->setErrorCb(function ($kafka, $err, $reason) {
            $message = sprintf("ERREUR KAFKA : %s (%s)", rd_kafka_err2str($err), $reason);
            $this->logger->notice($message);
            throw new InvalidArgumentException($message, $err);
        });

        // Callback de rééquilibrage pour consigner les affectations de partition (optionnel)
        $conf->setRebalanceCb(
            function (KafkaConsumer $kafka, $err, array $partitions = null) {
                switch ($err) {
                    case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                        // Assignation des partitions
                        $this->logger->notice('(callback) partition(s) assignée(s) : ' .
                            implode(', ', array_map(
                                    function ($p) {
                                        return $p->getPartition();
                                    }, $partitions)
                            ));
                        $kafka->assign($partitions);
                        break;

                    case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                        // Révocation des partitions
                        $this->logger->notice('(callback) partition(s) révoquée(s) : ' .
                            implode(', ', array_map(
                                function ($p) {
                                    return $p->getPartition();
                                },
                                $partitions)));
                        $kafka->assign(null);
                        break;
                    default:
                        throw new InvalidArgumentException($err);
                }
            });

        $conf->set('enable.auto.commit', 'false'); // Il est capital que l'autocommit soit désactivé.
        $conf->set('enable.partition.eof', 'true');  // par défaut, mais on veut être sûr.
        $conf->set('log.connection.close', 'false');
        $conf->set('debug', 'all');
        $conf->set('offset.store.method', 'none');
        $conf->set('metadata.broker.list', implode(',', $options['brokers']));
        $conf->set('security.protocol', 'ssl');

        $properties = $options['properties'] ?? [];
        foreach ($properties as $key => $val) {
            $conf->set($key, $val);
        }

        $this->kafkaConsumer = new KafkaConsumer($conf);
    }

    /**
     * @Route("/metrics")
     *
     * @return Response
     * @throws \Exception
     */
    public function metrics()
    {
        $kafkaTopics  = explode(" ", $_ENV['KAFKA_TOPICS']);
        $kafkaBrokers = explode("\n", $_ENV['KAFKA_BROKERS']);

        $options = [
            'brokers'    => $kafkaBrokers,
            'properties' => [
                'ssl.ca.location'          => $_ENV['KAFKA_CRT'],
                'ssl.certificate.location' => $_ENV['KAFKA_PUB'],
                'ssl.key.location'         => $_ENV['KAFKA_KEY'],
                'ssl.key.password'         => $_ENV['KAFKA_PASSWORD'],
                'group.id'                 => $_ENV['KAFKA_GROUP'],
            ]
        ];

        $this->initConsumer($options);

        $this->kafkaConsumer->subscribe($kafkaTopics);
        $metadata = $this->kafkaConsumer->getMetadata(true, null, 60e3);
        $result   = [];
        foreach ($metadata->getTopics() as $topic) {
            $topicName = $topic->getTopic();
            if (in_array($topicName, $kafkaTopics)) {
                $partitions = $topic->getPartitions();
                foreach ($partitions as $partition) {
                    $low = 0;
                    $high = 0;
                    $this->kafkaConsumer->queryWatermarkOffsets($topicName, $partition->getId(), $low, $high, 60e3);
                    $result[$topicName][$partition->getId()] = [
                        'partition' => $partition->getId(),
                        'min'  => $low,
                        'max'  => $high,
                        'diff' => $high - $low,
                    ];
                    $topicPartition = new TopicPartition($topicName, $partition->getId());
                    $topicPartitionsWithOffset = $this->kafkaConsumer->getCommittedOffsets([$topicPartition], 60e3);
                    foreach ($topicPartitionsWithOffset as $topicPartitionWithOffset) {
                        $result[$topicName][$partition->getId()]['current'] = $topicPartitionWithOffset->getOffset();
                    }
                }
            }
        }

        return $this->json($result);
    }
}
