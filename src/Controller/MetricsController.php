<?php

namespace App\Controller;

use InvalidArgumentException;
use Psr\Log\LoggerInterface;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
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
    public $consumer;
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

        $this->consumer = new KafkaConsumer($conf);
    }

    /**
     * @Route("/metrics")
     *
     * @return Response
     * @throws \Exception
     */
    public function metrics()
    {
        $kafkaTopics = explode(" ", $_ENV['KAFKA_TOPICS']);
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

        $this->consumer->subscribe($kafkaTopics);
        $topicPartitions = $this->consumer->getAssignment();
        //dump($topicPartitions);

        $offsetPositions = $this->consumer->getOffsetPositions($topicPartitions);
        //dump($offsetPositions);

//      foreach ($metadata->getTopics() as $topic) {
//          $topicName = $topic->getTopic();
//          if(in_array($topicName, $kafkaTopics)) {
//              $partitions = $topic->getPartitions();
//              foreach ($partitions as $partition) {
//                  $consumer->consumer->queryWatermarkOffsets($topicName, $partition->getId(), $low, $high, 60e3);
//              }
//          }
//      }

        $expected = [
            'my_first_topic'  => [
                0 => [
                    'min_offset'      => 8296,
                    'max_offset'      => 8299,
                    'diff_offset '    => 3,
                    'current_offset ' => 8298,
                ],
                1 => [
                    'min_offset'      => 8367,
                    'max_offset'      => 8371,
                    'diff_offset '    => 4,
                    'current_offset ' => 8369,
                ]
            ],
            'my_second_topic' => [
                0 => [
                    'min_offset'      => 8296,
                    'max_offset'      => 8299,
                    'diff_offset '    => 3,
                    'current_offset ' => 8298,
                ],
                1 => [
                    'min_offset'      => 8367,
                    'max_offset'      => 8371,
                    'diff_offset '    => 4,
                    'current_offset ' => 8369,
                ]
            ],
        ];

        return $this->json($expected);
    }
}
