import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

import pika
import argparse, json, timeit
import datetime

def parse_arguments():
    """
    This meturn handles the different arguments on the command line.
    """
    parser = argparse.ArgumentParser("Spark Job.")
    parser.add_argument("--address", type=str, help='Address on Socket Server')
    parser.add_argument("--port", type=int, default=9090, help='Port of  Socket Server')
    parser.add_argument("--id", type=str,
                        help="ID of the client.")
    return parser.parse_args()

args = parse_arguments()

cred = pika.PlainCredentials('rabbitmq', 'rabbitmq')
rabbitmq_parameters = pika.ConnectionParameters(args.address, '5672', '/', cred)

EXCHANGE = 'example'
OUTPUT_QUEUE = 'output_' + args.id
ROUTING_KEY = 'tenant_output_' + args.id

def publish(rdd):
    connection = pika.BlockingConnection(rabbitmq_parameters)
    channel = connection.channel()
    channel.queue_bind(OUTPUT_QUEUE, EXCHANGE, ROUTING_KEY)
    count = []
    counts = {"time":datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

    for record in rdd:
        count.append(record)
    counts["TopPULocations"] = count
    channel.basic_publish(exchange=EXCHANGE, routing_key=ROUTING_KEY, body=json.dumps(counts))
    connection.close()


if __name__ == "__main__":
    checkpointDir = "checkpoint"

    def functionToCreateContext():
        
        sc = SparkContext(appName="TenantStreamApp")
        ssc = StreamingContext(sc, 5)

        lines = ssc.socketTextStream(args.address, args.port)
        counts = lines.flatMap(lambda line: line.split(" "))\
                    .map(lambda word: (word, 1))\
                    .reduceByKeyAndWindow(lambda a, b: (a+b), 10, 5)

        counts.foreachRDD(lambda rdd: rdd.foreachPartition(publish))
        ssc.checkpoint(checkpointDir)
        return ssc
    ssc = StreamingContext.getOrCreate(checkpointDir, functionToCreateContext)

    ssc.start()
    ssc.awaitTermination()

