#!/usr/bin/env python

import logging
import sys
import argparse
from consumer import process_consumer
from producer import process_producer
from driver_config import driver_config

logger = logging.getLogger(__name__)


def main(args=None, config=None):
    print(args)
    if args.consumer:
        logging.info("Consumer started...")
        process_consumer(config)
    if args.producer:
        logging.info("Producer started...")
        process_producer(config)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--consumer", help="1 to execute consumer, 0 otherwise", type=int)
    parser.add_argument("--producer", help="1 to execute producer, 0 otherwise", type=int)
    argsv = parser.parse_args()

    logging.info("Starting main driver...")
    sys.exit(main(argsv, driver_config))
