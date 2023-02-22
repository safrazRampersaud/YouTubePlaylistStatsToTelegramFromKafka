#!/usr/bin/env python

import logging
import sys
import argparse
from consumer import process_consumer
from producer import process_producer

logger = logging.getLogger(__name__)


def main(args=None):
    print(args)
    if args.consumer:
        logging.info("Consumer started...")
        process_consumer()
    if args.producer:
        logging.info("Producer started...")
        process_producer()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--consumer", help="1 to execute consumer, 0 otherwise")
    parser.add_argument("--producer", help="1 to execute producer, 0 otherwise")
    argsv = parser.parse_args()

    logging.info("Starting main driver...")
    sys.exit(main(argsv))
