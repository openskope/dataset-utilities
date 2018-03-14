""" SKOPE index builder

Provides a CLT to manage the ElasticSearch index.
"""
import os
import sys
import argparse

import logging
log = logging.getLogger('dsindex')

from es_wrap import *


def add_local_arguments(parser):
    parser.add_argument('mapping', metavar='FILE',
        help='the name of mapping file')
    parser.add_argument('--debug', default=logging.WARN, 
        action='store_const', const=logging.DEBUG,
        help='enable debugging information')

def main():

    parser = argparse.ArgumentParser(description=__doc__)
    add_local_arguments(parser)
    add_elasticsearch_args(parser)
    args = parser.parse_args()

    logging.basicConfig(level=args.debug)

    es = config_elasticsearch(args.es_url)
    es.indices.delete(index=args.es_index, ignore=[400, 404])

    try:
        with open(args.mapping) as mapping:
            log.debug('creating index %s', args.es_index)
            es.indices.create(index=args.es_index, body=mapping.read())
    except IOError as e:
        logging.error('Unble to open mapping file %s', args.mapping)
        sys.exit(1)
    

if __name__ == '__main__':
    main()
