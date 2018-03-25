""" SKOPE index builder

Provides a CLI to manage the ElasticSearch index.
"""
import os
import sys
import argparse
import logging

from es_wrap import *


def add_local_arguments(parser):
    parser.add_argument('mapping', metavar='FILE',
        help='the name of mapping file')
    parser.add_argument('--force', default=False, action='store_true',
        help='deletes the index if it exists')
    parser.add_argument('--reindex', default='', metavar='INDEX',
        help='reindex from existing index')
    parser.add_argument('--alias', metavar='ALIAS', default='datasets',
        help='applies alises to newly created index (default=datasets)')
    parser.add_argument('--debug', default=logging.WARN, 
        action='store_const', const=logging.DEBUG,
        help='enable debugging information')


def main():

    parser = argparse.ArgumentParser(description=__doc__)
    add_local_arguments(parser)
    add_elasticsearch_args(parser)
    args = parser.parse_args()

    log = logging.getLogger(os.path.basename(sys.argv[0]))
    logging.basicConfig(level=args.debug)

    es = config_elasticsearch(args.es_url)
    if args.force:
        es.indices.delete(index=args.es_index, ignore=[400, 404])

    try:
        with open(args.mapping) as mapping:
            log.debug('creating index %s', args.es_index)
            es.indices.create(index=args.es_index, body=mapping.read())

    except IOError as e:
        log.error('Unble to open mapping file %s', args.mapping)
        sys.exit(1)

    except RequestError as e:
        log.error('index %s already exists, use --force option')
        sys.exit(1)

    if args.reindex:
        try:
            body = dict(source={"index": args.reindex}, 
                        dest={"index": args.es_index})
            es.reindex(body=body)

        except NotFoundError as e:
            log.error('source index %s not found', args.reindex)
            sys.exit(1)

    if args.alias and args.es_index != args.alias:
        log.debug('applying alias %s to index', args.alias)
        es.indices.delete_alias(index=['_all'], name=args.alias, ignore=[404])
        es.indices.put_alias(index=args.es_index, name=args.alias)

