import os
from furl import furl
from elasticsearch import Elasticsearch

import logging
log = logging.getLogger(__name__)


def config_elasticsearch(url):
    """Parse url and return Elasticsearch client."""
    f = furl(url)
    prefix = '/'.join(f.path.segments)
    host = dict(host=f.host, port=f.port, url_prefix=prefix, use_ssl=False)
    log.debug('ES parse host=%s, prefix=%s', f.host, f.path)
    return Elasticsearch([host])


def add_elasticsearch_args(parser):
    """Add standard Elasticsearch args to argparse."""

    parser.add_argument('--es-url', metavar='URL',
        default=os.environ.get('ES_URL', 'http://localhost:9200'),
        help='url to Elasticsearch (default=$ES_URL | http://localhost:9200)')

    parser.add_argument('--es-index', metavar='INDEX',
        default=os.environ.get('ES_INDEX', 'datasets'),
        help='Elasticsearch index name ( default=$ES_INDEX | datasets)') 


