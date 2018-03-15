"""
Indexes a dataset in Elasticsearch.

The dataset consists of a dataset.json file and optional supporting files
stored in the same directory. If a supporting file is found, it overrides
that section of the dataset.json. 

Where possible dsloader attempts to enhance and complete information
available in dataset.json by adding dataset ids, etc.
"""

import os
import sys
import json
import requests
from argparse import ArgumentParser
from geojson import Polygon

from es_wrap import *
from noaa import *

import logging
log = logging.getLogger('dsloader')


def new_overlay(**kwargs):

    new = dict(
        name=unicode('', 'utf-8'),
        description=unicode('', 'utf-8'),
        url=unicode('', 'utf-8'),
        type=unicode('wms', 'utf-8'),
        styles=[unicode('default', 'utf-8')],
        min=0,
        max=0,
    )

    new.update(kwargs)
    return new


def new_model(**kwargs):
    new = dict(
        name=unicode('', 'utf-8'),
        description=unicode('', 'utf-8'),
        url=unicode('', 'utf-8'),
        type=unicode('', 'utf-8'),
    )

    new.update(kwargs)
    return new


def new_download(**kwargs):
    new = dict(
        name=unicode('', 'utf-8'),
        description=unicode('', 'utf-8'),
        formats=unicode('', 'utf-8'),
        url=unicode('', 'utf-8'),
        size=0,
    )

    new.update(kwargs)
    return new


def new_analytics(**kwargs):
    new = dict(
        name=unicode('', 'utf-8'),
        description=unicode('', 'utf-8'),
        url=unicode('', 'utf-8'),
    )

    new.update(kwargs)
    return new


def new_metadata(markdown='', link='', description='', url=''):
    new = dict(
        markdown=unicode('', 'utf-8'),
        link=unicode('', 'utf-8'),
        description=unicode('', 'utf-8'),
        url=unicode('', 'utf-8'),
    )

    new.update(kwargs)
    return new


SERVICES = dict(overlays=new_overlay, model=new_model, 
                downloads=new_download, analytics=new_analytics)


def add_local_args(parser):

    parser.add_argument('src',
        help='name of the source dataset file (yaml or json)')
    parser.add_argument('--description-md', default='description.md', 
        metavar='FILE',
        help="the markdown file will update the dataset's title and "
             "description fields (default=description.md)")
    parser.add_argument('--info-md', default='info.md', metavar='FILE',
        help="the markdown file will update the dataset's information "
             "field (default=info.md)")
    parser.add_argument('--boundary', default='boundary.geojson', 
        metavar='FILE',
        help='the boundary geojson file (default=boundary.geojson)')
    parser.add_argument('--overlays', default='overlays.json', metavar='FILE',   
        help='overlay parameter file (default=overlays.json)')
    parser.add_argument('--overlay-md', default='overlay.md', metavar='FILE',
        help='markdown description of the overlay service '
             '(default=overlay.md)')
    parser.add_argument('--downloads', default='downloads.json', metavar='FILE',
        help='download parameter file (default=downloads.json)')
    parser.add_argument('--download-md', default='download.md', metavar='FILE',
        help='markdown description of the download service '
             '(default=download.md)')
    parser.add_argument('--analytics', default='analytics.json', 
        metavar='FILE',
        help='analytics parameter file (default=analytics.json)')
    parser.add_argument('--analytics-md', default='analytics.md', 
        metavar='FILE',
        help='markdown description of the analytic service '
             '(default=analytics.md)')
    parser.add_argument('--provenance-md', default='provenance.md', 
        metavar='FILE',
        help='markdown description of the analytic service '
             '(default=provenance.md)')
    parser.add_argument('--model', default='model.json', metavar='FILE',
        help='model parameter file (default=model.json)')
    parser.add_argument('--model-md', default='model.md', metavar='FILE',
        help='markdown description of the model service (default=model.md)')
    parser.add_argument('--noaa', default=False, action='store_true',
        help='the source dataset file is a NOAA metadata file')
    parser.add_argument('--debug',
        default = logging.INFO, action='store_const', const=logging.DEBUG,
        help='one or more urls pointing to a metadata download')


def update_description(doc, path, fname):

    filepath = os.path.join(path, fname)
    if not os.path.isfile(filepath):
        return

    with open(filepath) as f:
        md = f.readlines()

    for idx, line in enumerate(md):
        if line.startswith('# '):
            doc['title'] = unicode(line[2:].strip(), 'utf-8')
            del md[idx]
            break

    # skip empty lines after title
    for idx, line in enumerate(md):
        if not line.isspace():
            break
 
    doc['description'] = unicode(''.join(md[idx:]), 'utf-8')

def update_parameters(doc, service, path, fname):

    filepath = os.path.join(path, fname)
    if not os.path.isfile(filepath):
        return

    with open(filepath) as f:
         overlays = json.load(f)[service]
    
    doc[service] = [SERVICES[service](**o) for o in overlays]


def update_markdown(doc, service, path, fname):

    filepath = os.path.join(path, fname)
    if not os.path.isfile(filepath):
        return

    with open(filepath) as f:
        doc.setdefault(service, {})['markdown'] = unicode(f.read(), 'utf-8')


def generate_boundary(extents):

    left, bottom, right, top = extents
    return Polygon([[
        (left, bottom),
        (left, top),
        (right, top),
        (right, bottom),
        (left, bottom)
    ]])


def read_boundary(filepath)

    with open(filepath) as f:
        geojson = f.read()
    
    if geojson['type'] == 'FeatureCollection':
        return geojson['features'][0]['geometry']
    elif geojson['type'] == 'Feature':
        return geojson['geometry']
    else:
        return {}
    
    
def update_boundary(doc, path, fname):

    filepath = os.path.join(path, fname)
    if os.path.isfile(filepath):
        read_boundary(filepath)
    else:
        doc['region']['geometry'] = generate_boundary(doc['region']['extents'])


def append_variables(doc):
    """Append the list of variables to the dataset description."""
    
    variables = ', '.join([ v['name'] for v in doc['variables'] ])
    markdown = '\n'.join(['', '### Variables', variables ])
    doc['description'] = doc.get('description', unicode('', 'utf-8')) + markdown


#TODO
def validate_dataset(doc):
    """Check the document for errors and mistakes."""

    print json.dumps(doc)
    return True


def save_dataset_id(path, dataset_id):
    """Save the Elasticsearch _id in dataset directory."""

    filepath = os.path.join(path, 'ID')
    with open(filepath, 'w') as f:
        f.write(dataset_id)


def main():

    parser = ArgumentParser()
    add_local_args(parser)
    add_elasticsearch_args(parser)
    args = parser.parse_args()

    logging.basicConfig(level=args.debug)

    if args.noaa:
        doc = dict(type='dataset')
        noaa = NOAA(args.src)
        importNOAAMetadata(doc, noaa)

    else:
        with open(args.src) as f:
            doc = json.load(f)

    path, _ = os.path.split(args.src)

    update_description(doc, path, args.description_md)
    append_variables(doc)
    update_boundary(doc, path, args.boundary)
    update_markdown(doc, 'information', path, args.info_md)

    update_parameters(doc, 'overlays', path, args.overlays)
    update_markdown(doc, 'overlayService', path, args.overlay_md)

    update_parameters(doc, 'downloads', path, args.downloads)
    update_markdown(doc, 'downloadService', path, args.download_md)

    update_parameters(doc, 'analytics', path, args.analytics)
    update_markdown(doc, 'analyticService', path, args.analytics_md)

    update_parameters(doc, 'model', path, args.model)
    update_markdown(doc, 'modelService', path, args.model_md)

    update_markdown(doc, 'provenanceService', path, args.provenance_md)

    if args.force or validate_dataset(doc):
        es = config_elasticsearch(args.es_url)
        res = es.index(index=args.es_index, doc_type='dataset', body=doc)
        save_dataset_id(path, res['_id'])
    else:
        sys.exit(1) 


if __name__ == '__main__':
    main()
