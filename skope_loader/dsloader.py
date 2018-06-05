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
from slugify import slugify, UniqueSlugify

from es_wrap import *
from noaa import *

import logging
log = logging.getLogger('dsloader')


def new_overlays(**kwargs):
    """Set default values with overrides for keyword args."""

    new = dict(
        title=unicode('', 'utf-8'),
        name=unicode('', 'utf-8'),
        shortname=unicode('', 'utf-8'),
        description=unicode('', 'utf-8'),

        url=unicode('', 'utf-8'),
        type=unicode('wms', 'utf-8'),
        styles=[unicode('default', 'utf-8')],
        min=0,
        max=0,
    )

    new.update(kwargs)
    return new


def new_analytics(**kwargs):
    """Set default values with overrides for keyword args."""

    new = dict(
        title=unicode('', 'utf-8'),
        name=unicode('', 'utf-8'),
        shortname=unicode('', 'utf-8'),
        description=unicode('', 'utf-8'),

        url=unicode('', 'utf-8'),
    )

    new.update(kwargs)
    return new


def new_downloads(**kwargs):
    """Set default values with overrides for keyword args."""

    new = dict(
        title=unicode('', 'utf-8'),
        name=unicode('', 'utf-8'),
        shortname=unicode('', 'utf-8'),
        description=unicode('', 'utf-8'),

        url=unicode('', 'utf-8'),
        formats=unicode('', 'utf-8'),
        size=0,
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


def new_metadata(markdown='', link='', description='', url=''):
    new = dict(
        markdown=unicode('', 'utf-8'),
        link=unicode('', 'utf-8'),
        description=unicode('', 'utf-8'),
        url=unicode('', 'utf-8'),
    )

    new.update(kwargs)
    return new


SERVICES = dict(overlays=new_overlays, model=new_model, 
                downloads=new_downloads, analytics=new_analytics)


def add_local_args(parser):

    parser.add_argument('src',
        help='name of the source dataset file (yaml or json)')
    parser.add_argument('--preserve', default=False, action='store_true',
            help='do no delete pre-existing ES dataset when re-indexing')
    parser.add_argument('--force', default=False, action='store_true',
        help='force dataset creation if final validation fails')
    parser.add_argument('--novars', default=False, action='store_true',
        help='append variable list to end of dataset description')
    parser.add_argument('--debug',
        default=logging.WARN, action='store_const', const=logging.DEBUG,
        help='enable debugging output')
    parser.add_argument('--verbose', '-v',
            default=False, action='store_const', const=True,
            help='print the document after successful load')

    # standard file names used in loading dataset
    parser.add_argument('--description-md', default='description.md', 
        metavar='FILE',
        help="the markdown file will update the dataset's title and "
             "description fields (default=description.md)")
    parser.add_argument('--info-md', default='information.md', metavar='FILE',
        help="the markdown file will update the dataset's information "
             "field (default=information.md)")
    parser.add_argument('--boundary', default='boundary.geojson', 
        metavar='FILE',
        help='the boundary geojson file (default=boundary.geojson)')
    parser.add_argument('--overlays', default='overlays.json', 
        metavar='FILE',   
        help='overlay parameter file (default=overlays.json)')
    parser.add_argument('--overlays-md', default='overlays.md', 
        metavar='FILE',
        help='markdown description of the overlay service '
             '(default=overlays.md)')
    parser.add_argument('--downloads', default='downloads.json', 
        metavar='FILE',
        help='download parameter file (default=downloads.json)')
    parser.add_argument('--downloads-md', default='downloads.md', 
        metavar='FILE',
        help='markdown description of the download service '
             '(default=downloads.md)')
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


def add_skope_args(parser):
    """SKOPE spectific variables used to complete strings with templates."""

    parser.add_argument('--skope-deploy-host', 
        default=os.environ.get('SKOPE_DEPLOY_HOST', 'http://localhost'),
        help='template variable automatically applied to json parameter files')
    parser.add_argument('--skope-yyyy', 
            default=os.environ.get('SKOPE_YYYY', '{YYYY}'),
            help='temporal variable used for years (completed by webapp)')
    parser.add_argument('--skope-mm', 
            default=os.environ.get('SKOPE_MM', '{MM}'),
            help='temporal variable used for months (completed by webapp)')
    parser.add_argument('--skope-yyyy-mm', 
            default=os.environ.get('SKOPE_YYYY_MM', '{YYYY-MM}'),
            help='temporal variable used for year-month (completed by webapp)')


def get_skope_args(args):
    """Extract SKOPE specific variables from argparse namespace."""

    d = {k.replace('skope_','').upper(): v for (k, v) in vars(args).items() \
            if k.startswith('skope_')}
    d.update(dict(start='{start}', end='{end}', 
            boundaryGeometry='{boundaryGeometry}'))
    log.debug('template variables = %s', str(d))
    return d


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


def update_parameters(doc, service, path, fname, varstrings):
    """Read service parameter file and integrate into document.

    Args:
      doc (dict): dataset document
      service (str): service being parsed
      path (str): base path for parameter file
      fname (str): filename of parameter file
      varstrings (dict): variable substitution strings applied to parameters
    """

    shortnames = { v['title']:v['shortname'] for v in doc['variables']}

    filepath = os.path.join(path, fname)
    if not os.path.isfile(filepath):
        return

    with open(filepath) as f:
          #s = f.read().format(**kwargs)
          #parameters = json.loads(s)[service]
          parameters = json.load(f)[service]

    for idx, p in enumerate(parameters):

        # TODO name is deprecated, remove at some point
        if not p.get('title', '') and p.get('name', ''):
            log.warn("use of variable attribute 'name' in %s is deprecated",
                     fname)
            p['title'] = p['name']

        if not p.get('title', ''):
            log.error('missing title in %s[%d]', service, idx)
            sys.exit(1)

        if p.get('title') not in shortnames.keys():
            log.error('service %s variable %s not found in dataset variables',
                       service, p.get('title'))
            sys.exit(1)

        p['shortname'] = shortnames[p.get('title')]

        if 'url' in p.keys():
            p['url'] = p['url'].format(**varstrings)

        if not p.get('description', ''):
            p['description'] = 'dataset {} variable {}'.format(doc['title'],
                    p['title'].encode('utf-8'))
        
    # update service specific values
    doc[service] = [SERVICES[service](**p) for p in parameters]


def update_markdown(doc, service, path, fname):

    log.debug('adding md file %s for service %s', fname, service)
    doc.setdefault(service, {})
    filepath = os.path.join(path, fname)

    if os.path.isfile(filepath):
        with open(filepath) as f:
            doc[service]['markdown'] = unicode(f.read(), 'utf-8')

    elif doc[service].get('markdown', ''):
        log.debug('%s - file %s not found.', filepath)
        doc.setdefault(service, {})['markdown'] = unicode('', 'utf-8')


def generate_boundary(extents):
    """Create boundary geometry based on extents."""

    left, bottom, right, top = extents
    return Polygon([[
        (left, bottom),
        (left, top),
        (right, top),
        (right, bottom),
        (left, bottom)
    ]])


def read_boundary(filepath):
    """Read geojson boundary and return geometry."""

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
        doc['region']['geometry'] = read_boundary(filepath)
    elif doc['region'].get('extents', ''):
        doc['region']['geometry'] = generate_boundary(doc['region']['extents'])
    else:
        log.warn('geometry not set - missing boundary file and extents')


# TODO 'name' is deprecated, remove in the future
def normalize_variables(doc):
    """Add unique shortname and handle deprecated 'name' attribute."""

    for v in doc['variables']:
        title = v.get('title', '')
        if not title: 
            title = v.get('name', '')
            if not title:
                log.error('dataset variables missing title attribute')
                sys.exit(1)
            log.warn("use of variable attribute 'name' is deprecated")

        v['title'] = title
        v['shortname'] = slugify(title, to_lower=True)
        #if not v.get('description', ''):
        #    v['description'] = '{} of dataset {}'.format(v['title'], 
        #                                                 doc['title'])


def get_variables(doc, title=False):
    """Return the list of variables from the document."""

    field = 'shortname' if title==false else 'title'
    return [v[field] for v in doc['variables']]


def append_variables(doc):
    """Append the list of variables to the dataset description."""
    
    variables = ', '.join([ '%s (%s)' % (v['title'], v['class']) \
            for v in doc['variables'] ])
    markdown = '\n**Variables:** ' + variables
    doc['description'] = doc.get('description', unicode('', 'utf-8')) \
            + markdown


#TODO
def validate_dataset(doc):
    """Check the document for errors and mistakes."""

    return True


def update_dataset_id(es, results, path, preserve=False):
    """Delete existing document and update document id."""

    filepath = os.path.join(path, 'ID')

    if not preserve and os.path.exists(filepath):
        with open(filepath) as f:
            _id = f.read().strip()
        es.delete(index=results['_index'], doc_type=results['_type'], id=_id)

    with open(filepath, 'w') as f:
        f.write(results['_id'])


def main():

    parser = ArgumentParser()
    add_local_args(parser)
    add_elasticsearch_args(parser)
    add_skope_args(parser)
    args = parser.parse_args()

    logging.basicConfig(level=args.debug)

    template_vars = get_skope_args(args)

    skopeid = UniqueSlugify(to_lower=True)

    if args.noaa:
        doc = dict(type='dataset')
        noaa = NOAA(args.src)
        importNOAAMetadata(doc, noaa)

    else:
        with open(args.src) as f:
            doc = json.load(f)

    # path is used to locating supporting metadata files
    path, fname = os.path.split(args.src)

    update_description(doc, path, args.description_md)
    doc['skopeid'] = skopeid(doc['title'])
    normalize_variables(doc)
    if not args.novars:
        append_variables(doc)

    update_boundary(doc, path, args.boundary)
    update_markdown(doc, 'information', path, args.info_md)

    update_parameters(doc, 'overlays', path, args.overlays, template_vars)
    update_markdown(doc, 'overlayService', path, args.overlays_md)

    update_parameters(doc, 'downloads', path, args.downloads, template_vars)
    update_markdown(doc, 'downloadService', path, args.downloads_md)

    update_parameters(doc, 'analytics', path, args.analytics, template_vars)
    update_markdown(doc, 'analyticService', path, args.analytics_md)

    update_parameters(doc, 'model', path, args.model, template_vars)
    update_markdown(doc, 'modelService', path, args.model_md)

    update_markdown(doc, 'provenanceService', path, args.provenance_md)

    if args.force or validate_dataset(doc):
        es = config_elasticsearch(args.es_url)

        res = es.index(index=args.es_index, doc_type='dataset', body=doc)
        if res['_shards']['successful'] > 0:
            update_dataset_id(es, res, path, preserve=args.preserve)
            if args.verbose:
                sys.stdout.write(json.dumps(doc)+'\n')

    else:
        sys.exit(1) 


if __name__ == '__main__':
    main()
