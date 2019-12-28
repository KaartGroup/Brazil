#!/usr/bin/env python

import glob
from shapely import geometry
from shapely.ops import polygonize, cascaded_union
import sys
import fiona
import subprocess
import argparse
import requests
import json
import codecs
import os
from tqdm import tqdm

parser = argparse.ArgumentParser(description='Create maproulette projects with IBGE data and unname_roads data.')
parser.add_argument('shp_dir', help='directory with all the shp files for a state')
parser.add_argument('output', help='name for output file')
parser.add_argument('relation_id', help='osm relation id of the state of the data')
parser.add_argument('--unnamed_roads', help='sum the integers (default: find the max)', default='/Volumes/Brazil_Mexico/Brazil/Unnamed_Roads/osm_unnamed_20190925_filtered.geojson')

args = parser.parse_args()

output_dir = os.path.abspath(os.path.join(args.shp_dir, os.pardir))

'''
Merge all *face.shp files together
We also filter out all the baddies
'''
files = glob.glob(f"{args.shp_dir}/*face.shp")
meta = fiona.open(files[0]).meta
no_names = ['sem denominacao', 's/d', 'sem d', 'sd', 'sem nome']

print('MERGING & FILTERING')
out = os.path.join(output_dir, args.output)
ibge = []
with fiona.open(f'{out}_merged.shp', 'w', **meta) as merged:
    for f in tqdm(files):
        for feature in fiona.open(f):
            if feature['properties']['NM_NOME_LO'] is None or feature['properties']['NM_NOME_LO'] in no_names:
                continue
            ibge.append(feature)
            merged.write(feature)

print('===DONE===\n------------')
ibge_shapes = [geometry.shape(s['geometry']) for s in ibge]
ibge_union = cascaded_union(ibge_shapes)
# Get ways of the state outline relation
overpass_url =f'https://lz4.overpass-api.de/api/interpreter?data=[out:xml][timeout:90];rel({args.relation_id});way(r);out geom;'
response = requests.get(overpass_url)
xml = response.text

# Create a state osm xml file 
with open(f'{out}-state.osm', 'w') as osm:
    osm.write(xml)

# Install osmtogeojson if not already
o2g = subprocess.call(['which', 'osmtogeojson'])
if o2g != 0:
    subprocess.run(['npm', 'install', '-g', 'osmtogeojson'])

# Convert osm xml to geojson
print('CONVERTING XML TO GEOJSON')
with open(f'{out}-state.geojson', 'w') as out_file:
    p = subprocess.run(['osmtogeojson', f'{out}-state.osm'], stdout=out_file)
print('===DONE===\n------------')

state_polygon = None
with fiona.open(f'{out}-state.geojson', 'r') as features:
    shapes = []
    for feature in features:
        shapes.append(geometry.shape(feature['geometry']))
    state_polygon = next(polygonize(shapes))

    schema = { 'geometry': 'Polygon', 'properties': { 'name': 'str' } }
    with fiona.open(f'{out}-test.geojson', 'w', driver='GeoJSON', schema=schema, crs = features.crs) as test:
        test.write({
            'properties':{'name':'test'},
            'geometry': mapping(next(state_polygon))
})

with fiona.open(f'{args.unnamed_roads}', 'r') as unnamed:
    unnamed_roads = [geometry.shape(r['geometry']) for r in unnamed if geometry.shape(r['geometry']).intersects(state_polygon)]
    schema = { 'geometry': 'MultiLineString', 'properties': { 'name': 'str' } }
    with fiona.open(f'{out}-int.geojson', 'w', driver='GeoJSON', schema=schema, crs = unnamed.crs) as test:
        for road in unnamed_roads:
            test.write({
                'properties':{'name':'test'},
                'geometry': geometry.mapping(road)
            })
    tasks = [r for r in unnamed_roads if r.intersects(ibge_union)]
    
