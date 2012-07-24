#!/usr/bin/env python

from neo4jrestclient.client import GraphDatabase
import requests
import argparse
from csv import DictReader
from lxml import etree
from ijson import items
import urllib
INDEX_NAME = 'resources'
xml_namespaces = {
                  "dc": "http://purl.org/dc/elements/1.1/",
                  "dct": "http://purl.org/dc/terms/",
                  "ieee": "http://www.ieee.org/xsd/LOMv1p0",
                  "nsdl_dc": "http://ns.nsdl.org/nsdl_dc_v1.02/",
                  "xsi": "http://www.w3.org/2001/XMLSchema-instance",
                  "oa": "http://www.openarchives.org/OAI/2.0/"
                 }


def save_resource_node(envelope, db, idx):
    try:
        found_items = idx.query('resource:' + urllib.quote_plus(envelope['resource_locator']))
    except Exception, ex:
        print ex
        found_items = []
    if  len(found_items) > 0:
        new_node = found_items[0]
    else:
        new_node = db.nodes.create(resource=envelope['resource_locator'])
        idx['resource'][urllib.quote_plus(envelope['resource_locator'])] = new_node
    return new_node


def get_conforms_to_data(envelope):
    xml = etree.fromstring(envelope['resource_data'])
    return xml.xpath("./dct:conformsTo", namespaces=xml_namespaces)


def process_conforms_to_data(conforms_to, db, idx, new_node):
    for i in conforms_to:
        try:
            found_standard = idx.query('standard:' + urllib.quote_plus(i.text))
        except:
            found_standard = []
        if  len(found_standard) > 0:
            cc_node = found_standard[0]
        else:
            cc_node = db.nodes.create(purl=i.text)
            idx['standard'][urllib.quote_plus(i.text)] = cc_node
        new_node.conformsTo(cc_node)


def save_data(data_set, db, idx):
    for envelope in data_set:
        new_node = save_resource_node(envelope, db, idx)
        process_conforms_to_data(get_conforms_to_data(envelope), db, idx, new_node)


def parse_standards_data(data, prefix, valid_ids):
    for item in data:
        if 'leaf' in item and item['leaf'] and prefix + '.' + item['asn_statementNotation'] in valid_ids and 'asn_identifier' in item:
            return (item['asn_identifier'], prefix + '.' + item['asn_statementNotation'])
        if 'children' in item:
            return parse_standards_data(item['children'], prefix, valid_ids)


def import_cc_state(url, prefix, valid_ids):
    cc_info = []
    data = requests.get(url).json
    for item in data:
        cc_id = parse_standards_data(item['children'], prefix, valid_ids)
        if cc_id is not None:
            cc_info.append(cc_id)
    return cc_info


def process_cc_standards(db, idx):
    ids = set()
    with open('E0330_ccss_identifiers.csv', 'rU') as f:
        dr = DictReader(f)
        for row in dr:
            dot_node = db.nodes.create(standard=row['Dot notation'])
            url_node = db.nodes.create(standard=row['Current URL'])
            uuid_node = db.nodes.create(standard=row['GUID'])
            idx['standard'][urllib.quote_plus(row['Dot notation'])] = dot_node
            url_node.sameAs(dot_node)
            uuid_node.sameAs(dot_node)
            ids.add(row['Dot notation'])
    return ids


def process_prul_data(db, idx, urls, ids):
    for url in urls:
        results = import_cc_state(url[1], url[0], ids)
        for result in results:
            try:
                asn_node = idx.query('standard:' + urllib.quote_plus(result[0]))[0]
                cc_node = idx.query('standard:' + urllib.quote_plus(result[1]))[0]
                asn_node.sameAs(cc_node)
            except Exception, ex:
                print ex


def init_neo4j(url):
    db = GraphDatabase(url)
    if INDEX_NAME in db.nodes.indexes:
        idx = db.nodes.indexes.get(INDEX_NAME)
    else:
        idx = db.nodes.indexes.create(INDEX_NAME, type="fulltext")
    return db, idx


def process_data_service(url, db, idx):
    results = requests.get(url)
    results = items(results.raw, 'documents.item')
    for result_item in results:
        save_data(result_item['resource_data'], db, idx)


def main(args):
    urls = [('Literacy', 'http://asn.jesandco.org/resources/D10003FC_manifest.json'),
            ("Math", "http://asn.jesandco.org/resources/D10003FB_manifest.json")]
    db, idx = init_neo4j(args.db)
    process_data_service(args.url, db, idx)
    ids = process_cc_standards(db, idx, urls)
    process_prul_data(ids)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import LR data into Neo4j")
    parser.add_argument("--url", dest="url", default='https://node01.public.learningregistry.net/extract/standards-alignment-dct-conformsTo/resource-by-ts', help="URL to the data service to harvest from")
    parser.add_argument("--db", dest="db", default="http://graph.learningregistry.org/db/data/", help="URL to neo4j database")
    args = parser.parse_args()
    main(args)
