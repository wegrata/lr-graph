from neo4jrestclient import client, constants
import argparse
import urllib
import json

gdb = client.GraphDatabase("http://localhost:7474/db/data/")

def cypherQuery(idx, param):
	if idx == 'resources':
		qry = """START n=node:resources("resource:%s") MATCH other-[:submitted]->n RETURN other.submitter""" % urllib.quote_plus(param)
	elif idx == 'submitters':
		qry = """START n=node:submitters("submitter:%s") MATCH n-[:submitted]->other RETURN other.resource""" % urllib.quote_plus(param)

	cypher = gdb.extensions.CypherPlugin.execute_query
	nodes = cypher(query=qry, returns=constants.RAW)
	nodeList = nodes['data']
	
	return nodeList

def displayResults(resultList):
	for result in resultList:
		print urllib.unquote_plus(result[0])

def main(args):
	if args.res:
		nodeList = cypherQuery('resources', args.res)
		displayResults(nodeList)

	elif args.sub:		
		nodeList = cypherQuery('submitters', args.sub)
		displayResults(nodeList)

	elif args.sim1 or args.sim2:
		firstSub = args.sim1
		secondSub = args.sim2

		firstNodeList = cypherQuery('submitters', firstSub)
		secondNodeList = cypherQuery('submitters', secondSub)

		sameResources = [x for x in firstNodeList if x in secondNodeList]
		
		if sameResources:
			firstResourcesOnly = [x for x in firstNodeList if not x in secondNodeList]
			secondResourcesOnly = [x for x in secondNodeList if not x in firstNodeList]

			print 'Submitter %s also submitted:' % firstSub
			displayResults(firstResourcesOnly)
			
			print 'Submitter %s also submitted:' % secondSub
			displayResults(secondResourcesOnly)

		else:
			print 'No shared submitted resources between %s and %s' % (firstSub, secondSub)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import LR data into Neo4j")
    parser.add_argument("--res", dest="res", help="Name of resource to find submitters for")
    parser.add_argument("--sub", dest="sub", help="Name of submitter to find resources for, be sure to put in quotes")
    parser.add_argument("--sim1", dest="sim1", help="Name of first submitter to find alike resources, must include --sim2 as well. Be sure to put both submitters in quotes separated by a space.")
    parser.add_argument("--sim2", dest="sim2", help="Name of second submitter to find alike resources, must include --sim1 as well. Be sure to put both submitters in quotes separated by a space.")
    args = parser.parse_args()
    main(args)