#!/usr/bin/python

# Test Aerospike application using python

import argparse
import collections
from datetime import datetime, timedelta
import random

import aerospike
from aerospike import predexp as pe
from aerospike import predicates as p
from aerospike_helpers import cdt_ctx
from aerospike_helpers.operations import map_operations


parser = argparse.ArgumentParser(description='CDT challenge')
parser.add_argument('cmd', choices=['drop', 'clear', 'load', 'modify', 'expire', 'info'])
parser.add_argument('--host', dest='host', default='127.0.0.1')
args = parser.parse_args()


Customer = collections.namedtuple('Customer', ['name', 'seed'])
customers = [
	Customer('customer1', 123),
	Customer('customer2', 123),
	Customer('customer3', 234),
	Customer('customer4', 124),
]


def getkey(name):
	ns = 'ns1'
	st = 'set1'
	return (ns, st, name)


def cmd_drop(client):
	print 'Drop records'
	for c in customers:
		try:
			client.remove(getkey(c.name))
		except aerospike.exception.RecordNotFound as e:
			# Ignore if record isn't there
			pass


def clear_customer(client, name):
	ops = [
		map_operations.map_clear('tx')
	]
	client.operate(getkey(name), ops)


def cmd_clear(client):
	print 'Clear record data'
	for c in customers:
		try:
			clear_customer(client, c.name)
		except aerospike.exception.RecordNotFound as e:
			# Ignore if record isn't there
			pass


def add_customer(client, name, seed):
	iterations = 1000
	ops = []

	random.seed(seed)
	for i in range(iterations):
		tid = 100000 - i
		timestr = str(datetime.now() - timedelta(i))
		value = random.randint(1, 100)
		# This should be equivalent to the map operations following...
		#client.map_put(getkey(name), 'tx', tid, {'t':timestr, 'v':value}, meta={'ttl': aerospike.TTL_NEVER_EXPIRE} )
		ops.append( map_operations.map_put('tx', tid, {'t':timestr, 'v':value} ) )

	result = client.operate(getkey(name), ops, meta={'ttl': aerospike.TTL_NEVER_EXPIRE} )
	print(result)


def cmd_load(client):
	print 'Load records'
	for c in customers:
		add_customer(client, c.name, c.seed)


def cmd_modify(client):
	print 'modify!'


def cmd_expire(client):
	print 'expire!'


def cmd_info(client):
	print 'Show info'
	for c in customers:
		size = client.map_size(getkey(c.name), 'tx')
		print('---- Customer:%s  transactions:%d ----' % (c.name, size))

		# Naive iteration through map
		total = 0
		records = client.map_get_by_index_range(getkey(c.name), 'tx', 0, size, aerospike.MAP_RETURN_KEY_VALUE)
		for (_, record) in records:
			total = total + record['v']
		size2 = len(records)
		average = 0
		if size2 > 0:
			average = float(total) / float(size2)
		print('   Naive iteration: %d records average %f' % (size2, average))

		# Use predicates to get average
		ctx = [ cdt_ctx.cdt_ctx_map_key('v') ]	# We only want to consider the contents of map value 'v'
		query = client.query('ns1', 'set1')
		query.select('tx')


def main():
	config = {
		'hosts': [
			( args.host, 3000 ),
		],
		'policies': {
			'timeout': 1000 # milliseconds
		}
	}

	client = aerospike.client(config).connect()

	if args.cmd == 'drop':
		cmd_drop(client)
	elif args.cmd == 'clear':
		cmd_clear(client)
	elif args.cmd == 'load':
		cmd_load(client)
	elif args.cmd == 'modify':
		cmd_modify(client)
	elif args.cmd == 'expire':
		cmd_expire(client)
	elif args.cmd == 'info':
		cmd_info(client)

	client.close()


if __name__ == '__main__':
	main()
