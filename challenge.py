#!/usr/bin/python

DEBUG_FLAT = False


# Test Aerospike application using python

import argparse
import collections
from datetime import datetime, timedelta
import random

import aerospike
from aerospike import predexp as pe
from aerospike import predicates as p
from aerospike_helpers import cdt_ctx
from aerospike_helpers.operations import list_operations
from aerospike_helpers.operations import map_operations
from aerospike_helpers.operations import operations


parser = argparse.ArgumentParser(description='CDT challenge')
parser.add_argument('cmd', choices=['drop', 'clear', 'load', 'modify', 'expire', 'info'])
parser.add_argument('--host', dest='host', default='127.0.0.1')
args = parser.parse_args()



epoch = datetime.utcfromtimestamp(0)
tid_max = 100000

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
			print '  Removed %s' % (c.name)
		except aerospike.exception.RecordNotFound as e:
			# Ignore if record isn't there
			print '  Record %s not found (%s)' % (c.name, e)


def clear_customer(client, name):
	print '  Clear record %s' % name
	ops = [
		map_operations.map_clear('tx'),
	]
	client.operate(getkey(name), ops)


def cmd_clear(client):
	print 'Clear record data'
	for c in customers:
		try:
			clear_customer(client, c.name)
			print '  Cleared %s' % (c.name)
		except aerospike.exception.RecordNotFound as e:
			# Ignore if record isn't there
			print '  Record %s not found (%s)' % (c.name, e)


def add_customer(client, name, seed):
	iterations = 1000
	print '  Add %d transactions for record %s' % (iterations, name)
	ops = []

	random.seed(seed)
	for i in range(iterations):
		tid = tid_max - i
		timestamp = datetime.now() - timedelta(i)
		timestr = str(timestamp)
		timesec = (timestamp - epoch).total_seconds()
		value = random.randint(1, 100)

		# Without using client.operate, we could add each entry into the map separately.  But that's lame.
		#client.map_put(getkey(name), 'tx', tid, {'t':timestr, 'ts':timesec, 'v':value}, meta={'ttl': aerospike.TTL_NEVER_EXPIRE} )

		if DEBUG_FLAT:
			ops.append( map_operations.map_put('tx', tid, timesec ) )
		else:
			ops.append( map_operations.map_put('tx', tid, [ timesec, value, timestr ] ) )

	meta = {'ttl': aerospike.TTL_NEVER_EXPIRE}
	result = client.put(getkey(name), {"name":name}, meta=meta)
	result = client.put(getkey(name), {"tid_max":tid_max}, meta=meta)
	result = client.operate(getkey(name), ops, meta=meta)
	print '  %s' % str(result)


def cmd_load(client):
	print 'Load records'
	for c in customers:
		add_customer(client, c.name, c.seed)


def modify_customer(client, name, tid):
	print '  Modify record %s %s' % (name, tid)
	meta = {'ttl': aerospike.TTL_NEVER_EXPIRE}  # Otherwise default TTL applies!

	ctx = [
		cdt_ctx.cdt_ctx_map_key(tid),  # Using aerospike.CDTWildcard() will only match the first key, not all the keys.
	]

	ops = [
		# The value we want to increment is in a list, so we use a list_operation.  The top level being a map is dealt with using ctx.
		list_operations.list_increment('tx', 1, 100, ctx=ctx),
	]
	result = client.operate(getkey(name), ops, meta=meta)
	print '  %s' % str(result)


def cmd_modify(client):
	print 'Modify records'
	modify_customer(client, customers[0].name, tid_max)


def expire_customer(client, name):
	print '  Expire record %s' % name
	meta = {'ttl': aerospike.TTL_NEVER_EXPIRE}  # Otherwise default TTL applies!

	ops = [
		operations.increment('tid_max', 1),
		operations.read('tid_max'),
	]
	_, _, result = client.operate(getkey(name), ops, meta=meta)
	tid = result.get('tid_max', None)
	print '  New tid=%s' % tid

	# Only keep last 90 days of data
	now = (datetime.now() - epoch).total_seconds()
	timesec = (datetime.now() - timedelta(90) - epoch).total_seconds()
	ops = [
		map_operations.map_put('tx', tid, [ now, -1 ] ),
		map_operations.map_remove_by_value_range('tx', [0], [timesec], aerospike.MAP_RETURN_COUNT),
	]
	result = client.operate(getkey(name), ops, meta=meta)
	print '  %s' % str(result)


def cmd_expire(client):
	print 'Expire records'
	expire_customer(client, customers[-1].name)


def cmd_info(client):
	print 'Show info'
	for c in customers:
		try:
			size = client.map_size(getkey(c.name), 'tx')
		except aerospike.exception.RecordNotFound as e:
			print('  ---- Customer:%s not found (%s) ----' % (c.name, e))
			continue

		print('  ---- Customer:%s  transactions:%d ----' % (c.name, size))

		# Naive iteration through map
		total = 0
		count = 0
		records = client.map_get_by_index_range(getkey(c.name), 'tx', 0, size, aerospike.MAP_RETURN_KEY_VALUE)
		for (_, record) in records:
			if len(record) > 1:
				value = record[1]
				total = total + value
				count = count + 1
		average = float('nan')
		if count > 0:
			average = float(total) / float(count)
			print '  Key range: %d to %d' % (min(dict(records).keys()), max(dict(records).keys()))
		print '  Naive iteration: %d records average %f (%d items)' % (len(records), average, count)

		# Can we use predicates to get average???
		#ctx = [ cdt_ctx.cdt_ctx_list_index(1) ]	# We only want to consider the contents of map value 'v'
		#query = client.query('ns1', 'set1')
		#query.select('tx')


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
