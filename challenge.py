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
from aerospike_helpers.operations import map_operations


parser = argparse.ArgumentParser(description='CDT challenge')
parser.add_argument('cmd', choices=['drop', 'clear', 'load', 'modify', 'expire', 'info'])
parser.add_argument('--host', dest='host', default='127.0.0.1')
args = parser.parse_args()



epoch = datetime.utcfromtimestamp(0)

Customer = collections.namedtuple('Customer', ['name', 'seed'])
customers = [
	#Customer('customer1', 123),
	#Customer('customer2', 123),
	#Customer('customer3', 234),
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
	print '  Clear record %s', name
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
	print '  Add %d transactions for record %s' % (iterations, name)
	ops = []

	random.seed(seed)
	for i in range(iterations):
		tid = 100000 - i
		timestamp = datetime.now() - timedelta(i)
		timestr = str(timestamp)
		timesec = (timestamp - epoch).total_seconds()
		value = random.randint(1, 100)

		# Without using client.operate, we could add each entry into the map separately
		#client.map_put(getkey(name), 'tx', tid, {'t':timestr, 'ts':timesec, 'v':value}, meta={'ttl': aerospike.TTL_NEVER_EXPIRE} )

		if DEBUG_FLAT:
			ops.append( map_operations.map_put('tx', tid, timesec ) )
		else:
			ops.append( map_operations.map_put('tx', tid, {'t':timestr, 'ts':timesec, 'v':value} ) )

	meta = {'ttl': aerospike.TTL_NEVER_EXPIRE}
	result = client.operate(getkey(name), ops, meta=meta)
	print '  %s' % str(result)
	result = client.put(getkey(name), {"name":name}, meta=meta)


def cmd_load(client):
	print 'Load records'
	for c in customers:
		add_customer(client, c.name, c.seed)


def modify_customer(client, name):
	print '  Modify record %s' % name
	ctx = [ cdt_ctx.cdt_ctx_map_key('v') ]	# We only want to consider the contents of map value 'v'


def cmd_modify(client):
	print 'Modify records'
	modify_customer(client, customers[0].name)


def xxx_expire_customer(client, name):
	print '  Expire record %s' % name
	# Just keep last 90 days of data
	timesec = (datetime.now() - timedelta(90) - epoch).total_seconds()
	ctx = []
	if not DEBUG_FLAT:
		#ctx.append( cdt_ctx.cdt_ctx_map_key('ts') )	# We only want to consider the contents of map value 'ts'
		#ctx.append( cdt_ctx.cdt_ctx_map_key('100000') )	# We only want to consider the contents of map value 'ts'
		#ctx.append( cdt_ctx.cdt_ctx_map_index(1) )	# Removes the contents of second transaction id

		ctx.append( cdt_ctx.cdt_ctx_map_value(aerospike.CDTWildcard()) )	# Get map associated with transaction
		ctx.append( cdt_ctx.cdt_ctx_map_key('ts') )	# We only want to consider the contents of map value 'ts'
	ops = [
		map_operations.map_remove_by_value_range('tx', 0, timesec, aerospike.MAP_RETURN_COUNT, ctx=ctx)
	]
	result = client.operate(getkey(name), ops)
	print '  %s' % str(result)





#--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def expire_customer(client, name):
	print '  Test Expire record %s' % name
	# Just keep last 90 days of data
	timesec = (datetime.now() - timedelta(90) - epoch).total_seconds()
	ctx = []
	ctx.append( cdt_ctx.cdt_ctx_map_key(aerospike.CDTWildcard()) )	# Get map associated with transaction
	ctx.append( cdt_ctx.cdt_ctx_map_key('ts') )	# We only want to consider the contents of map value 'ts'

	ops = [
		map_operations.map_remove_by_value_range('tx', 0, timesec, aerospike.MAP_RETURN_INDEX, ctx=ctx)
	]
	result = client.operate(getkey(name), ops)
	print '  %s' % str(result)
#--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------





def cmd_expire(client):
	print 'Expire records'
	expire_customer(client, customers[-1].name)


def cmd_info(client):
	print 'Show info'
	for c in customers:
		size = client.map_size(getkey(c.name), 'tx')
		print('  ---- Customer:%s  transactions:%d ----' % (c.name, size))

		# Naive iteration through map
		total = 0
		count = 0
		records = client.map_get_by_index_range(getkey(c.name), 'tx', 0, size, aerospike.MAP_RETURN_KEY_VALUE)
		for (_, record) in records:
			value = record.get('v', None)
			if value is not None:
				total = total + value
				count = count + 1
		average = 0
		if count > 0:
			average = float(total) / float(count)
		print('  Naive iteration: %d records average %f (%d items)' % (len(records), average, count))

		# Can we use predicates to get average???
		#ctx = [ cdt_ctx.cdt_ctx_map_key('v') ]	# We only want to consider the contents of map value 'v'
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
