#!/usr/bin/python

# Test Aerospike application using python

import argparse


parser = argparse.ArgumentParser(description='CDT challenge')
parser.add_argument('cmd', choices=['init', 'modify', 'expire'])
args = parser.parse_args()


def init():
	print 'init!'

def modify():
	print 'modify!'

def expire():
	print 'expire!'

def main():
	if args.cmd == 'init':
		init()
	elif args.cmd == 'modify':
		modify()
	elif args.cmd == 'expire':
		expire()

if __name__ == '__main__':
	main()

