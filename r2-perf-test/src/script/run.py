from __future__ import print_function

import argparse
from collections import namedtuple
import json
import os
from subprocess import Popen, call
import subprocess
import shlex

Test = namedtuple("Test", ["name", "client_properties", "server_properties"])
TestGroup = namedtuple("TestGroup", ["name", "tests"])

def read_runbook(runbooks):
	test_groups = []
	for runbook in runbooks:
		with open(runbook) as json_file:
		 	script = json.load(json_file)
		 	test_group_name = script['testGroup']
		 	tests = script['tests']
		 	test_list = []
		 	for test in tests:
		 		name = test['name']
		 		client_properties = test.get('clientProperties', '')
		 		server_properties = test.get('serverProperties', '')
		 		test_instance = Test(name, client_properties, server_properties)
		 		test_list.append(test_instance)
		 	test_group = TestGroup(test_group_name, test_list)
		 	test_groups.append(test_group)

	return test_groups

def run(directory, test_group):
	print(directory)
	print(test_group)
	file_path = os.path.join(directory, test_group.name)
	with open(file_path, "w") as out:
		pass

def run_gradle(gradle, gradle_cmd, properties, cwd):
	raw_cmd = "{0} {1} {2}".format(gradle, gradle_cmd, properties)
	args = shlex.split(raw_cmd)
	run_server = Popen(args, cwd=cwd)
	return run_server

if __name__ == '__main__':
	parser = argparse.ArgumentParser("run tests according to runbook")
	parser.add_argument('runbooks', type=str, nargs='+', help='runbooks')
	parser.add_argument('--out', type=str, default='./out', help='output dir')
	parser.add_argument('--gradle', type=str, default='ligradle', help='gradle to run')
	parser.add_argument('--cwd', type=str, default='.', help='child process work directory')

	args = parser.parse_args()
	directory = args.dir
	if not os.path.exists(directory):
		os.makedirs(directory)

	test_groups = read_runbook(args.runbooks)
	for test_group in test_groups:
		run(directory, test_group)
	# p = run_gradle(args.gradle, 'runHttpServer', '', args.cwd)
	# p.wait()



