from __future__ import print_function

import argparse
from collections import namedtuple
import json
import logging
import os
import re
from subprocess import Popen, PIPE, call
import shlex
from signal import SIGKILL
import socket
import sys
from time import strftime, sleep

logger = logging.getLogger('r2-perf-test')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

error_or_warn = re.compile("WARN|ERROR")
done = re.compile("DONE")
empty = re.compile("^\s*$")
server_started = re.compile("=== Starting Http server ===")

Test = namedtuple("Test", ["name", "client_properties", "server_properties"])
TestGroup = namedtuple("TestGroup", ["name", "tests"])

def poke(port):
	s = socket.socket()
	try:
		s.connect(('localhost', port))
		return True
	except socket.error, e:
		return False

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

def run(directory, test_group, gradle, cwd):
	logger.info("processing test group: {0}".format(test_group.name))
	file_path = os.path.join(directory, test_group.name)
	with open(file_path, "w") as out:
		def write_to_file(msg):
			print(msg, file=out)

		for test in test_group.tests:
			logger.info("processing test: {0}".format(test.name))
			write_to_file(test.name)
			write_to_file("client properties: {0}".format(test.client_properties))
			write_to_file("server properties: {0}".format(test.server_properties))
			logger.info("starting server...")
			server_process = run_gradle(gradle, 'runHttpServer', test.server_properties, cwd)

			i = 0
			while not poke(8082):
				i = i + 1
				assert i < 120, "Server didn't start in 120 seconds"
				sleep(1)

			logger.info("starting client...")
			client_process = run_gradle(gradle, 'runHttpRestClient', test.client_properties, cwd)
			test_done = False
			for raw_line in client_process.stdout:
				line = raw_line.rstrip(os.linesep)
				if error_or_warn.search(line):
					logger.warn("client error or warn: " + line)
				elif done.search(line):
					test_done = True

				if test_done and not empty.match(line):
					write_to_file(line)

			client_process.wait()
			logger.info("stopped client...")
			server_process.kill()
			server_process.wait()
			logger.info("stopped server...")
			write_to_file("============================")

		logger.info("finished processing of test group: {0}".format(test_group.name))


def run_gradle(gradle, gradle_cmd, properties, cwd):
	raw_cmd = "{0} {1} {2}".format(gradle, gradle_cmd, properties)
	args = shlex.split(raw_cmd)
	return Popen(args, cwd=cwd, stdout=PIPE, stderr=PIPE)

if __name__ == '__main__':
	parser = argparse.ArgumentParser("run tests according to runbook")
	parser.add_argument('runbooks', type=str, nargs='+', help='runbooks')
	parser.add_argument('--out', type=str, default='./out', help='output dir')
	parser.add_argument('--gradle', type=str, default='ligradle', help='gradle to run')
	parser.add_argument('--cwd', type=str, default='.', help='child process work directory')

	args = parser.parse_args()
	directory = args.out
	if not os.path.exists(directory):
		os.makedirs(directory)

	logging_handler = logging.FileHandler(os.path.join(directory, 'run-{0}.log'.format(strftime("%Y-%m-%d_%H_%M_%S"))))
	logging_handler.setFormatter(formatter)
	logger.addHandler(logging_handler)
	logger.setLevel(logging.INFO)

	test_groups = read_runbook(args.runbooks)
	os.setpgrp() # create new process group, become its leader
	try:
		for test_group in test_groups:
			run(directory, test_group, args.gradle, args.cwd)
	finally:
		os.killpg(0, SIGKILL) # kill all processes in this group




