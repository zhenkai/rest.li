from __future__ import print_function

import argparse
from collections import namedtuple
import glob
import json
import logging
import os
import re
from subprocess import Popen, PIPE, check_call
import shlex
import shutil
from signal import SIGKILL
import socket
import sys
from time import strftime, sleep, time

logger = logging.getLogger('r2-perf-test')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

error_or_warn = re.compile("WARN|ERROR")
done = re.compile("DONE")
empty = re.compile("^\s*$")
server_started = re.compile("=== Starting Http server ===")

Test = namedtuple("Test", ["name", "client_properties", "server_properties"])
TestGroup = namedtuple("TestGroup", ["name", "branch", "tests"])

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
		 	common_client_properties = script.get('commonClientProperties', '')
		 	common_server_properties = script.get('commonServerProperties', '')
		 	test_branch = script.get('branch')
		 	tests = script['tests']
		 	test_list = []
		 	for test in tests:
		 		name = test['name']
		 		client_properties = test.get('clientProperties', '')
		 		server_properties = test.get('serverProperties', '')
		 		test_instance = Test(name, ' '.join((client_properties, common_client_properties)), ' '.join((server_properties, common_server_properties)))
		 		test_list.append(test_instance)
		 	test_group = TestGroup(test_group_name, test_branch, test_list)
		 	test_groups.append(test_group)

	return test_groups

def run(directory, test_group, gradle, cwd, verbose, build_dir):
	file_path = os.path.join(directory, test_group.name)
	gc_out_dir = os.path.join(directory, "{0}-gc".format(test_group.name))
	result_out_dir = os.path.join(directory, "{0}-result".format(test_group.name))
	if not os.path.exists(gc_out_dir):
		os.makedirs(gc_out_dir)
	if not os.path.exists(result_out_dir):
		os.makedirs(result_out_dir)


	for test in test_group.tests:
		for stage in ("1", "2"):
			test_name = "{0}-{1}".format(test.name, stage)
			logger.info("processing test: {0}".format(test_name))
			logger.info(test_name)
			logger.info("client properties: {0}".format(test.client_properties))
			logger.info("server properties: {0}".format(test.server_properties))
			logger.info("starting server...")
			server_process = run_gradle(gradle, 'runHttpServer', test.server_properties, cwd)

			i = 0
			while not poke(8082):
				i = i + 1
				assert i < 300, "Server didn't start within 5 minutes."
				sleep(1)

			logger.info("started server...")
			logger.info("starting client and running test...")
			client_process = run_gradle(gradle, 'runHttpRestClient', test.client_properties, cwd)
			if verbose:
				while client_process.poll() is None:
					output = client_process.stdout.readline()
					if output:
						logger.info(output.rstrip())
			else:
				client_process.wait()


			logger.info("stopped client...")
			server_process.kill()
			server_process.wait()
			logger.info("stopped server...")

			logger.info("copying results...")
			for result_file in glob.glob(os.path.join(build_dir, 'r2-perf-test/*.output')):
				shutil.move(result_file, os.path.join(result_out_dir, "{0}-{1}".format(test_name, os.path.basename(result_file))))
			logger.info("copying gc logs...")
			for gc_file in glob.glob(os.path.join(build_dir, 'r2-perf-test/logs/gc/*.log')):
				shutil.copy(gc_file, os.path.join(gc_out_dir, "{0}-{1}".format(test_name, os.path.basename(gc_file))))

	logger.info("finished processing of test group: {0}".format(test_group.name))


def run_gradle(gradle, gradle_cmd, properties, cwd):
	raw_cmd = "{0} {1} {2}".format(gradle, gradle_cmd, properties)
	args = shlex.split(raw_cmd)
	logger.info(args)
	return Popen(args, cwd=cwd, stdout=PIPE, stderr=PIPE)

if __name__ == '__main__':
	parser = argparse.ArgumentParser("run tests according to runbook")
	parser.add_argument('runbooks', type=str, nargs='+', help='runbooks')
	parser.add_argument('--out', type=str, default='./out', help='output dir')
	parser.add_argument('--gradle', type=str, default='ligradle', help='gradle to run')
	parser.add_argument('--cwd', type=str, default='.', help='child process work directory')
	parser.add_argument('--build-dir', dest='build_dir', type=str, default='../build', help='build directory')
	parser.add_argument('--verbose', dest='verbose', action='store_true')

	args = parser.parse_args()

	directory = args.out
	if not os.path.exists(directory):
		os.makedirs(directory)

	logging_handler = logging.FileHandler(os.path.join(directory, 'run-{0}.log'.format(strftime("%Y-%m-%d_%H_%M_%S"))))
	logging_handler.setFormatter(formatter)
	logger.addHandler(logging_handler)
	logger.setLevel(logging.INFO)

	test_groups = read_runbook(args.runbooks)
	start = time()
	os.setpgrp() # create new process group, become its leader
	try:
		for test_group in test_groups:
			logger.info("processing test group: {0}".format(test_group.name))
			if test_group.branch:
				check_call(['git', 'checkout', test_group.branch])
				logger.info("checked out branch {0}".format(test_group.branch))
			try:
				run(directory, test_group, args.gradle, args.cwd, args.verbose, args.build_dir)
			finally:
				if test_group.branch:
					check_call(['git', 'checkout', '@{-1}'])
					logger.info("resumed git repo to previous branch")
		stop = time()
		print("Took {0} seconds to finish tests".format(stop-start))
	except:
		e = sys.exc_info()[0]
		logger.exception(e)
		raise e
	finally:
		os.killpg(0, SIGKILL) # kill all processes in this group




