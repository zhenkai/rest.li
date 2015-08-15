from __future__ import print_function

import argparse
from collections import namedtuple
import glob
import json
import logging
import os
import re
from subprocess import Popen, PIPE, check_call, call
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

# hack due to gradle lock issue
copy_dir = '../../../pegasus_trunk-copy/pegasus/r2-perf-test'

Test = namedtuple("Test", ["name", "client_properties", "server_properties"])
TestGroup = namedtuple("TestGroup", ["name", "client_branch", "server_branch", "tests"])

def get_current_branch():
	return Popen(['git', 'rev-parse', '--abbrev-ref', 'HEAD'], stdout=PIPE).communicate()[0].strip()

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
		 	current_branch = get_current_branch()
		 	client_branch = script.get('clientBranch', current_branch)
		 	server_branch = script.get('serverBranch', current_branch)
		 	tests = script['tests']
		 	test_list = []
		 	for test in tests:
		 		name = test['name']
		 		client_properties = test.get('clientProperties', '')
		 		server_properties = test.get('serverProperties', '')
		 		test_instance = Test(name, ' '.join((client_properties, common_client_properties)), ' '.join((server_properties, common_server_properties)))
		 		test_list.append(test_instance)
		 	test_group = TestGroup(test_group_name, client_branch, server_branch, test_list)
		 	test_groups.append(test_group)

	return test_groups

def run(directory, test_group, gradle, cwd, verbose, build_dir):
	file_path = os.path.join(directory, test_group.name)
	gc_out_dir = os.path.join(directory, "{0}-gc".format(test_group.name))
	result_out_dir = os.path.join(directory, "{0}-result".format(test_group.name))
	#if not os.path.exists(gc_out_dir):
	#	os.makedirs(gc_out_dir)
	if not os.path.exists(result_out_dir):
		os.makedirs(result_out_dir)

	current_branch = get_current_branch()
	current_wd = os.getcwd()

	hack_flag = False

	if test_group.client_branch == test_group.server_branch and test_group.client_branch == current_branch:
		pass
	elif test_group.client_branch == test_group.server_branch:
		check_call(['git', 'checkout', test_group.client_branch])
	else:
		hack_flag = True
		check_call(['git', 'checkout', test_group.server_branch], cwd=copy_dir)
		if test_group.client_branch != current_branch:
			check_call(['git', 'checkout', test_group.client_branch])

	for test in test_group.tests:
		for stage in ("1", "2"):
			test_name = "{0}-{1}".format(test.name, stage)
			logger.info("processing test: {0}".format(test_name))
			logger.info(test_name)
			logger.info("client properties: {0}".format(test.client_properties))
			logger.info("server properties: {0}".format(test.server_properties))
			logger.info("starting server...")

			try:
				if hack_flag:
					os.chdir(copy_dir)
					print("running server at dir {0}".format(os.getcwd()))

				server_process = run_gradle(gradle, 'runHttpServer', test.server_properties, cwd)

				i = 0
				while not poke(8082):
					i = i + 1
					assert i < 300, "Server didn't start within 5 minutes."
					sleep(1)

				logger.info("started server...")
			finally:
				if hack_flag:
					os.chdir(current_wd)
					print("set wd back to {0}".format(os.getcwd()))


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

			call('jps | grep "RunHttpServer\|RunHttpRestClient\|GradleDaemon\|GradleMain\|GradleWorkerMain" | cut -d " " -f 1 | xargs kill -9', shell=True)
			sleep(10)
			assert not poke(8082), "what, server still up?"

			logger.info("copying results...")
			for result_file in glob.glob(os.path.join(build_dir, 'r2-perf-test/*.output')):
				shutil.move(result_file, os.path.join(result_out_dir, "{0}-{1}".format(test_name, os.path.basename(result_file))))
			#logger.info("copying gc logs...")
			#for gc_file in glob.glob(os.path.join(build_dir, 'r2-perf-test/logs/gc/*.log')):
				#shutil.copy(gc_file, os.path.join(gc_out_dir, "{0}-{1}".format(test_name, os.path.basename(gc_file))))

	os.chdir(current_wd)
	check_call(['git', 'checkout', current_branch])
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
	origin_branch = get_current_branch()
	origin_wd = os.getcwd()
	os.setpgrp() # create new process group, become its leader
	try:
		for test_group in test_groups:
			logger.info("processing test group: {0}".format(test_group.name))
			run(directory, test_group, args.gradle, args.cwd, args.verbose, args.build_dir)
		stop = time()
		print("Took {0} seconds to finish tests".format(stop-start))
	except:
		e = sys.exc_info()[0]
		logger.exception(e)
		raise e
	finally:
		os.chdir(origin_wd)
		check_call(['git', 'checkout', origin_branch])
		os.killpg(0, SIGKILL) # kill all processes in this group




