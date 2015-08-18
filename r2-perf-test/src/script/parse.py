from __future__ import print_function

import argparse
import os
import re

TEST_GROUP_REGEX = re.compile(r"processing test group: (.*$)")
TEST_REGEX = re.compile(r"processing test: (.*$)")
DONE = re.compile(r"DONE")
END = re.compile(r"BUILD SUCCESSFUL")
LOG_PREFIX = re.compile(r"^\d+.* INFO\s*")

GROUP_DISCOVERED = 'group_discovered'
TEST_DISCOVERED = 'test_discovered'
DONE_DISCOVERED = 'done_discovered'


if __name__ == '__main__':
	parser = argparse.ArgumentParser("parse and generate test results")
	parser.add_argument('logfile', type=str, help='test log file')
	parser.add_argument('--out', type=str, default='./out', help='output dir')

	args = parser.parse_args()

	if not os.path.exists(args.out):
		os.mkdir(args.out)

	with open(args.logfile, 'r') as input:
		group_num = 0
		test_num = 0
		group_name = ""
		test_name = ""
		output = None
		state = GROUP_DISCOVERED
		for line in input:
			line = line.strip()
			if state == GROUP_DISCOVERED:
				gs = TEST_GROUP_REGEX.search(line)
				if gs:
					group_name = gs.group(1)
					group_num = group_num + 1
					if not os.path.exists(os.path.join(args.out, group_name)):
						os.mkdir(os.path.join(args.out, group_name))
					state = GROUP_DISCOVERED
					continue

				ts = TEST_REGEX.search(line)
				if ts:
					test_name = ts.group(1)
					test_num = test_num + 1
					state = TEST_DISCOVERED
			elif state == TEST_DISCOVERED:
				ds = DONE.search(line)
				if ds:
					output = open(os.path.join(args.out, os.path.join(group_name, "{0}-result.output".format(test_name))), 'w')
					print(LOG_PREFIX.sub('', line), file=output)
					state = DONE_DISCOVERED
			elif state == DONE_DISCOVERED:
				es = END.search(line)
				if not es:
					print(LOG_PREFIX.sub('', line), file=output)
				else:
					output.close()
					state = GROUP_DISCOVERED
			else:
				raise ValueError("Unknown state: {0}".format(state))

	print("done")




