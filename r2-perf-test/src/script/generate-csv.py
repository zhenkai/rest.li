from __future__ import print_function

import argparse
from collections import namedtuple
import glob
import os
import re

Result = namedtuple('Result', ['group', 'name', 'median', 'qps', 'ninty', 'mean', 'nintyfive'])

def parse(test_output_dir):
	group_name = re.sub('-result', '', os.path.basename(os.path.normpath(test_output_dir)))
	results = {}
	for output_file in glob.glob(os.path.join(test_output_dir, '*.output')):
		output_file_name = os.path.splitext(os.path.basename(output_file))[0]
		test_name = re.sub(r"-\d-result", '', output_file_name)
		with open(output_file, 'r') as src:
			for raw_line in src:
				line = raw_line.strip()
				if line.find('50%') > -1:
					median = get_float(line)
				elif line.find('90%') > -1:
					ninty = get_float(line)
				elif line.find('Reqs') > -1:
					qps = get_float(line)
				elif line.find('Mean') > -1:
					mean = get_float(line)
				elif line.find('95%') > -1:
					nintyfive = get_float(line)

			result = Result(group_name, test_name, median, qps, ninty, mean, nintyfive)

			if not results.get(result.name):
				results[result.name] = []
			prev = results[result.name]
			prev.append(result)
			results[result.name] = prev

	final_results = {}

	for (name, result_list) in results.iteritems():
		name = name
		median = 0
		qps = 0
		ninty = 0
		nintyfive = 0
		mean = 0
		group_name = ""
		for result in result_list:
			median = median + result.median
			qps = qps + result.qps
			ninty = ninty + result.ninty
			mean = mean + result.mean
			nintyfive = nintyfive + result.nintyfive
			group_name = result.group

		count = len(result_list)

		final_result = Result(group_name, name, median/count, qps/count, ninty/count, mean/count, nintyfive/count)
		final_results[name] = final_result

	return group_name, final_results

def get_float(line):
	str_value = line.split(':')[1]
	return float(str_value.strip())

def generate(baseline_group_name, baseline, results, out_dir):
	if not os.path.exists(out_dir):
		os.makedirs(out_dir)

	for base_name, base_result in baseline.iteritems():
		file_name = os.path.join(out_dir, base_name)
		with open(file_name, 'w') as out:
			line = []
			line.append('Metrics')
			line.append(baseline_group_name)
			for (group_name, test_result_dict) in results:
				line.append(group_name)
			print(','.join(line), file=out)

			for field in ['mean', 'median', 'ninty', 'nintyfive', 'qps']:
				line = []
				if field == 'ninty':
					line.append('90% (ms)')
				elif field == 'nintyfive':
					line.append('95% (ms)')
				elif field == 'mean':
					line.append('Mean (ms)')
				elif field == 'median':
					line.append('Median (ms)')
				elif field == 'qps':
					line.append('Throughput (QPS)')
				base_value = getattr(base_result, field)
				line.append('{0:.3f}'.format(base_value))
				for (name, test_result_dict) in results:
					test_result = test_result_dict[base_name]
					test_value = getattr(test_result, field)
					delta = (test_value - base_value) / base_value * 100
					if delta > 0:
						format_str = '{0:.3f} (+{1:.1f}%)'
					else:
						format_str = '{0:.3f} ({1:.1f}%)'
					line.append(format_str.format(test_value, delta))

				print(','.join(line), file=out)

if __name__ == '__main__':
	parser = argparse.ArgumentParser("generate csv file for comparison")
	parser.add_argument('-b', '--baseline', type=str, required=True, help='baseline output dir')
	parser.add_argument('tests', type=str, nargs='+', help='test output dirs')
	parser.add_argument('--out', type=str, default='./out', help='output dir')

	args = parser.parse_args()

	baseline_group_name, baseline_result = parse(args.baseline)
        print(baseline_result)

	results = []
	for test in args.tests:
		test_group_name, test_results = parse(test)
                print(test_results)
		results.append((test_group_name,test_results))

	generate(baseline_group_name, baseline_result, results, args.out)

