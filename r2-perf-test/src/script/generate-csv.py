from __future__ import print_function

import argparse
from collections import namedtuple
import glob
import os
import re

Result = namedtuple('Result', ['group', 'name', 'mean', 'qps', 'ninty'])

def parse(test_output_dir):
	group_name = re.sub('-result', '', os.path.basename(os.path.normpath(test_output_dir)))
	results = {}
	for output_file in glob.glob(os.path.join(test_output_dir, '*.output')):
		output_file_name = os.path.splitext(os.path.basename(output_file))[0]
		test_name = re.sub(r"-\d-result", '', output_file_name)
		with open(output_file, 'r') as src:
			for raw_line in src:
				line = raw_line.strip()
				if line.find('Mean') > -1:
					mean = get_float(line)
				elif line.find('90%') > -1:
					ninty = get_float(line)
				elif line.find('Reqs') > -1:
					qps = get_float(line)

			result = Result(group_name, test_name, mean, qps, ninty)

			if results.get(result.name):
				prev_result = results.get(result.name)
				results[result.name] = Result(group_name, test_name, (mean + prev_result.mean)/2, (qps + prev_result.qps)/2, (ninty + prev_result.ninty)/2 )
			else:
				results[result.name] = result

	return group_name, results

def get_float(line):
	str_value = line.split(':')[1]
	return float(str_value.strip())

def generate(baseline_group_name, baseline, results, out_dir):
	if not os.path.exists(out_dir):
		os.makedirs(out_dir)
	for field in ['mean', 'qps', 'ninty']:
		file_name = os.path.join(out_dir, field)
		with open(file_name, 'w') as out:
			headers = []
			headers.append('Test Name')
			headers.append(baseline_group_name)
			for (name, result) in results:
				headers.append(name)

			print(','.join(headers), file=out)

			for base_name, base_result in baseline.iteritems():
				line = []
				line.append(base_name)
				base_value = getattr(base_result, field)
				if field == 'qps':
					line.append(str(int(base_value)))
				else:
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

	results = []
	for test in args.tests:
		test_group_name, test_results = parse(test)
		results.append((test_group_name,test_results))

	generate(baseline_group_name, baseline_result, results, args.out)

