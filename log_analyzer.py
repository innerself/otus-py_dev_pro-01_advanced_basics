#!/usr/bin/env python
# -*- coding: utf-8 -*-


# log_format ui_short '$remote_addr  $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';
import datetime
import gzip
import json
import re
from collections import namedtuple
from pathlib import Path
from typing import Dict, Union
from string import Template

config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log",
    "NGINX_LOG_NAME": "nginx-access-ui",
}

BASE_DIR = Path(__file__).parent


def find_latest_log(log_dir, config):
    latest_log_date = datetime.date.min
    latest_log_path = None

    for file in log_dir.iterdir():
        file_name = file.name.lower()

        if not file_name.startswith(config['NGINX_LOG_NAME']):
            continue

        regex = fr'{config["NGINX_LOG_NAME"]}.log-(?P<log_date>\d{{8}}).*'
        pattern = re.compile(regex)
        result = pattern.match(file_name)

        log_date = datetime.datetime.strptime(
            result.group('log_date'),
            '%Y%m%d',
        ).date()

        if log_date > latest_log_date:
            latest_log_date = log_date
            latest_log_path = Path(__file__).parent.joinpath(*file.parts)

    LatestLog = namedtuple('LatestLog', ['date', 'path', 'file_type'])  # NOSONAR
    latest_log = LatestLog(
        date=latest_log_date,
        path=latest_log_path,
        file_type='gzip' if latest_log_path.suffix == '.gz' else 'plain'
    )

    return latest_log


def parse_file(latest_log) -> Dict[str, Dict[str, Union[int, float]]]:
    file_opener = open if latest_log.file_type == 'plain' else gzip.open
    regex = r'^.*"(GET|POST)\s(?P<request>.*?)\sHTTP.*(?P<request_time>\d+\.\d+).+$'
    pattern = re.compile(regex)
    stats = {'requests': {}}
    with file_opener(latest_log.path) as log_file:
        for line in log_file:
            match = pattern.match(line.decode('utf-8'))

            if not match:
                continue

            request = match.group('request')
            if request in stats['requests'].keys():
                stats['requests'][request]['request_count'] += 1
                stats['requests'][request]['request_durations'].append(
                    float(match.group('request_time')),
                )
            else:
                stats['requests'][request] = {
                    'request_count': 1,
                    'request_durations': [float(match.group('request_time'))],
                }

    stats['total_requests'] = sum(
        req['request_count']
        for req
        in stats['requests'].values()
    )

    stats['total_requests_duration'] = sum(
        sum(req['request_durations'])
        for req
        in stats['requests'].values()
    )

    return stats


def calc_durations_median(count, durations):
    if len(durations) == 1:
        return durations[0]

    sorted_time = sorted(durations)
    count_is_even = count % 2 == 0

    if count_is_even:
        median = (sorted_time[count // 2 - 1] + sorted_time[count // 2]) / 2
    else:
        median = sorted_time[count // 2]

    return median


def prepare_log_data(log_data):
    result = list()
    digits_round_to = 4

    for url, data in log_data['requests'].items():
        time_sum = sum(data['request_durations'])

        result.append({
            'url': url,
            'count': data['request_count'],
            'count_perc': round((data['request_count'] / log_data['total_requests']) * 100, digits_round_to),
            'time_sum': round(time_sum, digits_round_to),
            'time_perc': round(time_sum / log_data['total_requests_duration'] * 100, digits_round_to),
            'time_avg': round(time_sum / data['request_count'], digits_round_to),
            'time_max': max(data['request_durations']),
            'time_med': round(calc_durations_median(
                data['request_count'],
                data['request_durations'],
            ), digits_round_to),
        })

    return result


def main():
    report_dir = Path(config['REPORT_DIR'])
    log_dir = Path(config['LOG_DIR'])

    if not log_dir.exists():
        log_dir.mkdir()

        # TODO print info and exit

    latest_log = find_latest_log(log_dir, config)
    raw_log_data = parse_file(latest_log)
    prepared_log_data = prepare_log_data(raw_log_data)

    if not report_dir.exists():
        report_dir.mkdir()

    with open(BASE_DIR.joinpath('report.html'), 'r') as file:
        report_template = Template(file.read())

    report = report_template.safe_substitute(table_json=json.dumps(prepared_log_data))

    report_path = report_dir.joinpath(f'report-{latest_log.date:%Y.%m.%d}.html')
    with open(report_path, 'w') as file:
        file.write(report)

    print()


if __name__ == "__main__":
    main()
