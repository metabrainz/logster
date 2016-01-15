from collections import defaultdict
import time
import re

from logster.logster_helper import MetricObject, LogsterParser
from logster.logster_helper import LogsterParsingException



#1452808791.454 75.128.36.54 "GET /ws/2/release-group/?artist=6e67dca0-80c0-4dbd-9063-faa176f99e1e&limit=100&type=album HTTP/1.0" 503 668 z=- up=unix:/tmp/musicbrainz-ws.socket ms=0.035 ums=0.035 ol=- h=musicbrainz.org
#
#    log_format proxylog
#        '$msec'
#        ' $http_x_mb_remote_addr'
#        ' "$request"'
#        ' $status'
#        ' $bytes_sent'
#        ' z=$gzip_ratio'
#        ' up=$upstream_addr'
#        ' ms=$request_time'
#        ' ums=$upstream_response_time'
#        ' ol=$sent_http_location'
#        ' h=$http_host'
#        ;

class NginxProxy(LogsterParser):
    def __init__(self, option_string=None):
        self.metrics = dict()
        self.reg = re.compile('.*HTTP/1.\d\" (?P<http_status_code>\d{3}) '
                              '(?P<response_size>\d+) '
                              'z=(?P<gzip_ratio>\S+) '
                              'up=(?P<upstream_addr>\S+) .*?'
                              'ms=(?P<request_time>\S+) '
                              'ums=(?P<upstream_response_time>\S+) .*?'
                              'ol=(?P<sent_http_location>\S+) '
                              'h=(?P<http_host>\S+).*')
        self.regstr = re.compile(r"[^a-z0-9_-]", re.IGNORECASE)
        self.regcleanup = re.compile(r"_+")

        self.metrics['upstreams'] = defaultdict(lambda : {
            'hits': 0,
            'ms': 0,
            'size': 0,
            'http_200': 0,
            'gzip_ratio': 0,
            'http_200_gzipped': 0,
            'http_200_ms': 0,
        })

    def to_key(self, string):
        return self.regcleanup.sub('_', self.regstr.sub('_', string))


    def parse_line(self, line):
        try:
            regMatch = self.reg.match(line)
            if regMatch:
                linebits = regMatch.groupdict()
                code = int(linebits['http_status_code'])
                size = int(linebits['response_size'])
                upstream = self.to_key(linebits['upstream_addr'])
                if upstream != '-':
                    self.metrics['upstreams'][upstream]['hits'] += 1
                    self.metrics['upstreams'][upstream]['size'] += size
                    self.metrics['upstreams'][upstream]['ms'] += float(linebits['upstream_response_time'])
                    if code == 200:
                        self.metrics['upstreams'][upstream]['http_200'] += 1
                        self.metrics['upstreams'][upstream]['http_200_ms'] += float(linebits['upstream_response_time'])
                        if linebits['gzip_ratio'] != '-':
                            self.metrics['upstreams'][upstream]['http_200_gzipped'] += 1
                            self.metrics['upstreams'][upstream]['gzip_ratio'] += float(linebits['gzip_ratio'])
            else:
                raise LogsterParsingException, "regmatch failed to match"

        except Exception, e:
            raise LogsterParsingException, "regmatch or contents failed with %s" % e

    def upstream_metric(self, upstream, metric):
        return 'upstream.' + upstream + '.' + metric;

    def get_state(self, duration):
        self.duration = duration
        metrics = []

        for upstream, value in self.metrics['upstreams'].items():
            metrics.append(
                MetricObject(
                    self.upstream_metric(upstream, 'hits'),
                    value['hits'] / self.duration,
                    "upstream hits/sec"
                )
            )
            metrics.append(
                MetricObject(
                    self.upstream_metric(upstream, 'resp_mean_time_ms'),
                    value['ms'] / value['hits'],
                    "upstream mean resp time/hit"
                )
            )

            metrics.append(
                MetricObject(
                    self.upstream_metric(upstream, 'size'),
                    value['size'],
                    "total size"
                )
            )

            metrics.append(
                MetricObject(
                    self.upstream_metric(upstream, 'speed'),
                    value['size'] / self.duration,
                    "bytes/s"
                )
            )

            metrics.append(
                MetricObject(
                    self.upstream_metric(upstream, 'http_200.hits'),
                    value['http_200'] / self.duration,
                    "http 200/sec"
                )
            )

            if value['http_200']:
                metrics.append(
                    MetricObject(
                        self.upstream_metric(upstream, 'http_200.gzipped'),
                        value['http_200_gzipped'] / self.duration,
                        "http 200 gzipped/sec"
                    )
                )

                metrics.append(
                    MetricObject(
                        self.upstream_metric(upstream, 'http_200.resp_mean_time_ms'),
                        value['http_200_ms'] / value['http_200'],
                        "upstream mean resp time/hit"
                    )
                )

                if value['http_200_gzipped']:
                    metrics.append(
                        MetricObject(
                            self.upstream_metric(upstream, 'http_200.mean_gzip_ratio'),
                            value['gzip_ratio'] / value['http_200_gzipped'],
                            "Mean gzip ratio/gzipped hit"
                        )
                    )

        return metrics
