import time
import re

from logster.logster_helper import MetricObject, LogsterParser
from logster.logster_helper import LogsterParsingException

class Status:
    """ A predicate on status codes paired with a unique name """
    def __init__(self, prop, pred):
        self.prop = prop
        self.pred = pred

    def matches(self, code):
        return self.pred(code)

# Status codes are matched here from top to bottom - so do most specific first
status = [
    Status('http_503', lambda c: c == 503),

    Status('http_1xx', lambda c: c >= 100 and c < 200),
    Status('http_2xx', lambda c: c >= 200 and c < 300),
    Status('http_3xx', lambda c: c >= 300 and c < 400),
    Status('http_4xx', lambda c: c >= 400 and c < 500),
    Status('http_5xx', lambda c: c >= 500 and c < 600)
]

class NginxStatus(LogsterParser):
    def __init__(self, option_string=None):
        self.metrics = dict( (s.prop, 0) for s in status)
        self.reg = re.compile('.*HTTP/1.\d\" (?P<http_status_code>\d{3}) .*')

    def parse_line(self, line):
        try:
            regMatch = self.reg.match(line)

            if regMatch:
                linebits = regMatch.groupdict()
                code = int(linebits['http_status_code'])

                for s in status:
                    if s.matches(code):
                        self.metrics[s.prop] += 1
                        break
            else:
                raise LogsterParsingException, "regmatch failed to match"

        except Exception, e:
            raise LogsterParsingException, "regmatch or contents failed with %s" % e

    def get_state(self, duration):
        self.duration = duration

        return [
            MetricObject(s.prop,
                         (self.metrics[s.prop] / self.duration),
                         "Responses/sec")
            for s in status
        ]
