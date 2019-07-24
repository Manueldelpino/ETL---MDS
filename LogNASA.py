import re
import datetime
import os

from pyspark.sql import Row
from pyspark import SparkContext

month_map = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
    'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12}

APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'

def parse_apache_time(s):

    """ Convert Apache time format into a Python datetime object
    Args:
        s (str): date and time in Apache time format
    Returns:
        datetime: datetime object (ignore timezone for now)
    """
    return datetime.datetime(int(s[7:11]),
                             month_map[s[3:6]],
                             int(s[0:2]),
                             int(s[12:14]),
                             int(s[15:17]),
                             int(s[18:20]))


def parseApacheLogLine(logline):

    """ Parse a line in the Apache Common Log format
    Args:
        logline (str): a line of text in the Apache Common Log format
    Returns:
        tuple: either a dictionary containing the parts of the Apache Access Log and 1,
               or the original invalid log line and 0
    """

    # A regular expression pattern to extract fields from the log line

    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        return (logline, 0)
    size_field = match.group(9)
    if size_field == '-':
        size = long(0)
    else:
        size = long(match.group(9))
    return (Row(
        host          = match.group(1),
        client_identd = match.group(2),
        user_id       = match.group(3),
        date_time     = parse_apache_time(match.group(4)),
        method        = match.group(5),
        endpoint      = match.group(6),
        protocol      = match.group(7),
        response_code = int(match.group(8)),
        content_size  = size
    ), 1)

def parseLogs():

    """ Read and parse log file """
    logFile = os.path.join('data', 'apache.access.log_small')
    parsed_logs = (sc
                   .textFile(logFile)
                   .map(parseApacheLogLine)
                   .cache())

    access_logs = (parsed_logs
                   .filter(lambda s: s[1] == 1)
                   .map(lambda s: s[0])
                   .cache())

    failed_logs = (parsed_logs
                   .filter(lambda s: s[1] == 0)
                   .map(lambda s: s[0]))
    failed_logs_count = failed_logs.count()
    if failed_logs_count > 0:
        print 'Number of invalid logline: %d' % failed_logs.count()
        for line in failed_logs.take(20):
            print 'Invalid logline: %s' % line

    print 'Read %d lines, successfully parsed %d lines, failed to parse %d lines' % (parsed_logs.count(), access_logs.count(), failed_logs.count())
    return parsed_logs, access_logs, failed_logs

sc= SparkContext()

parsed_logs, access_logs, failed_logs = parseLogs()

# Estadisticas basadas en el tamano de las peticiones
content_sizes = access_logs.map(lambda log: log.content_size).cache()

print 'Content Size Avg: %i, Min: %i, Max: %s' % (
    content_sizes.reduce(lambda a, b : a + b) / content_sizes.count(),
    content_sizes.min(),
    content_sizes.max())

print content_sizes.mean()

# N de peticiones de cada codigo de respuesta
responseCodeToCount = (access_logs
                       .map(lambda log: (log.response_code, 1))
                       .reduceByKey(lambda a, b : a + b)
                       .cache())
responseCodeToCountList = responseCodeToCount.take(100)
print 'Found %d response codes' % len(responseCodeToCountList)
print 'Response Code Counts: %s' % responseCodeToCountList

# 20 Hosts que han accedido mas de 10 veces.
hostCountPairTuple = access_logs.map(lambda log: (log.host, 1))

hostSum = hostCountPairTuple.reduceByKey(lambda a, b : a + b)

hostMoreThan10 = hostSum.filter(lambda s: s[1] > 10)

hostsPick20 = (hostMoreThan10
               .map(lambda s: s[0])
               .take(20))

print hostsPick20


# 10 endpoints mas visitados
endpointCounts = (access_logs
                  .map(lambda log: (log.endpoint, 1))
                  .reduceByKey(lambda a, b : a + b))

topEndpoints = endpointCounts.takeOrdered(10, lambda s: -1 * s[1])

print "10 endpoints mas visitados: ", topEndpoints


#10 endpoints erroneos mas visitados

not200 = access_logs.filter(lambda x: x.response_code!=200)

endpointCountPairTuple = not200.map(lambda x: (x.endpoint,1))

endpointSum = endpointCountPairTuple.reduceByKey(lambda x,y: x+y)

topTenErrURLs = endpointSum.takeOrdered(10, lambda (x,y): -1*y)
print 'Top Ten no200 URLs: %s' % topTenErrURLs

#hosts unicos

hosts = access_logs.map(lambda log: log.host)
uniqueHosts = hosts.distinct()
uniqueHostCount = uniqueHosts.count()
print "numero de hosts unicos: ", uniqueHostCount

# hosts unicos por dia

dayToHostPairTuple = access_logs.map(lambda log: (log.date_time.day, log.host))

dayGroupedHosts = dayToHostPairTuple.groupByKey()

dayHostCount = dayGroupedHosts.map(lambda (x, y): (x, len(set(y))))

dailyHosts = (dayHostCount.sortByKey(lambda x: x))

dailyHostsList = (dailyHosts.cache()
                  .take(30))
print "hosts unicos por dia: ", dailyHostsList

# Media de peticiones diaria por host

dayAndHostTuple = access_logs.map(lambda x: (x.date_time.day, x.host))

groupedByDay = dayAndHostTuple.groupByKey()

sortedByDay = groupedByDay.sortByKey(lambda x: x)

avgDailyReqPerHost = (sortedByDay.map(lambda (x, y): (x, len(y) / len(set(y))))
                      .collect())
print "Media de peticiones diaria por host: "
for i in avgDailyReqPerHost:
    print i[0],":",i[1]


# 40 endpoints distintos que generan codigo de respuesta = 404
badRecords = (access_logs.filter(lambda log: log.response_code==404)
              .cache())


badEndpoints = badRecords.map(lambda log: (log.host,1))

badUniqueEndpoints = badEndpoints.groupByKey().map(lambda (x,y):x)

badUniqueEndpointsPick40 = badUniqueEndpoints.take(40)
print '40 404 URLS: %s' % badUniqueEndpointsPick40


badEndpointsCountPairTuple = badRecords.map(lambda log: (log.endpoint,1))
badEndpointsSum = badEndpointsCountPairTuple.reduceByKey (lambda a,b: a+b)
badEndpointsTop20 = badEndpointsSum.takeOrdered(20, lambda (x,y): -1*y)
print 'Top 20 404 URLs: %s' % badEndpointsTop20
