__author__ = 'pc'
from twisted.web.client import Agent
from twisted.internet import reactor
from twisted.internet.protocol import Protocol
from twisted.internet.defer import Deferred
from twisted.internet import defer


import sys
import json
import csv
import os
import urllib


client_id = ""
client_secret = ""
version = 20150101
url_end_point = "https://api.foursquare.com/v2/venues/" #check!
header_row = ['fsq_id', 'fsq_categoryID', 'fsq_category_Name', 'fsq_address', 'fsq_cross_street',
              'lat', 'lng', 'postal', 'city', 'country', 'hours',
              'Visitors', 'Visits', 'PrimaryImageURL']
MAXCALLS = 5000
lines = 0

class ResourcePrinter(Protocol):
    def __init__(self, finished):
        self.finished = finished
    def dataReceived(self, data):
        response = json.loads(data)
        result = []
        hours = ''
        result.append('524e02fe11d29b2194e95a51') #here must be id
        result.append(response['response']['venue']['categories'][0]['id'])
        result.append(response['response']['venue']['categories'][0]['name'])
        result.append(response['response']['venue']['location']['address'])
        result.append(response['response']['venue']['location']['crossStreet'])
        result.append(response['response']['venue']['location']['lat'])
        result.append(response['response']['venue']['location']['lng'])
        result.append(response['response']['venue']['location']['postalCode'])
        result.append(response['response']['venue']['location']['city'])
        result.append(response['response']['venue']['location']['country'])
        for i in response['response']['venue']['hours']['timeframes']:
            if hours != '':
                hours += i['days'] + ' '
                hours += i['open'][0]['renderedTime']
            else:
                hours += i['days'] + ' '
                hours += i['open'][0]['renderedTime']
                hours += ', '
        result.append(hours)
        result.append(response['response']['venue']['stats']['usersCount']) #Visitors
        result.append(response['response']['venue']['stats']['checkinsCount']) #Visits
        result.append(response['response']['venue']['bestPhoto']['prefix'] +
                      str(response['response']['venue']['bestPhoto']['width']) + "x" +
                      str(response['response']['venue']['bestPhoto']['height']) +
                      response['response']['venue']['bestPhoto']['suffix'])
        output(result)
    def connectionLost(self, reason):
        self.finished.callback(None)


def Response(data):
    finished = Deferred()
    data.deliverBody(ResourcePrinter(finished))
    return finished


def Shutdown(ignored):
    reactor.stop()
    #print("--- %s seconds ---" % (time.time() - start_time))


def Failed(ignored):
    print "Faild to execute request"


def Succed(ignored):
    pass


def input():
    global lines
    try:
        with open(sys.argv[1], 'r') as csv_file:
            reader = csv.reader(csv_file)
            next(reader, None)
            for row in reader:
                lines += 1
                yield row
    except Exception:
        print "Can't open file", sys.exc_info()
        exit(0)


def foursquare_venue_url(id):
    global version, client_secret, client_id, url_end_point, MAXCALLS
    MAXCALLS -= 1
    if(MAXCALLS > 0):
	params = urllib.urlencode({"v": version,"client_id": client_id,
        	    "client_secret": client_secret })
	url = url_end_point + str(id)[2:-2] + "?" + params
	agent = Agent(reactor)
	request = agent.request('GET', url)
	request.addCallback(Response)
	request.addErrback(Failed)
    else:
	sys.exit(0)
    return request


def output(result):
    if(os.path.isfile(sys.argv[2])):
        #if file exists
        try:
            with open(sys.argv[2], 'a') as output_file:
                wr = csv.writer(output_file, delimiter = ';', quoting = csv.QUOTE_ALL)
                wr.writerow(result)
        except Exception:
            print 'Error in output ', sys.exc_info()
    else:
        try:
            #if file doesn't exist
            with open(sys.argv[2], 'w') as output_file:
                wr = csv.writer(output_file, delimiter = ';', quoting = csv.QUOTE_ALL)
                wr.writerow(header_row)
                wr.writerow(result)
        except Exception:
            print 'Error in output ', sys.exc_info()


if __name__ == "__main__":
    #start_time = time.time()
    reload(sys)
    sys.setdefaultencoding('utf8')
    ids = input()
    i = 0 #progress bar indicator
    deferred_calls = [foursquare_venue_url(id) for id in ids]
    d = defer.gatherResults(deferred_calls, consumeErrors=True)
    d.addCallback(Succed)
    d.addErrback(Failed)
    d.addBoth(Shutdown)

    reactor.run()
