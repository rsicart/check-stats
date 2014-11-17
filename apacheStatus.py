#!/usr/bin/env python3

import urllib.request
from lxml import html
import re
import operator
from datetime import datetime

import settings


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'



class apacheFork:

    def __init__(self, elem):


        self.srv = elem[0].xpath('descendant::text()')[0].strip()
        self.pid = elem[1].xpath('descendant::text()')[0].strip()
        self.acc = elem[2].xpath('descendant::text()')[0].strip()
        self.status = elem[3].xpath('descendant::text()')[0].strip()
        self.cpu = elem[4].xpath('descendant::text()')[0].strip()
        self.ss = elem[5].xpath('descendant::text()')[0].strip()
        self.req = elem[6].xpath('descendant::text()')[0].strip()
        self.conn = elem[7].xpath('descendant::text()')[0].strip()
        self.child = elem[8].xpath('descendant::text()')[0].strip()
        self.slot = elem[9].xpath('descendant::text()')[0].strip()
        self.client = elem[10].xpath('descendant::text()')[0].strip()
        self.vhost = elem[11].xpath('descendant::text()')[0].strip()
        self.request = elem[12].xpath('descendant::text()')[0].strip()
        self.script = re.search('^(?:GET|POST) /([^.]+\.php)', self.request)
        if self.script is not None:
            self.script = self.script.group(1)

    def __str__(self):
        return '{}::: pid:{}, status:{}, request:{}'.format(self.srv, self.pid, self.status, self.request)



class apacheServer:

    def __init__(self, name, color=False):
        self.statusMap = {
            '_': 'Waiting for Connection',
            'S': 'Starting up',
            'R': 'Reading Request',
            'W': 'Sending Reply',
            'K': 'Keepalive (read)',
            'D': 'DNS Lookup',
            'C': 'Closing connection',
            'L': 'Logging',
            'G': 'Gracefully finishing',
            'I': 'Idle cleanup of worker',
            '.': 'Open slot with no current process',
        }
        self.name = name
        self.color = color
        self.forks = []
        self.response = None

    def getStatus(self, status):
        return self.statusMap[status]

    def len(self):
        return len(self.forks)

    def fetch(self):
        url = 'http://{}/server-status'.format(self.name)
        self.response = urllib.request.urlopen(url).read()

    def parse(self):
        tree = html.fromstring(self.response.decode())
        tr = tree.xpath('//table[1]/tr')
        for elem in tr[1:]:
            a = apacheFork(elem)
            self.forks.append(a)

    def run(self):
        self.fetch()
        self.parse()
        print("\n\nGot {} forks for server {}".format(self.len(), self.name))
        self.getStats()
        self.printStats()

    def getStats(self):
        self.statsByScript = {}
        self.statsByStatus = {
            '_': {},
            'S': {},
            'R': {},
            'W': {},
            'K': {},
            'D': {},
            'C': {},
            'L': {},
            'G': {},
            'I': {},
            '.': {},
        }
        for f in self.forks:
            # by status
            try:
                self.statsByStatus[f.status][f.script] += 1
            except KeyError:
                self.statsByStatus[f.status].update({f.script: 0})
                self.statsByStatus[f.status][f.script] += 1

            # by script
            try:
                self.statsByScript[f.script][f.status] += 1
            except KeyError:
                self.statsByScript.update({f.script:{}})
                for k in self.statusMap.keys():
                    if k is not None:
                        self.statsByScript[f.script].update({k: 0})
                if k is not None:
                    self.statsByScript[f.script][f.status] += 1

    def printStats(self):
        ts = datetime.now() 
        print("\n===============================================")
        print("{} ::: {}".format(self.name, ts))
        print("===============================================")
        #self.prettyPrint("Stats by status:", self.statsByStatus)
        #self.prettyPrint("Stats by script:", self.statsByScript)
        self.printMatrix("Stats by script:", self.statsByScript)

    def printMatrix(self, title, multidimStats):

        # header
        print("\n-------------------------------------------")
        print("{}".format(title))
        print("-------------------------------------------")

        # columns
        print("\nScript name\t\t\t _  |  S  |  R  |  W  |  K  |  D  |  C  |  L  |  G  |  I  |  .  |  TOTAL")
        print("-----------\t\t\t-------------------------------------------------------------------------")

        for key1, list1 in multidimStats.items():
            total = 0

            # get subtotal by status
            for key2, subtotal in list1.items():
                total += subtotal

            # print line
            if self.color:
                print("{}{}{}{:03}{} | {}{:03}{} | {}{:03}{} | {}{:03}{} | {}{:03}{} | {}{:03}{} | {}{:03}{} | {}{:03}{} | {}{:03}{} | {}{:03}{} | {}{:03}{} | {}{:03}{}".format(
                    *self.getPrintableLine(key1, list1, total)
                ))
            else:
                print("{}{}{:03} | {:03} | {:03} | {:03} | {:03} | {:03} | {:03} | {:03} | {:03} | {:03} | {:03} | {:03}".format(
                    *self.getPrintableLine(key1, list1, total)
                ))

    def getPrintableLine(self, key1, list1, total):
        indent = self.getIndent(key1)
        if self.color:
            return [
                key1, indent,
                self.getColor(list1['_']), list1['_'], bcolors.ENDC,
                self.getColor(list1['S']), list1['S'], bcolors.ENDC,
                self.getColor(list1['R']), list1['R'], bcolors.ENDC,
                self.getColor(list1['W']), list1['W'], bcolors.ENDC,
                self.getColor(list1['K']), list1['K'], bcolors.ENDC,
                self.getColor(list1['D']), list1['D'], bcolors.ENDC,
                self.getColor(list1['C']), list1['C'], bcolors.ENDC,
                self.getColor(list1['L']), list1['L'], bcolors.ENDC,
                self.getColor(list1['G']), list1['G'], bcolors.ENDC,
                self.getColor(list1['I']), list1['I'], bcolors.ENDC,
                self.getColor(list1['.']), list1['.'], bcolors.ENDC,
                self.getColor(total), total, bcolors.ENDC
            ]
        else:
            return [
                key1, indent,
                list1['_'],
                list1['S'],
                list1['R'],
                list1['W'],
                list1['K'],
                list1['D'],
                list1['C'],
                list1['L'],
                list1['G'],
                list1['I'],
                list1['.'],
                total
            ]

    def getIndent(self, key):
        if key is None or len(key) < 8:
            indent = '\t\t\t\t'
        elif len(key) > 25:
            indent = '\t'
        elif len(key) >= 20 and len(key) < 25:
            indent = '\t\t'
        else:
            indent = '\t\t\t'
        return indent

    def getColor(self, value):
        thresholdLow = 0.33
        thresholdHigh = 0.66
        if value < (self.len() * thresholdLow):
            return bcolors.OKGREEN
        elif value >= (self.len() * thresholdLow) and value < (self.len() * thresholdHigh):
            return bcolors.WARNING
        else:
            return bcolors.FAIL

    def prettyPrint(self, title, multidimStats):
        print("\n-------------------------------------------")
        print("{}".format(title))
        for key1, list1 in multidimStats.items():
            total = 0
            print("\n*** {}:".format(key1))
            for key2, subtotal in list1.items():
                if subtotal < (self.len() * 0.25):
                    color = bcolors.OKGREEN
                elif subtotal >= (self.len() * 0.25) and subtotal < (self.len() * 0.50):
                    color = bcolors.WARNING
                elif subtotal >= (self.len() * 0.50):
                    color = bcolors.FAIL
                print("-> {}{}:\t {}{}".format(color, subtotal, key2, bcolors.ENDC))
                total += subtotal
            print("-> {}:\t {}".format(total, "TOTAL"))


if __name__ == '__main__':
    for server in settings.HOSTS['frontals']:
        a = apacheServer(server, color=True)
        a.run()

