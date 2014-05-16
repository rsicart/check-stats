#!/usr/bin/env python

'''
Script to check stats for last 4 days.
We can check different targets for the tests."
For example, for csv logs we can check frontal cluster files or cron server only.
Another example, for http logs we can choose to parse apache or nginx log files.

Author: R.Sicart
'''
import sys, getopt
import subprocess
from datetime import date
import MySQLdb

# Init settings
try:
	import settings
	DBCONF = settings.DBCONF
	HOSTS = settings.HOSTS
	HOME_FOLDER = settings.HOME_FOLDER
	CRON_CSV_FOLDER = settings.CRON_CSV_FOLDER
	FRONT_CSV_FOLDER = settings.FRONT_CSV_FOLDER
except:
	DBCONF = {
		'dbhost': 'localhost',
		'dbname': "",
		'dbtable': "",
		'dbuser': "",
		'dbpass': "",
	}
	HOSTS = {
		'cron': [],
		'frontals': [],
	}
	HOME_FOLDER = '/home/user'
	CRON_CSV_FOLDER = ''
	FRONT_CSV_FOLDER = ''

def usage():
	print "\nUsage: check-stats.py [options]"
	print "\nScript to check stats for last {} days. We can check different targets for the tests.".format(4)
	print "For example, for csv logs we can check frontal cluster files or cron server only."
	print "Another example, for http logs we can choose to parse apache or nginx log files."
	print "\nOptions:"
	print " -h | --help shows usage"
	print " -f | --from=YYYY-MM-DD to specify beginning date."
	print " -r | --to=YYYY-MM-DD to specify ending date."
	print " -c | --campaign=INT to set campaign id."
	print " -b | --banner=INT to set banner id."
	print " -w | --website=INT to set website id."
	print " -e | --event=(*display|click|click_certified) to specify an event type."
	print " -s | --script=(*tag|banner|advertise) to specify script name to parse http logs."
	print " -l | --lookforcsvlogs=all to select csv log target: cron server (default) or frontal cluster (all)."
	print " -x | --exclude=http,csv,db,track to avoid executing specifyc tasks."
	print " -d | --daemon=(*apache|nginx) service to check for http logs."
	print " -r | --track= !!! NOT IMPLEMENTED !!!"
	print " -v | --verbose shows some detailed info."
	print "\n"


def getDbConfig():
	''' Returns a dict with database configuration
	'''
	return DBCONF


def getHosts(target = 'frontals'):
	hosts = HOSTS
	if target in hosts:
		return hosts[target]
	else:
		return hosts['frontals']


def getHttpPattern(script, campaign, banner, website):
	''' Return grep pattern to search in apache logs
	'''
	httpScript = {
		'tag':'tag\.php?id={}-{}-{}'.format(campaign, banner, website),
		'banner':'banner\.php?id={}-{}-{}'.format(campaign, banner, website),
		'advertise':'advertise\.php?live-test=true&website-id={}&campaign-id={}&ad-asset-id={}'.format(website, campaign, banner),
	}
	if script in httpScript:
		return httpScript[script]
	else:
		return httpScript['tag']

def countHttpLogs(dateFrom, dateTo, campaign, banner, website, script, daemon = 'apache', verbose = False):
	''' Checks displays for a specific campaing, banner, website and date
	'''
	if not checkDates(dateFrom, dateTo):
		usage()
		exit(2)

	logfolder = '/var/log/apache2'
	if daemon == 'nginx':
		logfolder = '/var/log/nginx'

	logfile = 'access_log'
	grepLogs = []
	# TODO: check if adding access_log by default is right !!! (f.e. when checking yesterdays stats)
	if dateFrom == date.today() or dateTo == date.today():
		grepLogs.append(logfolder + '/' + logfile)

	if dateFrom < date.today():
		# logs for today are stored in tomorrows filename.gz
		dateFrom = dateFrom.replace(day = dateFrom.day + 1)
		dateTo = dateTo.replace(day = dateTo.day + 1)

		dateFromBack = dateFrom
		for day in range(dateFrom.day, dateTo.day + 1):
			logfile = 'access_log-{}.gz'.format(dateFromBack.strftime("%Y%m%d"))
			grepLogs.append(logfolder + '/' + logfile)
			dateFromBack = dateFromBack.replace(day = day + 1)

	if len(grepLogs) == 0:
		print 'Error: date range is wrong.'
		usage()
		exit(6)

	total = 0
	hits = {}
	procs = {}
	pattern = getHttpPattern(script, campaign, banner, website)
	for server in getHosts('frontals'):
		command = 'ssh {} zgrep --no-filename -c -e \'{}\' {}'.format(server, pattern, ' '.join(grepLogs))
		procs[server] = subprocess.Popen(command.split(), stdout = subprocess.PIPE)
		hits[server] = 0

	while procs:
		for server, proc in procs.items():
			if proc.poll() is not None:
				output = proc.stdout.read()
				try:
					''' output is iterable (multiple files)
					'''
					output_iter = iter(output)
					for sum in output.rstrip().split('\n'):
						total += int(sum)
						hits[server] += int(sum)
				except TypeError:
					total += int(output)
					hits[server] += int(output)
				''' remove proc from queue '''
				del(procs[server])

	if verbose:
		for server, subtotal in hits.items():
			# total per server
			print '{}:\t{}'.format(server, hits[server])

	print total


def checkDates(dateFrom, dateTo):
	''' Check if dates are valid
		dateFrom and dateTo must share same year and month, and a max difference of days of 4
	'''
	today = date.today()
	if dateTo > today or dateFrom > today:
		print "Wrong dates: dates in the futre not valid."
		return False
	if dateFrom.year != today.year or dateTo.year != today.year:
		print "Wrong year."
		return False
	if dateFrom.month != today.month or dateTo.month != today.month:
		print "Wrong month."
		return False
	if dateTo.day - dateFrom.day > 4:
		print "Wrong day range."
		return False
	return True


def countCsvLogs(dateFrom, dateTo, campaign, banner, website, event, server = HOSTS['cron'], verbose = False):
	''' Checks displays for a specific campaing, banner, website and date
		dateFrom and dateTo must share same year and month, and a max difference of days of 4
	'''
	if not checkDates(dateFrom, dateTo):
		usage()
		exit(2)

	displayFilter = getEventFilter(event)
	total = 0

	if server == 'all':
		target = 'frontals'
		targetFolder = FRONT_CSV_FOLDER
	else:
		target = 'cron'
		targetFolder = CRON_CSV_FOLDER

	''' Check front server
	'''
	hits = {}
	procs = {}
	for host in getHosts(target):
		hits[host] = 0
		procs[host] = {}
		try:
			# range + 1 to get today's data
			for day in range(dateFrom.day, dateTo.day + 1):
				procs[host] = {day:0}
				logfolder = '{}/{}/{}/{}'.format(targetFolder, dateFrom.year, dateFrom.month, day)
				command = 'ssh {} find {} -name \"{}_{}_{}\" -print0 | xargs -0 cat | grep \"{}\" | wc -l'.format(host, logfolder, campaign, banner, website, displayFilter)
				procs[host] = {
					day: subprocess.Popen(command.split(), stdout = subprocess.PIPE)
				}
		except TypeError:
			pass

	while procs:
		for host, date in procs.items():
			for day, proc in date.items():
				if proc.poll() is not None:
					output = proc.stdout.read()
					total += int(output)
					hits[host] += int(output)
					del(date[day])
			if len(date) == 0:
				del(procs[host])

	if verbose:
		for host, subtotal in hits.items():
			print '{}:\t{}'.format(host, subtotal)

	print total


def getDbDisplays(dateFrom, dateTo, campaign, banner, website):
	''' Counts displays for a campaign, banner, website and date
	'''
	if not checkDates(dateFrom, dateTo):
		usage()
		exit(2)

	conf = getDbConfig()
	db = MySQLdb.connect(
		host=conf['dbhost'],
		user=conf['dbuser'],
		passwd=conf['dbpass'],
		db=conf['dbname']
	)
	cur = db.cursor()
	# Default sql request
	sql = "SELECT SUM(DISPLAY) FROM {} WHERE IDCAMPAIGN='{}' AND IDBANNER='{}' AND IDWEBSITE='{}' AND DATE_SAVE = '{}';".format(conf['dbtable'], campaign, banner, website, dateFrom.isoformat())
	# Date range sql request
	if dateFrom != dateTo:
		sql = "SELECT SUM(DISPLAY) FROM {} WHERE IDCAMPAIGN='{}' AND IDBANNER='{}' AND IDWEBSITE='{}' AND DATE_SAVE BETWEEN '{}' AND '{}';".format(conf['dbtable'], campaign, banner, website, dateFrom.isoformat(), dateTo.isoformat())
	cur.execute(sql)
	total = cur.fetchone()
	db.close()
	print total[0]


def getEventFilter(event = 'display'):
	eventTypes = {
		'display': '1,0,0,0,0,0,0,',
		'click': '0,1,0,0,0,0,0,',
		'click_certified': '0,0,0,0,1,0,0,',
	}
	if event in eventTypes:
		return eventTypes[event]
	else:
		return eventTypes['display']


def getExclude(csv):
	valid = ['http', 'csv', 'db', 'track']
	options = csv.split(',')
	return [val for val in options if val in valid]


def checkTracking(version):
	raise NotImplementedError, "TODO ! Check if tracking tags exist in advertiser page looking in http logs etc..."

def main(argv):
	try:
		opts, args = getopt.getopt(argv, "hf:t:c:b:w:e:s:l:x:d:r:v", ["help", "from=", "to=", "campaign=", "banner=", "website=", "event=", "script=", "lookforcsvlogs=", "exclude=", "daemon=", "track=", "verbose"])
	except getopt.GetoptError:
		usage()
		sys.exit(2)

	actions = {
		'from': date.today(),
		'to': date.today(),
		'campaign': None,
		'banner': None,
		'website': None,
		'script': None,
		'event': None,
		'lookforcsvlogs': None,
		'exclude': [],
		'daemon': None,
		'track': None,
		'verbose': False,
	}

	''' Parse script arguments
	'''
	for opt, arg in opts:
		if opt in ('-h', "--help"):
			usage()
			sys.exit(1)
		elif opt in ('-f', '--from'):
			year, month, day  = arg.split('-')
			actions['from'] = date(int(year), int(month), int(day))
		elif opt in ('-t', '--to'):
			year, month, day  = arg.split('-')
			actions['to'] = date(int(year), int(month), int(day))
		elif opt in ('-c', '--campaign'):
			actions['campaign'] = int(arg)
		elif opt in ('-b', '--banner'):
			actions['banner'] = int(arg)
		elif opt in ('-w', '--website'):
			actions['website'] = int(arg)
		elif opt in ('-e', '--event'):
			actions['event'] = arg
		elif opt in ('-s', '--script'):
			actions['script'] = arg
		elif opt in ('-l', '--lookforcsvlogs'):
			actions['lookforcsvlogs'] = arg
		elif opt in ('-d', '--daemon'):
			actions['daemon'] = arg
		elif opt in ('-r', '--track'):
			actions['track'] = arg
		elif opt in ('-x', '--exclude'):
			actions['exclude'] = getExclude(arg)
		elif opt in ('-v', '--verbose'):
			actions['verbose'] = True

	if actions['campaign'] is None or actions['banner'] is None or actions['website'] is None:
		usage()
		exit(5)

	print '--------------------------------------------------------------------------------'
	print 'From: {} ::: To: {}'.format(actions['from'], actions['to'])
	print 'Campaign: {} ::: Website: {} ::: Banner: {}'.format(actions['campaign'], actions['website'], actions['banner'])
	print '--------------------------------------------------------------------------------'

	if 'db' not in actions['exclude']:
		print "Db stats:"
		getDbDisplays(actions['from'], actions['to'], actions['campaign'], actions['banner'], actions['website'])

	if 'http' not in actions['exclude']:
		print "Http logs:"
		countHttpLogs(actions['from'], actions['to'], actions['campaign'], actions['banner'], actions['website'], actions['script'], actions['daemon'], actions['verbose'])

	if 'csv' not in actions['exclude']:
		print "Csv logs:"
		countCsvLogs(actions['from'], actions['to'], actions['campaign'], actions['banner'], actions['website'], actions['event'], actions['lookforcsvlogs'], actions['verbose'])

	if 'track' not in actions['exclude']:
		print "Tracking:"
		checkTracking(actions['track'])


if __name__ == "__main__":
	main(sys.argv[1:])
