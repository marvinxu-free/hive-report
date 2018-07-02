import os
import time
import datetime

# Jinja2 variables
EXEC_CMD = "  hive "
DEBUG = ("{{debug}}" == "true")


def debug(msg):
    if DEBUG:
        print("[job_builder][DEBUG]" + msg)


ONE_HOUR = datetime.timedelta(hours=1)
ONE_DAY = datetime.timedelta(days=1)


def parse_azkaban_ts(azkaban_ts):
    """Parse Azkaban timestamp, return a datetime object in UTC timezone.
    e.g. 2017-06-229T01:04:06.370-08:00, 2015-01-21T01:04:06.370-07:00

    """
    try:
        str_job_start_ts, x = azkaban_ts.split(".")
        now = datetime.datetime.strptime(str_job_start_ts, "%Y-%m-%dT%H:%M:%S")
        if x.endswith("-08:00"):
            now += datetime.timedelta(hours=8)
        elif x.endswith("-07:00"):
            now += datetime.timedelta(hours=7)
    except:
        print('failed to parse azkaban.flow.start.timestamp : %s' % os.getenv("azkaban.flow.start.timestamp"))
        raise Exception("Failed to parse azkaban timestamp")
        # now = datetime.datetime.now()
    return now


def from_utc_to_tz(dt, tz):
    """Shift timezone from UTC to GMT+8.

    """
    if tz not in ('GMT+8'):
        raise Exception("tz %s is currently not supported." % tz)
    if tz == 'GMT+8':
        dt_new = dt + datetime.timedelta(hours=8)
    return dt_new


def get_day(dt):
    return dt.strftime("%Y%m%d")


def get_hour(dt):
    return dt.strftime("%H")


def check(job, date, flag="done"):
    """Check job flags.

    """
    cmd = 'workboard check %s %s %s' % (job, date, flag)
    return (os.system(cmd) == 0)


def wait_for(job, date, flag="done", interval=600, retry=20):
    """Wait for job flags.

    """
    cmd = 'workboard check %s %s %s' % (job, date, flag)
    for i in range(retry):
        if os.system(cmd) == 0:
            break
        time.sleep(interval)
        seconds = (i + 1) * interval
        print("waiting [%s] for %d seconds" % (cmd, seconds))
    return i + 1 < retry


def execute_sql(sql_file, args):
    """Execute SQL file.

    """
    cmd = EXEC_CMD
    for k, v in args.iteritems():
        cmd += ' -d %s="%s"' % (k, v)
    cmd += " -f %s" % sql_file
    print(cmd)
    debug(open(sql_file, 'r').read())

    ret = os.system(cmd)
    if ret != 0:
        raise Exception("Hive SQL Failed")


def execute_sh(sh_file, args):
    """Execute shell script.

    """
    cmd = ""
    for k, v in args.iteritems():
        cmd += '%s="%s" ' % (k, v)
    cmd += "sh %s" % sh_file
    print(cmd)

    ret = os.system(cmd)
    if ret != 0:
        raise Exception("Shell Script Failed.")
