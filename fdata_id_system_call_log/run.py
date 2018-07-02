import sys
sys.path.append("..")

from common import *


if __name__ == "__main__":
    import sys
    azkaban_ts = sys.argv[1]

    from common import parse_azkaban_ts
    now = parse_azkaban_ts(azkaban_ts)
    last_day = now - ONE_DAY
    env = {'day': last_day.strftime("%Y%m%d")}
    print env
    execute_sql('run.sql', env)

