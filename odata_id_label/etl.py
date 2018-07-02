print __doc__

import sys
import json

fin = sys.argv[1]
fout = sys.argv[2]


def deal(line):
    doc = json.loads(line.strip())
    if isinstance(doc["tcpts"], dict):
        doc["tcpts"] = float(doc["tcpts"]["$numberLong"])
    if isinstance(doc["timestamp"], dict):
        doc["timestamp"] = float(doc["timestamp"]["$numberLong"])
    if isinstance(doc["message"]["__timestamp"], dict):
        doc["message"]["__timestamp"] = float(doc["message"]["__timestamp"]["$numberLong"])
    return json.dumps(doc)


def main():
    with open(fin) as f:
        res = []
        for line in f.readlines():
            res.append(deal(line))
    out = open(fout, 'w')
    out.write("\n".join(res))


if __name__ == '__main__':
    main()
