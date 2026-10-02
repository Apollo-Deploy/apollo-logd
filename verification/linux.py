#!/usr/bin/env python3
"""Linux production-binary qualification. Does not invoke Rust unit tests."""
import argparse, hashlib, json, pathlib, platform, resource
from correctness import correctness,quota
from resources import stream_scale,throughput,backlog
from adversarial import adversarial

parser=argparse.ArgumentParser()
parser.add_argument('--binary',type=pathlib.Path,required=True)
parser.add_argument('--evidence',type=pathlib.Path,required=True)
args=parser.parse_args()
assert platform.system()=='Linux'
soft,hard=resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE,(min(16384,hard),hard))
result={'platform':platform.platform(),'binary_bytes':args.binary.stat().st_size,
        'binary_sha256':hashlib.sha256(args.binary.read_bytes()).hexdigest()}
try:
    result['correctness']=correctness(args.binary)
    result['quota']=quota(args.binary)
    result['adversarial']=adversarial(args.binary)
    result['resources']=stream_scale(args.binary)
    result['backlog']=backlog(args.binary)
    result['throughput']=[throughput(args.binary,rate) for rate in (1,10,50)]
    result['result']='PASS'
except Exception as error:
    result.update(result='FAIL',failure=repr(error));raise
finally:args.evidence.write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
