import json, os, pathlib, subprocess, tempfile, time

class Daemon:
    def __init__(self,binary,quota=16*1024*1024,segment=128*1024,streams=4096,reserve=0):
        self.root=pathlib.Path(tempfile.mkdtemp(prefix='apollo-logd-q-')); self.root.chmod(0o700)
        self.spool=self.root/'spool';self.spool.mkdir(mode=0o700)
        self.socket=self.root/'capture.sock'
        self.command=[str(binary),'--spool',str(self.spool),'--socket',str(self.socket),
                      '--quota-bytes',str(quota),'--segment-bytes',str(segment),'--max-streams',str(streams),'--reserve-bytes',str(reserve)]
        self.quota=quota;self.process=None;self.log=open(self.root/'daemon.log','ab',buffering=0)
        self.start()
    def start(self):
        self.process=subprocess.Popen(self.command,stdout=self.log,stderr=self.log)
        deadline=time.monotonic()+5
        while time.monotonic()<deadline:
            self.alive()
            if self.socket.exists():
                time.sleep(0.08);self.alive();return
            time.sleep(0.02)
        raise AssertionError('logd readiness deadline')
    def alive(self):
        assert self.process.poll() is None, (self.root/'daemon.log').read_text()
    def kill(self):
        self.process.kill();self.process.wait(timeout=3)
    def stop(self):
        if self.process.poll() is None:
            self.process.terminate();self.process.wait(timeout=3)
        self.log.close()
    def disk(self): return sum(p.stat().st_size for p in self.spool.iterdir())
    def usage(self):
        self.alive();path=pathlib.Path(f'/proc/{self.process.pid}')
        status=(path/'status').read_text().splitlines()
        rss=int(next(line.split()[1] for line in status if line.startswith('VmRSS:')))*1024
        stat=(path/'stat').read_text().split()
        return {'rss_bytes':rss,'cpu_ticks':int(stat[13])+int(stat[14]),'fds':len(list((path/'fd').iterdir())),
                'threads':len(list((path/'task').iterdir()))}

def measure(daemon,seconds=2):
    start=daemon.usage();peak=start.copy();begin=time.monotonic()
    while time.monotonic()-begin<seconds:
        time.sleep(0.05);current=daemon.usage()
        for key in ('rss_bytes','fds','threads'): peak[key]=max(peak[key],current[key])
    end=daemon.usage();peak['cpu_core_percent']=100*(end['cpu_ticks']-start['cpu_ticks'])/os.sysconf('SC_CLK_TCK')/(time.monotonic()-begin)
    del peak['cpu_ticks'];return peak
