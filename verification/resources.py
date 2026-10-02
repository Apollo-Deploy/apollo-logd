"""Bounded process-level resource measurements against the release executable."""
import os, threading, time
from process import Daemon, measure
from wire import Producer, Consumer

GATE=10*1024*1024

def stream_scale(binary):
    daemon=Daemon(binary,quota=256*1024*1024,segment=8*1024*1024)
    producers=[]
    try:
        result={'idle':measure(daemon)}
        for count,label in ((100,'100_workloads'),(1000,'1000_streams')):
            while len(producers)<count: producers.append(Producer(daemon.socket,pipes=True))
            result[label]=measure(daemon)
        for producer in producers:producer.close()
        assert all(case['rss_bytes']<GATE and case['threads']==1 for case in result.values()),result
        result['evidence_directory']=str(daemon.root)
        return result
    finally:daemon.stop()

class Drain:
    """Streaming fixture: stores counters only, no backlog retained in Python RAM."""
    def __init__(self,path):
        from wire import connect
        self.client=connect(path,2);self.client.settimeout(0.05)
        self.records=0;self.bytes=0;self.dropped=0;self.max_latency_ms=0;self.high={}
        self.running=True;self.error=None
        self.thread=threading.Thread(target=self.run);self.thread.start()
    def run(self):
        import socket,struct
        from wire import unpack,records,packet,MAX_PACKET
        try:
            while self.running:
                try:data=self.client.recv(MAX_PACKET)
                except socket.timeout:continue
                if not data:break
                kind,body=unpack(data)
                if kind!=4:continue
                acks={}
                for record in records(body):
                    stream=record['id'];high=self.high.get(stream,0)
                    assert record['first']==high+1, 'noncontiguous stream'
                    self.high[stream]=record['sequence'];self.records+=1
                    if record['kind']==1:
                        self.bytes+=len(record['payload'])
                        wall=struct.unpack_from('<Q',record['encoded'],48)[0]
                        self.max_latency_ms=max(self.max_latency_ms,(time.time_ns()-wall)/1e6)
                    elif record['kind']==2:self.dropped+=struct.unpack_from('<Q',record['payload'],16)[0]
                    acks[stream]=record
                # Throughput-only receiver: no durability claim. Correctness and real
                # Agent qualification separately exercise fdatasync-before-ACK.
                for record in acks.values():
                    self.client.sendall(packet(5,record['id']+struct.pack('<QQ',record['generation'],record['sequence'])))
        except Exception as error:self.error=repr(error)
    def close(self):
        self.running=False;self.thread.join(2);self.client.close()
        assert self.error is None,self.error

def throughput(binary,mbps,seconds=3,spool_options=None):
    daemon=Daemon(binary,**(spool_options or {'quota':256*1024*1024,'segment':8*1024*1024}))
    consumer=Drain(daemon.socket);producer=Producer(daemon.socket,pipes=True)
    try:
        total=int(mbps*1_000_000*seconds);chunk=b'\0\xff'*32768
        before=daemon.usage();start=time.monotonic();sent=0;peak=before['rss_bytes']
        while sent<total:
            n=min(len(chunk),total-sent);producer.write(1,chunk[:n]);sent+=n
            target=sent/(mbps*1_000_000)-(time.monotonic()-start)
            if target>0:time.sleep(target)
            peak=max(peak,daemon.usage()['rss_bytes'])
        producer.close();deadline=time.monotonic()+10
        while consumer.bytes+consumer.dropped<total and time.monotonic()<deadline:
            daemon.alive();time.sleep(0.05)
        elapsed=time.monotonic()-start;after=daemon.usage()
        assert consumer.bytes+consumer.dropped==total,(consumer.bytes,consumer.dropped,total)
        assert peak<GATE and after['threads']==1
        return {'target_MB_per_sec':mbps,'written_bytes':total,'delivered_bytes':consumer.bytes,
                'dropped_bytes':consumer.dropped,'duration_sec':elapsed,'achieved_MB_per_sec':sent/elapsed/1e6,
                'cpu_core_percent':100*(after['cpu_ticks']-before['cpu_ticks'])/os.sysconf('SC_CLK_TCK')/elapsed,
                'peak_rss_bytes':peak,'max_capture_to_receive_ms':consumer.max_latency_ms,
                'records':consumer.records,'threads':after['threads'],'evidence_directory':str(daemon.root)}
    finally:
        try:consumer.close()
        finally:daemon.stop()

def backlog(binary):
    daemon=Daemon(binary,quota=128*1024*1024,segment=8*1024*1024)
    producer=Producer(daemon.socket,pipes=True)
    try:
        result={};before=daemon.disk()
        for total,label in ((1*1024*1024,'1_MiB'),(32*1024*1024,'32_MiB'),(96*1024*1024,'96_MiB')):
            prior=result.get('bytes_written',0)
            for _ in range((total-prior)//65536):producer.write(1,b'X'*65536)
            time.sleep(0.1)
            result[label]={'rss_bytes':daemon.usage()['rss_bytes'],'wal_bytes':daemon.disk()}
            result['bytes_written']=total
        producer.close();after=daemon.disk()
        rss=[result[label]['rss_bytes'] for label in ('1_MiB','32_MiB','96_MiB')]
        assert max(rss)<GATE and max(rss)-min(rss)<1024*1024,result
        result['overhead_bytes']=after-before-result['bytes_written']
        result['overhead_percent']=100*result['overhead_bytes']/result['bytes_written']
        result['evidence_directory']=str(daemon.root)
        return result
    finally:daemon.stop()
