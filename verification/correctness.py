import os, pathlib, socket, struct, time
from process import Daemon
from wire import Consumer, Producer, connect, packet

def correctness(binary):
    daemon=Daemon(binary);result={'evidence_directory':str(daemon.root)}
    try:
        stdout=bytes(range(256))*8193+b'no-newline';stderr=b'\x00\xff\xfe'+b'x'*37
        producer=Producer(daemon.socket,pipes=True)
        producer.write(1,stdout[:3]);producer.write(1,stdout[3:]);producer.write(2,stderr);producer.close()
        time.sleep(0.2)
        consumer=Consumer(daemon.socket,daemon.root/'accepted.alr')
        consumer.poll(4,ack=False,until=lambda:any(r['kind']==3 for r in consumer.known.values()))
        known=consumer.known;consumer.close();time.sleep(0.1)
        daemon.kill();daemon.start()
        consumer=Consumer(daemon.socket,daemon.root/'accepted.alr',known)
        consumer.poll(3)
        assert consumer.duplicates==len(known) and len(known)>10
        for channel,expected in ((1,stdout),(2,stderr)):
            actual=b''.join(r['payload'] for r in known.values() if r['channel']==channel)
            assert actual==expected,'opaque bytes changed'
        assert daemon.disk()<=daemon.quota
        assert len(list(daemon.spool.glob('*.wal')))<=1,'ACKed segments not reclaimed'
        result['opaque_fifo_fragmented_huge_output']=True
        result['crash_replay_identical_records']=consumer.duplicates
        result['acked_segments_reclaimed']=True
        consumer.close()
        baseline=daemon.usage()['fds']
        for _ in range(100):
            peer=connect(daemon.socket,1);peer.sendall(b'ALP1'+bytes(28));peer.close()
        time.sleep(0.3)
        assert daemon.usage()['fds']<=baseline+1,'rejected peer leaked FD'
        result['malformed_peer_fd_leak']=False
        daemon.kill()
        last=sorted(daemon.spool.glob('*.wal'))[-1]
        before=last.stat().st_size
        with open(last,'ab') as file: file.write(b'ALR')
        daemon.start();assert last.stat().st_size==before
        result['partial_tail_recovery']=True
        first=Producer(daemon.socket)
        bad=first.descriptor[:32]+struct.pack('<Q',2)+first.descriptor[40:]
        peer=connect(daemon.socket,1);peer.sendall(packet(2,bad));assert peer.recv(128)==b'';peer.close()
        first.write(1,b'after-rejection');first.close()
        result['conflicting_generation_rejected']=True
        return result
    finally:daemon.stop()

def quota(binary):
    daemon=Daemon(binary,quota=1024*1024,segment=128*1024,streams=64)
    try:
        producer=Producer(daemon.socket,pipes=True)
        total=8*1024*1024
        for _ in range(total//65536):producer.write(1,b'x'*65536)
        producer.close();time.sleep(0.2)
        assert daemon.disk()<=daemon.quota
        before=daemon.usage()
        consumer=Consumer(daemon.socket,daemon.root/'accepted.alr')
        consumer.poll(8,until=lambda:any(r['kind']==3 for r in consumer.known.values()))
        retained=sum(len(r['payload']) for r in consumer.known.values() if r['kind']==1)
        gaps=[r for r in consumer.known.values() if r['kind']==2]
        lost=sum(struct.unpack_from('<Q',r['payload'],16)[0] for r in gaps)
        assert gaps and lost>0 and retained+lost==total,(retained,lost,total)
        assert any(r['kind']==3 for r in consumer.known.values()),'close did not drain after loss'
        consumer.close()
        return {'written_bytes':total,'retained_bytes':retained,'explicit_gap_bytes':lost,'gaps':len(gaps),
                'near_full_usage':before,'evidence_directory':str(daemon.root)}
    finally:daemon.stop()
