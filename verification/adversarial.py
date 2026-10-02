"""Adversarial Linux process fixtures; never invoke Rust unit tests."""
import os, random, socket, struct, subprocess, time
from process import Daemon
from wire import Producer, Consumer, connect, packet, unpack


def rejected(client):
    try:assert client.recv(131072)==b'', 'invalid peer retained'
    except ConnectionResetError:pass
    finally:client.close()


def adversarial(binary):
    daemon=Daemon(binary,streams=64)
    result={'evidence_directory':str(daemon.root)}
    try:
        producer=Producer(daemon.socket);producer.write(1,b'first\0\xff')
        consumer=Consumer(daemon.socket,daemon.root/'accepted.alr');consumer.poll(.3,ack=False)
        assert consumer.high[producer.id]==1
        consumer.client.sendall(packet(5,producer.id+struct.pack('<QQ',2,1)));rejected(consumer.client);consumer.file.close()
        result['stale_ack_rejected']=True
        consumer=Consumer(daemon.socket,daemon.root/'accepted.alr');consumer.poll(.3,ack=False)
        consumer.client.sendall(packet(5,producer.id+struct.pack('<QQ',1,2)));rejected(consumer.client);consumer.file.close()
        result['unsent_ack_rejected']=True
        # A source loss range must survive exactly as one stable gap.
        producer.client.sendall(packet(8,struct.pack('<QQQQII',1,1,80,5,4,0)))
        time.sleep(.1)
        consumer=Consumer(daemon.socket,daemon.root/'accepted.alr');consumer.poll(.3,ack=False)
        gaps=[r for r in consumer.known.values() if r['kind']==2]
        assert gaps and gaps[0]['first']==2 and gaps[0]['sequence']==6
        consumer.client.sendall(packet(5,producer.id+struct.pack('<QQ',1,3)));rejected(consumer.client);consumer.file.close()
        result['partial_gap_ack_rejected']=True
        # Capture resumes after a loss marker, even with Agent absent.
        producer.write(2,b'after-gap');producer.close();time.sleep(.15)
        consumer=Consumer(daemon.socket,daemon.root/'accepted.alr');consumer.poll(.3)
        assert any(r['payload']==b'after-gap' for r in consumer.known.values())
        known=consumer.known;consumer.close()
        consumer=Consumer(daemon.socket,daemon.root/'accepted.alr',known)
        consumer.client.sendall(packet(5,producer.id+struct.pack('<QQ',1,8)))
        time.sleep(.1);daemon.alive();consumer.close()
        result['duplicate_ack_safe']=True;result['offline_capture_after_gap']=True
        peer=connect(daemon.socket,1);peer.sendall(packet(2,producer.descriptor));rejected(peer)
        result['closed_stream_rejected']=True
        # Fenced generations cannot attach or reuse the old stream identity.
        old=Producer(daemon.socket);old.client.close();time.sleep(.1)
        fresh=Producer(daemon.socket,os.urandom(16)+old.descriptor[16:32]+struct.pack('<QB7x',2,1))
        peer=connect(daemon.socket,1);peer.sendall(packet(2,old.descriptor));rejected(peer);fresh.close()
        result['stale_registration_rejected']=True
        baseline=daemon.usage()['fds'];rng=random.Random(20261002)
        for _ in range(500):
            peer=connect(daemon.socket,1)
            # Exercise the descriptor decoder with valid outer checksums.
            body=rng.randbytes(rng.choice((0,1,47,48,49,128,4096)))
            peer.sendall(packet(2,body));rejected(peer)
        time.sleep(.1);assert daemon.usage()['fds']<=baseline+1
        result['bounded_malformed_registrations']=500
        if os.geteuid()==0:
            daemon.root.chmod(0o711);daemon.socket.chmod(0o666)
            code="import socket,sys; s=socket.socket(socket.AF_UNIX,socket.SOCK_SEQPACKET); s.connect(sys.argv[1]); assert s.recv(32)==b''"
            subprocess.run(['python3','-c',code,str(daemon.socket)],user=65534,group=65534,check=True,timeout=3)
            daemon.socket.chmod(0o660);daemon.root.chmod(0o700)
            result['untrusted_uid_rejected']=True
        # A complete CRC-corrupt record is not silently skipped or truncated.
        source=Producer(daemon.socket);source.write(1,b'corruption-target');source.close();time.sleep(.15)
        daemon.kill();last=sorted(daemon.spool.glob('*.wal'))[-1]
        with open(last,'r+b') as file:
            file.seek(32+72);value=file.read(1);assert value
            file.seek(32+72);file.write(bytes([value[0]^1]))
        before=last.read_bytes()
        failed=subprocess.run(daemon.command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=5)
        assert failed.returncode!=0 and last.read_bytes()==before
        result['complete_corruption_fails_closed']=True
        return result
    finally:daemon.stop()


if __name__=='__main__':
    import argparse, json, pathlib
    parser=argparse.ArgumentParser()
    parser.add_argument('--binary',type=pathlib.Path,required=True)
    parser.add_argument('--evidence',type=pathlib.Path,required=True)
    args=parser.parse_args()
    result=adversarial(args.binary)
    args.evidence.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))
