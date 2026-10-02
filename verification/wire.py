"""Independent ALP1/ALR1 process-boundary qualification client."""
import array, os, socket, struct, time, uuid, zlib

MAX_PACKET = 131072

def packet(kind, body=b""):
    return struct.pack("<4sBBHII", b"ALP1", kind, 0, 1, len(body), zlib.crc32(body)) + body

def unpack(data):
    magic, kind, flags, version, length, crc = struct.unpack("<4sBBHII", data[:16])
    assert (magic, flags, version, length, crc) == (b"ALP1", 0, 1, len(data)-16, zlib.crc32(data[16:]))
    return kind, data[16:]

def records(body):
    while body:
        assert len(body) >= 72 and body[:4] == b"ALR1"
        length, crc = struct.unpack_from("<II", body, 4)
        assert 72 <= length <= 65608 and length <= len(body)
        data, body = body[:length], body[length:]
        assert zlib.crc32(data[12:]) == crc
        kind, channel, flags = struct.unpack_from("<BBH", data, 12)
        generation, sequence, wall, mono, size, reserved = struct.unpack_from("<QQQQII", data, 32)
        assert size == len(data)-72 and reserved == 0 and sequence > 0
        first = struct.unpack_from("<Q", data, 72)[0] if kind == 2 else sequence
        yield {"id": data[16:32], "generation": generation, "sequence": sequence,
               "first":first, "kind":kind, "channel":channel, "payload":data[72:], "encoded":data}

def connect(path, role):
    client = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    client.settimeout(3)
    client.connect(str(path))
    client.sendall(packet(1, bytes([role])))
    return client

class Producer:
    def __init__(self, path, descriptor=None, pipes=False):
        self.descriptor = descriptor or (uuid.uuid4().bytes + uuid.uuid4().bytes + struct.pack("<QB7x", 1, 1))
        self.id = self.descriptor[:16]
        self.client = connect(path, 1)
        self.writers=[]
        readers=[]
        if pipes:
            for _ in range(2):
                read, write = os.pipe2(os.O_CLOEXEC)
                readers.append(read); self.writers.append(write)
        try:
            ancillary = [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", readers))] if readers else []
            self.client.sendmsg([packet(2, self.descriptor)], ancillary)
            assert unpack(self.client.recv(128)) == (3, self.descriptor)
        finally:
            for fd in readers: os.close(fd)
        if pipes: self.client.close()

    def write(self, channel, data):
        if self.writers:
            while data:
                n=os.write(self.writers[channel-1], data)
                data=data[n:]
        else:
            for offset in range(0, len(data), 65536):
                self.client.sendall(packet(7, bytes([channel])+bytes(7)+data[offset:offset+65536]))

    def close(self):
        if self.writers:
            for fd in self.writers: os.close(fd)
            self.writers=[]
        elif self.client.fileno() >= 0:
            self.client.sendall(packet(6)); self.client.close()

class Consumer:
    def __init__(self, path, durable_path, known=None):
        self.client=connect(path, 2)
        self.client.settimeout(0.15)
        self.file=open(durable_path,"ab",buffering=0)
        self.known=known if known is not None else {}
        self.high={}
        for (stream, sequence), record in self.known.items(): self.high[stream]=max(sequence,self.high.get(stream,0))
        self.registrations={}
        self.duplicates=0

    def poll(self, seconds=1, ack=True, until=None):
        deadline=time.monotonic()+seconds
        while time.monotonic()<deadline:
            try: data=self.client.recv(MAX_PACKET)
            except socket.timeout:
                if until and until(): return
                continue
            assert data, "logd closed Agent connection"
            kind, body=unpack(data)
            if kind==2: self.registrations[body[:16]]=body; continue
            if kind==9: continue
            assert kind==4
            batch=list(records(body))
            for record in batch:
                stream, sequence=record['id'],record['sequence']
                assert stream in self.registrations
                key=(stream,sequence)
                if key in self.known:
                    assert self.known[key]['encoded']==record['encoded'], 'retry changed record identity'
                    self.duplicates+=1
                else:
                    assert record['first']==self.high.get(stream,0)+1, 'silent sequence hole'
                    self.file.write(record['encoded']); self.known[key]=record; self.high[stream]=sequence
            os.fdatasync(self.file.fileno())
            if ack:
                for record in batch:
                    self.client.sendall(packet(5,record['id']+struct.pack('<QQ',record['generation'],record['sequence'])))
            if until and until(): return

    def close(self):
        self.client.close(); self.file.close()
