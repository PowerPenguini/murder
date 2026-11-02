# Asks one name server for the A or AAAA records of a name, with no
# resolver between: dnsquery.py NAME SERVER [A|AAAA]. Prints the rcode and
# the addresses with their TTLs. Standard library only.
import socket, struct, sys, random
name, server = sys.argv[1], sys.argv[2]
qtype = 28 if len(sys.argv) > 3 and sys.argv[3] == "AAAA" else 1
tid = random.randint(0, 65535)
q = struct.pack(">HHHHHH", tid, 0, 1, 0, 0, 0) + b"".join(bytes([len(p)]) + p.encode() for p in name.strip(".").split(".")) + b"\0" + struct.pack(">HH", qtype, 1)
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.settimeout(5)
s.sendto(q, (socket.gethostbyname(server), 53)); d, _ = s.recvfrom(4096)
_, flags, qd, an, _, _ = struct.unpack(">HHHHHH", d[:12])
i = 12
while d[i]: i += d[i] + 1
i += 5
out = []
for _ in range(an):
    if d[i] & 0xC0 == 0xC0: i += 2
    else:
        while d[i]: i += d[i] + 1
        i += 1
    t, c, ttl, l = struct.unpack(">HHIH", d[i:i+10]); i += 10
    if t == 1: out.append(socket.inet_ntop(socket.AF_INET, d[i:i+4]) + f" ttl={ttl}")
    if t == 28: out.append(socket.inet_ntop(socket.AF_INET6, d[i:i+16]) + f" ttl={ttl}")
    i += l
print("rcode", flags & 0xF, sorted(out))
