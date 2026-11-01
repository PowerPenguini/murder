"""Holds a tenant's notification stream open through a chosen node for a
while and reports what arrived and how the stream ended."""
import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from addhoc_api import Client
import urllib.request
tenant, ip, email, seconds = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
c = Client(tenant, ip).sign_in(email, os.environ["PW"])
req = urllib.request.Request(c.base + "/api/notifications/stream", headers={"Accept": "text/event-stream", "Origin": c.base})
start, lines, end = time.time(), 0, "still open"
try:
    with c.opener.open(req, timeout=seconds + 30) as resp:
        print("status", resp.status, resp.headers.get("Content-Type"), "encoding:", resp.headers.get("Content-Encoding"))
        while time.time() - start < seconds:
            line = resp.readline()
            if not line:
                end = "closed by the server after %.0fs" % (time.time() - start); break
            if line.strip():
                lines += 1
except Exception as e:
    end = "error after %.0fs: %s" % (time.time() - start, e)
print("%d event lines in %.0fs; %s" % (lines, time.time() - start, end))
