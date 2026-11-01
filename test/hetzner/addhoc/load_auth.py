"""Signed-in load on AddHoc tenants through chosen edges: each worker signs
in once, then loops over the fuelings list, the session and a receipt
download, counting failures and latency. Usage:
load_auth.py SECONDS tenant=ip[,ip...]:email ... (passwords in $PW_<tenant>)"""
import os, sys, threading, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from addhoc_api import Client

seconds = int(sys.argv[1])
results = {"ok": 0, "failed": 0, "times": [], "errors": []}
lock = threading.Lock()

def worker(signed, ip, tenant):
    c = signed.via(ip)
    status, fuelings = c.request("GET", "/api/fuelings")
    receipt = None
    if status == 200 and isinstance(fuelings, (list, dict)):
        items = fuelings if isinstance(fuelings, list) else fuelings.get("items", [])
        for item in items:
            attachments = item.get("receiptAttachments") or []
            receipt = attachments[0].get("id") if attachments else None
            if receipt:
                break
    paths = ["/api/fuelings", "/api/auth/me", "/api/drivers"] + (["/api/receipts/%s" % receipt] if receipt else [])
    end = time.time() + seconds
    while time.time() < end:
        for path in paths:
            t = time.time()
            try:
                status, _ = c.request("GET", path)
            except Exception as e:
                status = str(e)
            took = time.time() - t
            with lock:
                if status == 200:
                    results["ok"] += 1; results["times"].append(took)
                else:
                    results["failed"] += 1
                    if len(results["errors"]) < 10:
                        results["errors"].append("%s %s %s via %s: %s" % (time.strftime("%H:%M:%S"), tenant, path, ip, str(status)[:120]))

threads = []
for arg in sys.argv[2:]:
    tenant_ips, email = arg.rsplit(":", 1)
    tenant, ips = tenant_ips.split("=")
    ips = ips.split(",")
    # One sign-in per tenant: its session serves every worker.
    signed = Client(tenant, ips[0]).sign_in(email, os.environ["PW_" + tenant])
    for ip in ips:
        threads.append(threading.Thread(target=worker, args=(signed, ip, tenant)))
for t in threads: t.start()
for t in threads: t.join()
times = sorted(results["times"]) or [0]
pick = lambda q: times[min(len(times) - 1, int(len(times) * q))] * 1000
print("ok=%d failed=%d p50=%.0fms p99=%.0fms max=%.0fms" % (results["ok"], results["failed"], pick(0.5), pick(0.99), times[-1] * 1000))
for e in results["errors"]: print(e)
