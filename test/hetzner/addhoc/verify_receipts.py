"""Downloads every receipt of a tenant through its API and checks each
against the SHA-256 the app recorded when it was uploaded."""
import hashlib, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from addhoc_api import Client
tenant, ip, email = sys.argv[1:4]
c = Client(tenant, ip).sign_in(email, os.environ["PW"])
status, items = c.request("GET", "/api/fuelings")
items = items if isinstance(items, list) else items.get("items", [])
checked = bad = 0
for item in items:
    for receipt in item.get("receiptAttachments") or []:
        status, body = c.request("GET", "/api/receipts/%s" % receipt["id"])
        checked += 1
        if status != 200 or hashlib.sha256(body).hexdigest() != receipt["checksumSha256"]:
            bad += 1
print("%s: %d fuelings, %d receipts checked, %d bad" % (tenant, len(items), checked, bad))
