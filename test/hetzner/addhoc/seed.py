"""Seeds an AddHoc tenant with a company's worth of data through its API:
drivers, vehicles and fuelings with receipt scans (PDFs of a phone scan's
size). Usage: seed.py TENANT IP EMAIL (password in $PW) DRIVERS VEHICLES FUELINGS"""
import json, os, sys, time, datetime
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from addhoc_api import Client

tenant, ip, email = sys.argv[1:4]
drivers_n, vehicles_n, fuelings_n = (int(x) for x in sys.argv[4:7])
c = Client(tenant, ip).sign_in(email, os.environ["PW"])
start = time.time()
run = "".join(chr(65 + int(d)) for d in str(int(start))[-3:])  # three letters, new each run


def created(path, body):
    status, answer = c.request("POST", path, body)
    if status not in (200, 201) or not isinstance(answer, dict):
        sys.exit("%s: %s %s" % (path, status, str(answer)[:300]))
    return answer["id"]


drivers = [created("/api/drivers", {"name": "Kierowca %s %03d" % (run, i), "wageType": "hourly_base", "monthlyBaseCents": 0, "hourlyRateMicros": 0, "active": True}) for i in range(drivers_n)]
vehicles = [created("/api/vehicles", {"kind": "tractor", "registrationNumber": "W%s%04d" % (run, i)}) for i in range(vehicles_n)]
failed = 0
day = datetime.datetime(2026, 1, 1, 8, tzinfo=datetime.timezone.utc)
for i in range(fuelings_n):
    pdf = b"%PDF-1.4\n" + os.urandom(250_000) + b"\n%%EOF\n"
    payload = {"vehicleId": vehicles[i % len(vehicles)], "driverId": drivers[i % len(drivers)],
               "fuelingDate": (day + datetime.timedelta(hours=7 * i)).isoformat().replace("+00:00", "Z"),
               "litersMilli": 350000 + (i % 50) * 1000, "currency": "PLN",
               "amountMinor": 200000 + i, "fuelType": "DIESEL", "toBrim": True, "paymentMethod": "E100"}
    status, answer = c.multipart("/api/fuelings", {"payload": json.dumps(payload)}, {"receipt": ("receipt-%04d.pdf" % i, "application/pdf", pdf)})
    if status not in (200, 201):
        failed += 1
        if failed <= 3:
            print("fueling %d: %s %s" % (i, status, str(answer)[:300]))
print("%s: %d drivers, %d vehicles, %d fuelings (%d failed) in %.0fs" % (tenant, len(drivers), len(vehicles), fuelings_n - failed, failed, time.time() - start))
