"""A small AddHoc API client for tests on Murder: signs a staff account in
(enrolling its second factor), then makes requests with its session."""
import base64, os, hashlib, hmac, http.client, http.cookiejar, json, socket, ssl, struct, sys, time, urllib.error, urllib.request, uuid

class _PinnedConnection(http.client.HTTPSConnection):
    """Connects to a chosen node's address, with the tenant's host name in
    the TLS handshake and the request, as curl --resolve does."""
    pin = None
    def connect(self):
        sock = socket.create_connection((self.pin, self.port or 443), self.timeout)
        self.sock = self._context.wrap_socket(sock, server_hostname=self.host)

class _PinnedHandler(urllib.request.HTTPSHandler):
    def __init__(self, ip, context):
        super().__init__(context=context)
        self.ip = ip
    def https_open(self, req):
        ip = self.ip
        class Connection(_PinnedConnection):
            pin = ip
        return self.do_open(Connection, req, context=self._context)

def totp(secret):
    key = base64.b32decode(secret.upper() + "=" * (-len(secret) % 8))
    digest = hmac.new(key, struct.pack(">Q", int(time.time()) // 30), hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    return "%06d" % ((struct.unpack(">I", digest[offset:offset + 4])[0] & 0x7FFFFFFF) % 1_000_000)

class Client:
    def __init__(self, tenant, ip, domain="murder.test"):
        self.base = "https://%s.%s" % (tenant, domain)
        self.host, self.ip = "%s.%s" % (tenant, domain), ip
        self.jar = http.cookiejar.CookieJar()
        ctx = ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
        self.opener = urllib.request.build_opener(_PinnedHandler(ip, ctx), urllib.request.HTTPCookieProcessor(self.jar))

    def via(self, ip):
        """Another client of the same session, through another node."""
        other = Client(self.host.split(".")[0], ip, self.host.split(".", 1)[1])
        for cookie in self.jar:
            other.jar.set_cookie(cookie)
        return other

    def request(self, method, path, body=None, headers=None, raw=False):
        url = self.base + path
        data = body if raw or body is None else json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Origin", self.base)
        if body is not None and not raw:
            req.add_header("Content-Type", "application/json")
        for k, v in (headers or {}).items():
            req.add_header(k, v)
        try:
            with self.opener.open(req, timeout=60) as resp:
                text = resp.read()
                return resp.status, (json.loads(text) if text and resp.headers.get_content_type() == "application/json" else text)
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode(errors="replace")

    def sign_in(self, email, password):
        status, answer = self.request("POST", "/api/auth/login", {"email": email, "password": password})
        assert status == 200, (status, answer)
        # The second factor's key is kept beside this file, private, so a
        # later sign-in answers the code.
        keyfile = "%s/.mfa-%s" % (__file__.rsplit("/", 1)[0], self.host)
        mfa = answer.get("mfa") if isinstance(answer, dict) else None
        if mfa == "enroll":
            status, started = self.request("POST", "/api/auth/mfa/enrollment")
            assert status == 200, (status, started)
            fd = os.open(keyfile, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            os.fchmod(fd, 0o600)  # a file there from before keeps its mode otherwise
            os.write(fd, started["secret"].encode()); os.close(fd)
            status, confirmed = self.request("POST", "/api/auth/mfa/enrollment/confirm", {"code": totp(started["secret"]), "trustBrowser": True})
            assert status == 200, (status, confirmed)
        elif mfa == "verify":
            with open(keyfile) as f:
                secret = f.read().strip()
            status, verified = self.request("POST", "/api/auth/mfa/verify", {"code": totp(secret), "trustBrowser": True})
            if status != 200:  # the code of this step was just used: wait for the next
                time.sleep(31 - time.time() % 30)
                status, verified = self.request("POST", "/api/auth/mfa/verify", {"code": totp(secret), "trustBrowser": True})
            assert status == 200, (status, verified)
        return self

    def multipart(self, path, fields, files):
        boundary = uuid.uuid4().hex
        parts = []
        for name, value in fields.items():
            parts.append(("--%s\r\nContent-Disposition: form-data; name=\"%s\"\r\n\r\n%s\r\n" % (boundary, name, value)).encode())
        for name, (filename, content_type, data) in files.items():
            parts.append(("--%s\r\nContent-Disposition: form-data; name=\"%s\"; filename=\"%s\"\r\nContent-Type: %s\r\n\r\n" % (boundary, name, filename, content_type)).encode() + data + b"\r\n")
        parts.append(("--%s--\r\n" % boundary).encode())
        return self.request("POST", path, b"".join(parts), {"Content-Type": "multipart/form-data; boundary=" + boundary}, raw=True)
