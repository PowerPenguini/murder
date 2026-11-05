// Murder panel: passkey ceremonies. No framework, nothing from elsewhere.
"use strict";
(() => {
  const status = document.getElementById("status");
  const say = (text, bad) => {
    status.textContent = text.charAt(0).toUpperCase() + text.slice(1);
    status.classList.toggle("bad", !!bad);
  };
  const toBuffer = (b64url) => {
    const b64 = b64url.replace(/-/g, "+").replace(/_/g, "/").padEnd(Math.ceil(b64url.length / 4) * 4, "=");
    return Uint8Array.from(atob(b64), (c) => c.charCodeAt(0)).buffer;
  };
  const toB64url = (buffer) => {
    let text = "";
    for (const byte of new Uint8Array(buffer)) text += String.fromCharCode(byte);
    return btoa(text).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  };
  const post = async (path, body) => {
    const response = await fetch(path, {
      method: "POST", credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const answer = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(answer.error || response.statusText);
    return answer;
  };
  const credentialJSON = (credential) => {
    const r = credential.response;
    const out = { id: credential.id, rawId: toB64url(credential.rawId), type: credential.type, clientExtensionResults: credential.getClientExtensionResults(), response: {} };
    for (const field of ["clientDataJSON", "attestationObject", "authenticatorData", "signature", "userHandle"]) {
      if (r[field]) out.response[field] = toB64url(r[field]);
    }
    if (r.getTransports) out.response.transports = r.getTransports();
    if (credential.authenticatorAttachment) out.authenticatorAttachment = credential.authenticatorAttachment;
    return out;
  };
  if (!window.PublicKeyCredential) {
    say("this browser has no passkeys (WebAuthn): open the panel in another", true);
  }

  const signin = document.getElementById("signin");
  if (signin) {
    signin.addEventListener("click", async () => {
      signin.disabled = true;
      try {
        say("asking for a passkey…");
        const options = (await post("/auth/login/begin")).publicKey;
        options.challenge = toBuffer(options.challenge);
        for (const c of options.allowCredentials || []) c.id = toBuffer(c.id);
        const credential = await navigator.credentials.get({ publicKey: options });
        say("checking…");
        const answer = await post("/auth/login/finish", credentialJSON(credential));
        say("signed in as " + answer.user);
        // Back to the page that sent here, on this panel only.
        const next = new URLSearchParams(location.search).get("next") || "/";
        // The page a draft was kept for picks it up again (forms.js).
        try {
          sessionStorage.setItem("murder-resume", new URL(next, location.origin).pathname);
        } catch {}
        location.replace(/^\/(?![\/\\])/.test(next) ? next : "/");
      } catch (error) {
        say(error.name === "NotAllowedError" ? "cancelled, or no passkey here" : error.message, true);
        signin.disabled = false;
      }
    });
  }

  const add = document.getElementById("add");
  if (add) {
    const token = document.getElementById("token");
    // An invitation link carries its token after "#": never sent to a
    // server, and wiped from the address bar.
    if (location.hash.length > 1) {
      token.value = decodeURIComponent(location.hash.slice(1));
      document.getElementById("tokenfield").hidden = true;
      history.replaceState(null, "", location.pathname);
    }
    add.addEventListener("click", async () => {
      add.disabled = true;
      try {
        say("checking the invitation…");
        const begun = await post("/auth/enroll/begin", { token: token.value, label: document.getElementById("label").value });
        const options = begun.options.publicKey;
        options.challenge = toBuffer(options.challenge);
        options.user.id = toBuffer(options.user.id);
        for (const c of options.excludeCredentials || []) c.id = toBuffer(c.id);
        say("making a passkey for " + begun.user + "…");
        const credential = await navigator.credentials.create({ publicKey: options });
        const answer = await post("/auth/enroll/finish", credentialJSON(credential));
        say("passkey added for " + answer.user);
        if (answer.codes) {
          const list = document.getElementById("codelist");
          for (const code of answer.codes) {
            const item = document.createElement("li");
            item.textContent = code;
            list.appendChild(item);
          }
          document.getElementById("enroll").hidden = true;
          document.getElementById("codes").hidden = false;
        } else {
          location.replace("/");
        }
      } catch (error) {
        say(error.name === "NotAllowedError" ? "cancelled" : error.message, true);
        add.disabled = false;
      }
    });
  }
})();
