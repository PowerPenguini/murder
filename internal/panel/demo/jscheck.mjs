// jscheck.mjs BASE (node internal/panel/demo/jscheck.mjs http://127.0.0.1:7485, with the demo running): checks the panel's scripts by driving Chromium.
import { spawn } from "node:child_process";
import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
const base = process.argv[2] || "http://127.0.0.1:7485";
const port = 9700 + Math.floor(Math.random() * 90);
// Chromium's profile, removed as the check ends.
const profile = `${process.env.TMPDIR || tmpdir()}/jscheck-${port}`;
process.on("exit", () => { try { rmSync(profile, { recursive: true, force: true }); } catch {} });
const chrome = spawn("chromium", ["--headless=new", "--no-sandbox", "--disable-gpu", `--remote-debugging-port=${port}`, `--user-data-dir=${profile}`, "about:blank"], { stdio: "ignore" });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let target;
for (let i = 0; i < 50 && !target; i++) { await sleep(200); try { target = (await (await fetch(`http://127.0.0.1:${port}/json/list`)).json()).find((x) => x.type === "page"); } catch {} }
const ws = new WebSocket(target.webSocketDebuggerUrl);
await new Promise((r) => ws.addEventListener("open", r));
let id = 0; const waiting = new Map(); let dialogs = 0;
ws.addEventListener("message", (m) => { const x = JSON.parse(m.data); if (x.id) waiting.get(x.id)?.(x);
  if (x.method === "Page.javascriptDialogOpening") { dialogs++; ws.send(JSON.stringify({ id: 999999, method: "Page.handleJavaScriptDialog", params: { accept: true } })); } });
const send = (method, params = {}) => new Promise((r) => { const n = ++id; waiting.set(n, r); ws.send(JSON.stringify({ id: n, method, params })); });
const ev = async (expr) => (await send("Runtime.evaluate", { expression: expr, returnByValue: true, awaitPromise: true })).result.result.value;
// Navigates and waits for the page to finish loading (up to 15 s), then a
// moment for its scripts.
const go = async (path) => {
  await send("Page.navigate", { url: base + path }); await sleep(300);
  for (let i = 0; i < 60; i++) { if ((await send("Runtime.evaluate", { expression: "document.readyState", returnByValue: true })).result?.result?.value === "complete") break; await sleep(250); }
  await sleep(600);
};
const mouse = (type, x, y) => send("Input.dispatchMouseEvent", { type, x, y, button: "left", clickCount: type === "mouseMoved" ? 0 : 1 });
const key = (k, code) => send("Input.dispatchKeyEvent", { type: "keyDown", key: k, code, windowsVirtualKeyCode: { ArrowDown: 40, ArrowUp: 38 }[k] });
await send("Page.enable"); await send("Runtime.enable");
await send("Emulation.setDeviceMetricsOverride", { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
const results = [];
const check = (name, ok, detail) => { results.push(ok); console.log((ok ? "ok   " : "FAIL ") + name + (detail ? ": " + detail : "")); };
// Gives atlas a disk limit of 2 GB, through the settings' review, then has
// its data grow to fill it (the demo's /demo/fill), as data grows into a
// limit set long ago.
const fillAtlas = async () => {
  await go("/tenants/atlas");
  await ev(`(async () => {
    const page = new DOMParser().parseFromString(await (await fetch("/tenants/atlas/settings")).text(), "text/html");
    const body = new URLSearchParams(new FormData(page.querySelector("#settings")));
    body.set("own-diskGB", "on"); body.set("diskGB", "2"); body.set("do", "review");
    const review = new DOMParser().parseFromString(await (await fetch("/tenants/atlas/settings", { method: "POST", body })).text(), "text/html");
    const save = new URLSearchParams(new FormData(review.querySelector("form.review-box")));
    save.set("do", "save");
    await fetch("/tenants/atlas/settings", { method: "POST", body: save });
    await fetch("/demo/fill?tenant=atlas", { method: "POST" });
    return 1;
  })()`);
};

// 1.2: hover the memory plot near a mark, move down to its annotation: the reading stays.
await go("/tenants/baltic-freight");
const geo = JSON.parse(await ev(`(() => { const f = document.querySelectorAll("figure.chart")[1]; f.scrollIntoView({block: "center"}); const m = JSON.parse(f.dataset.marks); const p = f.querySelector(".plot").getBoundingClientRect(); const a = f.querySelector(".legrow").getBoundingClientRect(); const x = p.left + p.width * m[m.length - 1].X / 1000; return JSON.stringify({x, y: p.top + p.height / 2, ay: a.top + a.height / 2, below: a.bottom + 120}); })()`));
await mouse("mouseMoved", geo.x, geo.y); await sleep(150);
const before = await ev(`document.querySelectorAll("figure.chart")[1].querySelector(".annot").textContent`);
for (let y = geo.y; y <= geo.ay; y += 8) { await mouse("mouseMoved", geo.x, y); await sleep(20); }
await mouse("mouseMoved", geo.x + 40, geo.ay); await sleep(150);
const after = await ev(`document.querySelectorAll("figure.chart")[1].querySelector(".annot").textContent`);
check("chart reading kept on the way to its annotation", before !== "" && after === before, JSON.stringify([before, after]));
await mouse("mouseMoved", 30, 30); await sleep(150);
check("chart reading goes on leaving the figure", (await ev(`document.querySelectorAll("figure.chart")[1].querySelector(".annot").textContent`)) === "");
await mouse("mousePressed", geo.x, geo.y); await mouse("mouseReleased", geo.x, geo.y); await mouse("mouseMoved", 30, 30); await sleep(150);
check("a click pins the reading", (await ev(`document.querySelectorAll("figure.chart")[1].querySelector(".annot").textContent`)) !== "");

// Round 8: a mark lane is counted right, at desktop and phone widths: no
// more restart bars drawn than the legend counts, and no two drawn groups
// closer than 4 px.
const laneProbe = `JSON.stringify((() => {
  const legend = document.querySelector(".marks-legend"); const said = legend ? parseInt(legend.textContent) || 0 : 0;
  const drawn = (r) => { for (let e = r; e && e.tagName !== "svg"; e = e.parentNode) if (e.getAttribute && e.getAttribute("display") === "none") return false; return true; };
  const out = [];
  for (const lane of document.querySelectorAll("figure.chart svg.lane")) {
    const rects = [...lane.querySelectorAll("rect.m-restart, rect.m-change")].filter(drawn).map((r) => [r.getBoundingClientRect(), r.closest("a")]);
    const bars = rects.filter(([, a], k) => lane.querySelectorAll("rect.m-restart").length && true).filter(([r, a]) => a && [...a.querySelectorAll("rect.m-restart")].filter(drawn).some((x) => x.getBoundingClientRect().left === r.left)).length;
    rects.sort((p, q) => p[0].left - q[0].left);
    let close = 0;
    for (let k = 1; k < rects.length; k++) if (rects[k][1] !== rects[k - 1][1] && rects[k][0].left - rects[k - 1][0].right < 4) close++;
    out.push({ bars, said, close });
  }
  return out;
})())`;
for (const width of [1440, 390]) {
  await send("Emulation.setDeviceMetricsOverride", { width, height: 1000, deviceScaleFactor: 1, mobile: width < 600 });
  for (const path of ["/tenants/baltic-freight", "/tenants/acme-logistics"]) {
    await go(path);
    const lanes = JSON.parse(await ev(laneProbe));
    check(`${path} at ${width}: lanes counted right, groups apart`, lanes.length > 0 && lanes.every((l) => l.bars <= l.said && l.close === 0), JSON.stringify(lanes[1] || lanes[0]));
  }
}
await send("Emulation.setDeviceMetricsOverride", { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });

// Where a disk limit link landed: the caret, the field's value, the row and
// its table's heading against any sticky bar, the address's fragment.
const landing = `JSON.stringify((() => {
  const field = document.getElementById("f-diskGB"), row = document.getElementById("diskGB");
  const head = row.closest("table").previousElementSibling;
  const bar = document.querySelector(".side"), barBottom = innerWidth <= 760 ? bar.getBoundingClientRect().bottom : 0;
  const r = row.getBoundingClientRect(), h = head.getBoundingClientRect();
  return { focus: document.activeElement && document.activeElement.id, value: field.value, rowTop: Math.round(r.top), rowBottom: Math.round(r.bottom), headTop: Math.round(h.top), barBottom: Math.round(barBottom), height: innerHeight,
    hash: location.hash, red: !!row.querySelector(".st.bad"), scrollY: Math.round(scrollY) };
})())`;
// On a desktop screen the row shows from the top: the page is not
// scrolled away from the tenant's name.
const inView = (l) => l.focus === "f-diskGB" && l.rowTop >= l.barBottom && l.rowBottom <= l.height && l.headTop >= l.barBottom && l.hash === "" && (l.barBottom > 0 || l.scrollY === 0);

// Round 11: "Set its disk limit" lands with the caret in the Disk field,
// and the row and its table's heading in view below any sticky bar.
for (const width of [1440, 390]) {
  await send("Emulation.setDeviceMetricsOverride", { width, height: 844, deviceScaleFactor: 1, mobile: width < 600 });
  // From the tenant's page, as a person comes: a new page, not a jump
  // within this one.
  await go("/tenants/atlas");
  await go("/tenants/atlas/settings?set=diskGB#diskGB");
  await sleep(400);
  const landed = JSON.parse(await ev(landing));
  check(`the disk limit link at ${width}: caret in the field, row and heading in view`, inView(landed), JSON.stringify(landed));
}
await send("Emulation.setDeviceMetricsOverride", { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });

// 1.3: a removed added row is not restored with the draft.
await go("/tenants/atlas/settings");
await ev(`(() => { const set = (el, v) => { el.value = v; el.dispatchEvent(new Event("input", {bubbles: true})); }; document.querySelector("[data-add-row=env]").click(); const n = document.querySelectorAll("input[name=envnewname]"); set(n[n.length-1], "TEST_REMOVAL"); set(document.querySelectorAll("input[name=envnewvalue]")[n.length-1], "x"); const rows = document.querySelectorAll("#env tr.add"); rows[rows.length-1].querySelector(".remove-row").click(); sessionStorage.setItem("murder-resume", location.pathname); return 1; })()`);
dialogs = 0;
await send("Page.reload"); await sleep(1500);
const restored = await ev(`[...document.querySelectorAll("input[name=envnewname]")].map((i) => i.value).join(",")`);
check("a removed row is not restored", !restored.includes("TEST_REMOVAL"), restored);
await ev(`(() => { const b = [...document.querySelectorAll("button.link")].find((x) => x.textContent.includes("Discard the draft")); if (b) b.click(); return !!b; })()`);
await sleep(1500);
check("discarding the draft asks nothing more", dialogs <= 1, dialogs + " dialogs (1 is the reload above)");

// 1.5: arrow keys move between the paces, the review follows in place, focus stays.
await go("/apps/web?memoryMB=512");
await ev(`document.querySelector("#spec form button.primary").click()`); await sleep(1500);
const url1 = await ev(`location.href`);
await ev(`document.querySelector("input[name=pace]:checked").focus()`);
await key("ArrowUp", "ArrowUp"); await sleep(1200);
const after1 = JSON.parse(await ev(`JSON.stringify({pace: document.querySelector("input[name=pace]:checked").value, focus: document.activeElement && document.activeElement.value, plan: document.querySelector("#plan .head .dim") && document.querySelector("#plan .head .dim").textContent, url: location.href})`));
check("ArrowUp chose waves, in place", after1.pace === "waves" && after1.focus === "waves" && after1.plan === "in waves", JSON.stringify(after1));
await key("ArrowDown", "ArrowDown"); await sleep(1000); await key("ArrowDown", "ArrowDown"); await sleep(1200);
const after2 = JSON.parse(await ev(`JSON.stringify({pace: document.querySelector("input[name=pace]:checked").value, focus: document.activeElement && document.activeElement.value, plan: document.querySelector("#plan .head .dim").textContent, url: location.href})`));
check("two ArrowDowns reached every tenant now, focus kept", after2.pace === "now" && after2.focus === "now" && after2.plan === "every tenant now", JSON.stringify(after2));
check("the address is a GET review with the pace", after2.url.includes("/apps/web/review?") && after2.url.includes("pace=now") && url1.includes("/apps/web/review?"), after2.url);
dialogs = 0; await send("Page.reload"); await sleep(1500);
check("a reload asks nothing and keeps the pace", dialogs === 0 && (await ev(`document.querySelector("input[name=pace]:checked").value`)) === "now");
// Round 12: "Raise its disk limit", on a tenant whose data fills its disk
// limit, lands as "Set" does, from the overview and from its page: the
// caret in the field, a value that leaves room in it, the row red. Last:
// it leaves atlas broken.
await fillAtlas();
for (const width of [1440, 390]) {
  await send("Emulation.setDeviceMetricsOverride", { width, height: 844, deviceScaleFactor: 1, mobile: width < 600 });
  for (const from of ["/", "/tenants/atlas"]) {
    await go(from);
    const href = await ev(`(() => { const a = [...document.querySelectorAll("a")].find((a) => a.textContent.trim() === "Raise its disk limit" && a.href.includes("/tenants/atlas/")); return a ? a.getAttribute("href") : ""; })()`);
    await go(href || "/missing");
    await sleep(400);
    const raised = JSON.parse(await ev(landing));
    check(`"Raise its disk limit" from ${from} at ${width}: caret in the field, 10 in it, the row red and in view`, href.includes("set=diskGB") && inView(raised) && raised.value === "10" && raised.red, JSON.stringify(raised));
  }
}
ws.close(); chrome.kill(); await sleep(500);
process.exit(results.every(Boolean) ? 0 : 1);
