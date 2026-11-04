// jscheck.mjs BASE (node internal/panel/demo/jscheck.mjs http://127.0.0.1:7485, with the demo running): checks the panel's scripts by driving Chromium.
import { spawn } from "node:child_process";
const base = process.argv[2] || "http://127.0.0.1:7485";
const port = 9700 + Math.floor(Math.random() * 90);
const chrome = spawn("chromium", ["--headless=new", "--no-sandbox", "--disable-gpu", `--remote-debugging-port=${port}`, `--user-data-dir=/tmp/jscheck-${port}`, "about:blank"], { stdio: "ignore" });
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
const go = async (path) => { await send("Page.navigate", { url: base + path }); await sleep(1200); };
const mouse = (type, x, y) => send("Input.dispatchMouseEvent", { type, x, y, button: "left", clickCount: type === "mouseMoved" ? 0 : 1 });
const key = (k, code) => send("Input.dispatchKeyEvent", { type: "keyDown", key: k, code, windowsVirtualKeyCode: { ArrowDown: 40, ArrowUp: 38 }[k] });
await send("Page.enable"); await send("Runtime.enable");
await send("Emulation.setDeviceMetricsOverride", { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
const results = [];
const check = (name, ok, detail) => { results.push(ok); console.log((ok ? "ok   " : "FAIL ") + name + (detail ? ": " + detail : "")); };

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
ws.close(); chrome.kill();
process.exit(results.every(Boolean) ? 0 : 1);
