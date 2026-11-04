// widthcheck.mjs BASE (node internal/panel/demo/widthcheck.mjs http://127.0.0.1:7480, with the demo running): fails when a panel page is wider than the window at
// 390, 1024, 1280 or 1440 px, or a block sticks out of the page's column.
// Drives Chromium through its DevTools protocol (Node 22 or later).
import { spawn } from "node:child_process";
import { rmSync } from "node:fs";
const base = process.argv[2] || "http://127.0.0.1:7480";
const paths = ["/", "/tenants", "/tenants?show=attention", "/tenants/baltic-freight", "/tenants/acme-logistics", "/tenants/baltic-freight/logs",
  "/tenants/atlas/settings", "/apps", "/apps/web", "/events", "/events?show=all", "/cluster", "/access", "/nodes/hel-1", "/nodes/fsn-1"];
const widths = [390, 1024, 1280, 1440];
const port = 9800 + Math.floor(Math.random() * 90);
const profile = `/tmp/widthcheck-${port}`;
const chrome = spawn("chromium", ["--headless=new", "--no-sandbox", "--disable-gpu", `--remote-debugging-port=${port}`, `--user-data-dir=${profile}`, "about:blank"], { stdio: "ignore" });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let target;
for (let i = 0; i < 50 && !target; i++) {
  await sleep(200);
  try { target = (await (await fetch(`http://127.0.0.1:${port}/json/list`)).json()).find((x) => x.type === "page"); } catch {}
}
const ws = new WebSocket(target.webSocketDebuggerUrl);
await new Promise((r) => ws.addEventListener("open", r));
let id = 0;
const waiting = new Map();
ws.addEventListener("message", (m) => { const x = JSON.parse(m.data); if (x.id) waiting.get(x.id)?.(x); });
const send = (method, params = {}) => new Promise((r) => { const n = ++id; waiting.set(n, r); ws.send(JSON.stringify({ id: n, method, params })); });
// What sticks out: the document past the window, or a visible block in
// main past main's content box.
const probe = `JSON.stringify((() => {
  const main = document.querySelector("main"), box = main.getBoundingClientRect(), pad = parseFloat(getComputedStyle(main).paddingRight);
  const edge = box.right - pad + 1;
  const out = [...main.querySelectorAll("*")].filter((e) => {
    if (e.closest(".pop, details:not([open]) > :not(summary), .sr, .scroll")) return false;
    const r = e.getBoundingClientRect();
    return r.width > 0 && r.right > edge;
  }).slice(0, 3).map((e) => e.tagName.toLowerCase() + (e.id ? "#" + e.id : "") + (e.className && typeof e.className === "string" ? "." + e.className.split(" ").join(".") : "") + " " + Math.round(e.getBoundingClientRect().right));
  return { doc: document.documentElement.scrollWidth, win: innerWidth, out };
})())`;
let failed = 0;
for (const width of widths) {
  await send("Emulation.setDeviceMetricsOverride", { width, height: 900, deviceScaleFactor: 1, mobile: width < 600 });
  for (const path of paths) {
    await send("Page.navigate", { url: base + path });
    await sleep(700);
    const r = JSON.parse((await send("Runtime.evaluate", { expression: probe, returnByValue: true })).result.result.value);
    if (r.doc > r.win || r.out.length) {
      failed++;
      console.log(`${width} ${path}: page ${r.doc} in ${r.win}${r.out.length ? "; sticks out: " + r.out.join(", ") : ""}`);
    }
  }
}
ws.close();
chrome.kill();
await sleep(300);
rmSync(profile, { recursive: true, force: true });
console.log(failed ? `${failed} pages too wide` : `every page fits at ${widths.join(", ")} px`);
process.exit(failed ? 1 : 0);
