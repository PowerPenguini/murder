// widthcheck.mjs BASE (node internal/panel/demo/widthcheck.mjs http://127.0.0.1:7480, with the demo running): fails when a panel page is wider than the window at
// 390, 1024, 1280 or 1440 px, or a block sticks out of the page's column.
// Drives Chromium through its DevTools protocol (Node 22 or later).
import { spawn } from "node:child_process";
import { rmSync } from "node:fs";
const base = process.argv[2] || "http://127.0.0.1:7480";
const paths = ["/", "/tenants", "/tenants?show=attention", "/tenants/baltic-freight", "/tenants/acme-logistics", "/tenants/baltic-freight/logs",
  "/tenants/atlas/settings", "/apps", "/apps/web", "/events", "/events?show=all", "/cluster", "/access", "/nodes/hel-1", "/nodes/fsn-1",
  "/tenants/baltic-freight/settings?memoryMB=512", "/apps/web?memoryMB=512", "/tenants?show=broken", "/tenants/baltic-freight?range=1h",
  "/cluster?tenant=kowalski-trans&at=2026-11-04+09:00", "/jobs/5eedd8a1n0000001", "/nodes/nbg-1"];
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
ws.addEventListener("message", (m) => {
  const x = JSON.parse(m.data);
  if (x.id) waiting.get(x.id)?.(x);
  // A page asking before it is left would hold the check: said, and let go.
  if (x.method === "Page.javascriptDialogOpening") {
    console.log("a page asked before it was left:", x.params.message || x.params.type);
    ws.send(JSON.stringify({ id: 999999, method: "Page.handleJavaScriptDialog", params: { accept: true } }));
  }
});
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
  // A column whose header shows but whose cells are hidden in every row:
  // a control drawn at one width and lost at another.
  const lost = [];
  for (const table of main.querySelectorAll("table")) {
    if (getComputedStyle(table).display !== "table" || !table.tHead || !table.tBodies[0]) continue;
    const heads = [...table.tHead.rows[0].cells];
    let col = 0;
    const cols = heads.filter((th) => getComputedStyle(th).display !== "none").map((th) => { const c = col; col += th.colSpan; return [th, c]; });
    for (const [th, c] of cols) {
      if (th.getBoundingClientRect().width === 0) continue;
      let seen = false, rows = 0;
      for (const tr of table.tBodies[0].rows) {
        if (tr.getBoundingClientRect().height === 0) continue;
        rows++;
        let at = 0;
        // A hidden cell takes no column: the next visible one does.
        for (const td of tr.cells) {
          if (getComputedStyle(td).display === "none") continue;
          if (at <= c && c < at + td.colSpan) seen = true;
          at += td.colSpan;
        }
      }
      if (rows && !seen) lost.push((table.id || table.className || "table") + ": " + (th.textContent.trim() || "column " + (c + 1)));
    }
    // A row of several cells that shows fewer than the header's columns
    // has a cell hidden at this width.
    for (const tr of table.tBodies[0].rows) {
      if (tr.getBoundingClientRect().height === 0) continue;
      const shown = [...tr.cells].filter((td) => getComputedStyle(td).display !== "none");
      const width = shown.reduce((n, td) => n + td.colSpan, 0);
      if (shown.length > 1 && width < col) {
        lost.push((table.id || table.className || "table") + ": a row shows " + width + " of " + col + " columns");
        break;
      }
    }
  }
  return { doc: document.documentElement.scrollWidth, win: innerWidth, out: out.concat(lost.map((l) => "no cells under " + l)) };
})())`;
await send("Page.enable");
let failed = 0;
for (const width of widths) {
  await send("Emulation.setDeviceMetricsOverride", { width, height: 900, deviceScaleFactor: 1, mobile: width < 600 });
  for (const path of paths) {
    await send("Page.navigate", { url: base + path });
    await sleep(700);
    let answer;
    for (let tries = 0; tries < 15; tries++) {
      answer = await send("Runtime.evaluate", { expression: probe, returnByValue: true });
      if (answer.result && !answer.result.exceptionDetails && answer.result.result.value !== undefined) break;
      await sleep(200);
    }
    if (!answer.result || answer.result.exceptionDetails || answer.result.result.value === undefined) { console.log(width, path, "probe failed:", JSON.stringify(answer).slice(0, 300)); failed++; continue; }
    const r = JSON.parse(answer.result.result.value);
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
