// widthcheck.mjs BASE (node internal/panel/demo/widthcheck.mjs http://127.0.0.1:7480, with the demo running): fails when a panel page is wider than the window at
// 390, 1024, 1280 or 1440 px, or a block sticks out of the page's column,
// or text comes closer than 8 px to the frame of its box or open menu.
// Drives Chromium through its DevTools protocol (Node 22 or later).
import { spawn } from "node:child_process";
import { rmSync } from "node:fs";
const base = process.argv[2] || "http://127.0.0.1:7480";
const paths = ["/", "/tenants", "/tenants?show=attention", "/tenants/baltic-freight", "/tenants/acme-logistics", "/tenants/baltic-freight/logs",
  "/tenants/atlas/settings", "/apps", "/apps/web", "/events", "/events?show=all", "/cluster", "/access", "/nodes/hel-1", "/nodes/fsn-1",
  "/tenants/baltic-freight/settings?memoryMB=512", "/apps/web?memoryMB=512", "/tenants?show=broken", "/tenants/baltic-freight?range=1h",
  "/cluster?tenant=kowalski-trans&at=2026-11-04+09:00", "/jobs/5eedd8a1n0000001", "/nodes/nbg-1"];
const widths = process.env.WIDTHS ? process.env.WIDTHS.split(",").map(Number) : [390, 1024, 1280, 1440];
// PATHS=/a,/b checks those pages alone.
if (process.env.PATHS) paths.splice(0, paths.length, ...process.env.PATHS.split(","));
const port = 9800 + Math.floor(Math.random() * 90);
const profile = `${process.env.TMPDIR || "/tmp"}/widthcheck-${port}`;
process.on("exit", () => { try { rmSync(profile, { recursive: true, force: true }); } catch {} });
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
const probe = `document.readyState !== "complete" ? undefined : JSON.stringify((() => {
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
// Text never touches a box's edge: in each framed box (a notice, a problem,
// a review, a panel) and in every menu or sheet, opened one at a time, and
// each action in a menu of actions, every line of text and every field or
// button keeps at least 8 px from the box's frame. Returns what is closer.
const edges = `(async () => {
  const MIN = 8, close = [];
  const frame = ".pop, .notice, .problem, .review-box, .panel";
  const shown = (e) => { const r = e.getBoundingClientRect(); return r.width > 0 && r.height > 0 && getComputedStyle(e).visibility !== "hidden"; };
  const measure = (box, where) => {
    const b = box.getBoundingClientRect(), cs = getComputedStyle(box);
    const left = b.left + parseFloat(cs.borderLeftWidth), right = b.right - parseFloat(cs.borderRightWidth);
    const top = b.top + parseFloat(cs.borderTopWidth), bottom = b.bottom - parseFloat(cs.borderBottomWidth);
    // A box that is only a left rule (a notice row) has no frame on its
    // other sides: those are measured from its padding all the same.
    const rects = [];
    const walker = document.createTreeWalker(box, NodeFilter.SHOW_TEXT);
    for (let n = walker.nextNode(); n; n = walker.nextNode()) {
      if (!n.textContent.trim() || !n.parentElement || !shown(n.parentElement)) continue;
      if (n.parentElement.closest(".sr, [hidden]")) continue;
      // Text in a box nested inside (a field's own frame) is that box's.
      const inner = n.parentElement.closest(frame + ", input, select, textarea, button, .button, summary.button");
      if (inner && inner !== box && box.contains(inner) && !inner.matches("button, .button, summary.button")) continue;
      if (n.parentElement.closest("details:not([open]) > :not(summary)")) continue;
      // Word by word: a line's trailing space hangs past its box.
      const range = document.createRange();
      for (const m of n.textContent.matchAll(/\\S+/g)) {
        range.setStart(n, m.index); range.setEnd(n, m.index + m[0].length);
        for (const r of range.getClientRects()) if (r.width > 0) rects.push([r, m[0].slice(0, 24)]);
      }
    }
    for (const e of box.querySelectorAll("input:not([type=hidden]):not([type=checkbox]):not([type=radio]), select, textarea, button, .button")) {
      if (shown(e) && !e.closest(".sr, [hidden]")) rects.push([e.getBoundingClientRect(), e.tagName.toLowerCase() + " " + (e.textContent || e.placeholder || "").trim().slice(0, 20)]);
    }
    for (const [r, what] of rects) {
      // Rows scrolled out of a sheet are not drawn at its edge.
      if (r.bottom < top || r.top > bottom) continue;
      const gaps = [r.left - left, right - r.right];
      if (r.top >= top && r.bottom <= bottom) gaps.push(r.top - top, bottom - r.bottom);
      const gap = Math.min(...gaps);
      if (gap < MIN) close.push(where + ": '" + what + "' " + Math.round(gap) + " px from its frame");
    }
  };
  const main = document.querySelector("main");
  for (const box of main.querySelectorAll(".notice, .problem, .review-box, .panel")) if (shown(box) && !box.closest(".pop")) measure(box, box.className.split(" ")[0]);
  const wait = () => new Promise((r) => setTimeout(r, 60));
  for (const d of document.querySelectorAll("details")) {
    const pop = d.querySelector(":scope > .pop");
    if (!pop || !shown(d.querySelector(":scope > summary"))) continue;
    d.open = true; await wait();
    const name = "menu " + d.querySelector(":scope > summary").textContent.trim().slice(0, 24);
    measure(pop, name);
    for (const act of pop.querySelectorAll(":scope > details.act")) { act.open = true; await wait(); measure(pop, name + " / " + act.querySelector("summary").textContent.trim().slice(0, 20)); act.open = false; }
    d.open = false; await wait();
  }
  return JSON.stringify([...new Set(close)].slice(0, 6));
})()`;
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
    const edge = await send("Runtime.evaluate", { expression: edges, awaitPromise: true, returnByValue: true });
    const touching = edge.result && edge.result.result && typeof edge.result.result.value === "string" ? JSON.parse(edge.result.result.value) : ["the edge check failed: " + JSON.stringify(edge).slice(0, 200)];
    if (touching.length) r.out = r.out.concat(touching.map((t) => "touches: " + t));
    if (r.doc > r.win || r.out.length) {
      failed++;
      console.log(`${width} ${path}: page ${r.doc} in ${r.win}${r.out.length ? "; sticks out: " + r.out.join(", ") : ""}`);
    }
  }
}
ws.close();
chrome.kill();
await sleep(300);
await sleep(500);
try { rmSync(profile, { recursive: true, force: true, maxRetries: 5, retryDelay: 200 }); } catch {}
console.log(failed ? `${failed} pages fail` : `every page fits at ${widths.join(", ")} px`);
process.exit(failed ? 1 : 0);
