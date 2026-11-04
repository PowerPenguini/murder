// Murder panel: a chart says the moment under the pointer, the finger or
// the arrow keys: its time and every series' value, in its caption. Without
// this script the caption says the latest values.
"use strict";
(() => {
  // The charts of one group share their cursor: a moment read on one is
  // read on all, so a rise in memory lines up with errors and response.
  const groups = new Map();
  const arm = (figure) => {
    if (figure.dataset.armed) return;
    figure.dataset.armed = "1";
    let points;
    try {
      points = JSON.parse(figure.dataset.points || "[]");
    } catch {
      return;
    }
    const plot = figure.querySelector(".plot");
    const readout = figure.querySelector(".readout");
    const cursor = figure.querySelector("line.cursor");
    if (!plot || !readout || !points || points.length === 0) return;
    const title = (figure.querySelector(".ctitle") || {}).textContent || "the chart";
    plot.tabIndex = 0;
    plot.setAttribute("role", "slider");
    plot.setAttribute("aria-label", title + ": the arrow keys step through its moments");
    plot.setAttribute("aria-valuemin", "0");
    plot.setAttribute("aria-valuemax", String(points.length - 1));
    let marks = [];
    try {
      marks = JSON.parse(figure.dataset.marks || "[]") || [];
    } catch {}
    let at = -1;
    const group = figure.closest(".charts");
    const peers = () => (group && groups.get(group)) || [];
    const show = (i, quiet) => {
      at = Math.max(0, Math.min(points.length - 1, i));
      const [x, when, ...values] = points[at];
      const near = marks.filter((m) => Math.abs(m.X - x) <= 12).map((m) => m.Text);
      readout.textContent = when + " UTC · " + values.join(" · ") + (near.length ? " · " + near.join(", ") : "");
      readout.classList.add("at");
      plot.setAttribute("aria-valuenow", String(at));
      plot.setAttribute("aria-valuetext", readout.textContent);
      if (cursor) {
        cursor.setAttribute("x1", x);
        cursor.setAttribute("x2", x);
        cursor.setAttribute("visibility", "visible");
      }
      if (!quiet) for (const peer of peers()) if (peer.figure !== figure) peer.atX(x);
    };
    const rest = (quiet) => {
      at = -1;
      readout.textContent = readout.dataset.rest || "";
      readout.classList.remove("at");
      if (cursor) cursor.setAttribute("visibility", "hidden");
      if (!quiet) for (const peer of peers()) if (peer.figure !== figure) peer.rest(true);
    };
    // The point nearest a place along the plot, from 0 to 1000.
    const nearest = (x) => {
      let best = 0;
      for (let i = 1; i < points.length; i++) if (Math.abs(points[i][0] - x) < Math.abs(points[best][0] - x)) best = i;
      return best;
    };
    const fromEvent = (event) => {
      const box = plot.getBoundingClientRect();
      if (box.width === 0) return;
      show(nearest(((event.clientX - box.left) / box.width) * 1000));
    };
    if (group) {
      if (!groups.has(group)) groups.set(group, []);
      groups.get(group).push({ figure, atX: (x) => show(nearest(x), true), rest: (quiet) => rest(quiet) });
    }
    plot.addEventListener("pointermove", fromEvent);
    plot.addEventListener("pointerdown", fromEvent);
    plot.addEventListener("pointerleave", (event) => { if (event.pointerType === "mouse" && document.activeElement !== plot) rest(); });
    plot.addEventListener("blur", () => rest());
    plot.addEventListener("keydown", (event) => {
      const step = { ArrowLeft: -1, ArrowRight: 1, PageUp: -12, PageDown: 12 }[event.key];
      if (step) {
        event.preventDefault();
        show(at < 0 ? points.length - 1 : at + step);
      } else if (event.key === "Home") {
        event.preventDefault();
        show(0);
      } else if (event.key === "End") {
        event.preventDefault();
        show(points.length - 1);
      } else if (event.key === "Escape") {
        rest();
      }
    });
  };
  const all = () => document.querySelectorAll("figure.chart[data-points]").forEach(arm);
  all();
  // The page following the cluster puts new charts in.
  const live = document.getElementById("live");
  if (live) new MutationObserver(all).observe(live, { childList: true, subtree: true });
})();
