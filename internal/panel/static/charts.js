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
    const annot = figure.querySelector(".annot");
    const lane = figure.querySelector("svg.lane");
    // The plot's thousandths a width in pixels takes.
    const units = (px) => {
      const box = plot.getBoundingClientRect();
      return box.width > 0 ? (px * 1000) / box.width : px;
    };
    // The mark nearest x, within 12 px, is said under the plot in one
    // line of fixed height: its time as a link to its event, then what it
    // was. Others near it are counted, and listed whole on asking.
    const say = (x) => {
      if (!annot) return;
      const near = marks.filter((m) => Math.abs(m.X - x) <= units(12)).sort((a, b) => Math.abs(a.X - x) - Math.abs(b.X - x));
      annot.textContent = "";
      if (near.length === 0) return;
      const item = (m) => {
        const link = document.createElement("a");
        link.href = m.Href;
        link.textContent = m.Label;
        link.title = m.Text;
        return link;
      };
      const desc = document.createElement("span");
      desc.className = "desc";
      desc.textContent = ": " + near[0].Desc;
      desc.title = near[0].Text;
      annot.append(item(near[0]), desc);
      if (near.length > 1) {
        const more = document.createElement("details");
        more.className = "nearby";
        const summary = document.createElement("summary");
        summary.textContent = (near.length - 1) + " nearby";
        const list = document.createElement("ul");
        for (const m of near) {
          const li = document.createElement("li");
          li.append(item(m), ": " + m.Desc);
          list.append(li);
        }
        more.append(summary, list);
        annot.append(more);
      }
    };
    const show = (i, quiet) => {
      at = Math.max(0, Math.min(points.length - 1, i));
      const [x, when, ...values] = points[at];
      readout.textContent = when + " UTC · " + values.join(" · ");
      // The chart read says its marks; those following it, none.
      if (quiet) {
        if (annot) annot.textContent = "";
      } else say(x);
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
      if (annot) annot.textContent = "";
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
    // A mark's touch area is 24 px wide, kept within the plot; a tap on the
    // lane reads the moment there, and says the marks near it.
    if (lane) {
      const widen = () => {
        const w = units(24);
        for (const hit of lane.querySelectorAll("rect.hit")) {
          const x = Number(hit.nextElementSibling.getAttribute("x"));
          hit.setAttribute("width", String(w));
          hit.setAttribute("x", String(Math.max(0, Math.min(1000 - w, x - w / 2))));
        }
      };
      widen();
      new ResizeObserver(widen).observe(plot);
      lane.addEventListener("click", (event) => {
        // A mouse, or the keyboard (Enter: no pointer), opens the event; a
        // touch reads the moment and says the events near it.
        if (event.target.closest("a") && (event.pointerType === "mouse" || event.detail === 0)) return;
        event.preventDefault();
        fromEvent(event);
        pin(true);
      });
      // One lane in a group is reached by the keyboard: the others repeat it.
      if (group && peers().length > 0 && peers()[0].figure !== figure) {
        lane.setAttribute("aria-hidden", "true");
        for (const link of lane.querySelectorAll("a")) link.setAttribute("tabindex", "-1");
      }
    }
    // The reading stays while the pointer is anywhere in the figure (its
    // plot, lane, readout line and nearby list), so its links can be
    // reached; it goes as the pointer leaves the figure. A click or tap
    // pins it until Escape, or a click elsewhere.
    let pinned = false;
    const pin = (on) => {
      pinned = on;
      figure.classList.toggle("pinned", on);
    };
    plot.addEventListener("pointermove", (event) => { if (!pinned) fromEvent(event); });
    plot.addEventListener("pointerdown", (event) => { fromEvent(event); pin(true); });
    figure.addEventListener("pointerleave", (event) => {
      if (event.pointerType === "mouse" && !pinned && !figure.contains(document.activeElement)) rest();
    });
    document.addEventListener("pointerdown", (event) => {
      if (pinned && !figure.contains(event.target)) { pin(false); rest(); }
    });
    figure.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && at >= 0) { pin(false); rest(); }
    });
    // Focus moving to the chart's own links keeps what is read.
    plot.addEventListener("blur", (event) => { if (!figure.contains(event.relatedTarget)) rest(); });
    figure.addEventListener("focusout", (event) => {
      if (event.target !== plot && at >= 0 && !figure.contains(event.relatedTarget)) rest();
    });
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
      }
    });
  };
  const all = () => document.querySelectorAll("figure.chart[data-points]").forEach(arm);
  all();
  // The page following the cluster puts new charts in.
  const live = document.getElementById("live");
  if (live) new MutationObserver(all).observe(live, { childList: true, subtree: true });
})();
