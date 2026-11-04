// Murder panel: the page follows the cluster. Every few seconds it asks
// for itself again and puts in the new content, and says how fresh what
// it shows is: updated a moment ago, paused while someone is at work on it
// (typing, a choice made, a menu open, text selected, a form sent), or that
// the node it asks does not answer, so what it shows is old. A page that
// holds something being edited (an app's spec, settings) puts in only its
// parts marked data-live; logs hold still unless followed.
"use strict";
(() => {
  const live = document.getElementById("live");
  if (!live) return;
  const state = document.getElementById("livestate");
  const text = state && state.querySelector(".livetext");
  const node = (state && state.dataset.node) || "the node";
  const every = 5000;
  // When what the page shows was read, by this browser's clock.
  let readAt = Date.now();
  let failures = 0, lostWhy = "", stopped = "", timer = 0, asking = false;

  // A page holding an edit (an app's spec, settings, logs) puts in only
  // its parts marked data-live, so the edit is never replaced.
  const partial = () => !!document.querySelector("[data-still]");
  const selectChanged = (select) => {
    const initial = [...select.options].findIndex((option) => option.defaultSelected);
    return select.selectedIndex !== (initial < 0 ? 0 : initial);
  };
  const editing = () => {
    const active = document.activeElement;
    if (active && active.closest("#main form") && active.matches("input, select, textarea")) return true;
    return [...document.querySelectorAll("#main input[type=text], #main input[type=number], #main input[type=search], #main textarea")].some((input) => input.value !== input.defaultValue) ||
      [...document.querySelectorAll("#main input[type=file]")].some((input) => input.files.length > 0) ||
      [...document.querySelectorAll("#main select")].some(selectChanged);
  };
  // Why the page does not follow the cluster now; "" if it does.
  const pausedBy = () => {
    if (document.querySelector(".once")) return "Paused while a secret shows";
    if (document.querySelector("button[data-label]")) return "Paused while your action runs";
    // A menu (a disclosure that pops over the page) holds the page still;
    // an open disclosure of text does not: its part is not replaced.
    if (document.querySelector("#main details[open]:has(> .pop), #main details[open].drop, #main details[open].menu-text, #main details[open].rowmore, .side details[open]")) return "Paused while a menu is open";
    if (document.querySelector("details.event[open]")) return "Paused while an event is open";
    if (document.querySelector(".readout.at")) return "Paused while you read a chart";
    if (partial()) return "";
    if (editing()) return "Paused while you edit";
    if (String(getSelection()) !== "") return "Paused while text is selected";
    return "";
  };

  const age = (ms) => {
    const s = Math.max(0, Math.round(ms / 1000));
    if (s < 60) return s + "s";
    if (s < 3600) return Math.floor(s / 60) + "m";
    return Math.floor(s / 3600) + "h";
  };
  const clock = (at) => new Date(at).toISOString().slice(11, 19) + " UTC";

  // The banner that says the page shows old data, at the top of the page.
  const others = (state && state.dataset.others) || "";
  const signIn = () => {
    const link = document.createElement("a");
    link.href = "/login?next=" + encodeURIComponent(location.pathname + location.search);
    link.textContent = "Sign in again";
    return link;
  };
  // The banner that says the page shows old data, at the top of the page:
  // the node is lost, the session ended, or the page is gone.
  const banner = () => {
    let bar = document.getElementById("stale-banner");
    const main = document.getElementById("main");
    if (!lostWhy && !stopped) {
      if (bar) bar.remove();
      document.body.classList.remove("stale");
      return;
    }
    document.body.classList.add("stale");
    for (const count of document.querySelectorAll(".side .count, .navmenu .count")) count.title = "as of " + clock(readAt);
    // The node it cannot reach is not said to be up: no one knows.
    if (lostWhy) {
      for (const cell of document.querySelectorAll('[data-node-state="' + CSS.escape(node) + '"]')) {
        cell.className = "st mute";
        cell.textContent = "not known since " + clock(readAt).slice(0, 5) + " UTC";
      }
    }
    if (!bar && main) {
      bar = document.createElement("div");
      bar.id = "stale-banner";
      bar.className = "notice bad stale-banner";
      bar.setAttribute("role", "alert");
      main.prepend(bar);
    }
    if (!bar) return;
    bar.replaceChildren();
    const lead = document.createElement("span");
    lead.className = "lead-in";
    const since = " This page shows what it said at " + clock(readAt) + ", " + age(Date.now() - readAt) + " ago: greyed values may be out of date. ";
    if (stopped === "signed out") {
      lead.textContent = "You are signed out. ";
      bar.append(lead, document.createTextNode("Your session ended, so the page no longer follows the cluster, and its buttons do nothing until you sign in." + since), signIn());
      return;
    }
    if (stopped) {
      lead.textContent = "This page is gone. ";
      bar.append(lead, document.createTextNode("What it showed was purged, renamed or ended." + since));
      const home = document.createElement("a");
      home.href = "/";
      home.textContent = "Open the overview";
      bar.append(home);
      return;
    }
    lead.textContent = "Can't reach " + node + " since " + clock(readAt) + ". ";
    bar.append(lead, document.createTextNode(lostWhy + " Values below are from then, greyed. " + (others ? others + " still answered: reload, and the panel's address takes you to a node that answers. " : "")));
    const retry = document.createElement("button");
    retry.type = "button";
    retry.className = "link";
    retry.textContent = "Retry";
    retry.addEventListener("click", () => tick(true));
    bar.append(retry);
  };

  const say = () => {
    if (!state || !text) return;
    state.classList.remove("on", "paused", "lost", "off");
    text.replaceChildren();
    if (stopped) {
      state.classList.add("off");
      if (stopped === "signed out") {
        text.append("Signed out. ", signIn());
      } else {
        text.textContent = stopped;
      }
      return;
    }
    if (lostWhy) {
      state.classList.add("lost");
      text.textContent = "Offline " + clock(readAt).slice(0, 5) + " ";
      state.title = "Can't reach " + node + " since " + clock(readAt);
      const retry = document.createElement("button");
      retry.type = "button";
      retry.className = "link";
      retry.textContent = "Retry";
      retry.addEventListener("click", () => tick(true));
      text.append(retry);
      return;
    }
    // Paused says why where it is seen: what holds the page still.
    const paused = pausedBy();
    if (paused) {
      state.classList.add("paused");
      // Two lines: "Paused · 3s", then why, quieter.
      const why = document.createElement("span");
      why.className = "why";
      why.textContent = paused.replace(/^Paused /, "");
      text.append("Paused · " + age(Date.now() - readAt), why);
      state.title = paused + ", read " + age(Date.now() - readAt) + " ago";
      return;
    }
    state.classList.add("on");
    // A page that is a form to fill (a review, settings) does not follow
    // the cluster by design: it says so, not that it was paused.
    if (partial() && !document.querySelector("[data-live]")) {
      state.classList.remove("on");
      state.classList.add("paused");
      text.textContent = "Not live: a form";
      state.title = "This page holds a form and does not follow the cluster; it was read " + age(Date.now() - readAt) + " ago";
      return;
    }
    text.textContent = "Live · " + age(Date.now() - readAt);
    state.title = "Live: updated " + age(Date.now() - readAt) + " ago";
  };

  const stop = (why) => {
    stopped = why;
    clearInterval(timer);
    // Signed out, what the page offers would fail: its buttons wait.
    if (why === "signed out") {
      for (const button of document.querySelectorAll("#main form[method=post] button, .pagehead form button")) {
        button.disabled = true;
        button.title = "Sign in again first";
      }
    }
    banner();
    say();
  };

  // Puts the fresh page in: all of it, or only its live parts on a page
  // that holds an edit.
  const put = (page) => {
    for (const id of ["menu", "navmenu"]) {
      const fresh = page.getElementById(id), old = document.getElementById(id);
      if (!fresh || !old) continue;
      if (id === "navmenu") {
        const summary = fresh.querySelector("summary"), list = fresh.querySelector(".navlist");
        if (summary) old.querySelector("summary").replaceWith(summary);
        if (list && !old.open) old.querySelector(".navlist").replaceWith(list);
      } else {
        old.replaceChildren(...fresh.childNodes);
      }
    }
    document.title = page.title;
    const fresh = page.getElementById("live");
    if (!fresh) return;
    if (document.querySelector("[data-still]")) {
      // Lines followed stay at their end, unless scrolled back.
      const lines = document.querySelector("[data-follow]");
      const atEnd = !lines || lines.scrollTop + lines.clientHeight >= lines.scrollHeight - 24;
      for (const part of document.querySelectorAll("[data-live][id]")) {
        const next = fresh.querySelector("#" + CSS.escape(part.id));
        if (next && !part.contains(document.activeElement) && !part.querySelector("details[open], button[data-label]")) part.replaceWith(next);
      }
      const followed = document.querySelector("[data-follow]");
      if (followed && atEnd) followed.scrollTop = followed.scrollHeight;
      return;
    }
    // What an action did is shown once, not again.
    for (const notice of fresh.querySelectorAll(".notice")) notice.remove();
    const into = fresh.querySelector("main") || fresh;
    for (const notice of [...live.querySelectorAll(".notice:not(.stale-banner)")].reverse()) into.prepend(notice);
    // Events left open stay open.
    const open = new Set([...live.querySelectorAll("details.event[open]")].map((d) => d.dataset.id));
    live.replaceChildren(...fresh.childNodes);
    for (const d of live.querySelectorAll("details.event")) if (open.has(d.dataset.id)) d.open = true;
  };

  const tick = async (now) => {
    if (stopped || asking || document.hidden) return;
    if (!now && pausedBy()) return say();
    asking = true;
    const abort = new AbortController();
    const late = setTimeout(() => abort.abort(), 8000);
    try {
      const response = await fetch(location.href, { credentials: "same-origin", headers: { "X-Murder-Live": "1" }, signal: abort.signal, cache: "no-store" });
      if (response.redirected || response.status === 401) return stop("signed out");
      if (response.status === 404) return stop("This page is gone");
      const page = new DOMParser().parseFromString(await response.text(), "text/html");
      if (!response.ok) {
        const said = page.querySelector("h1");
        throw new Error(said ? said.textContent.trim() + "." : "It answered " + response.status + ".");
      }
      if (!now && pausedBy()) return;
      put(page);
      readAt = Date.now();
      failures = 0;
      lostWhy = "";
    } catch (error) {
      failures++;
      // One slow answer is not yet a lost node.
      if (failures >= 2 || now) lostWhy = error.name === "AbortError" ? "It did not answer within 8 seconds." : (error.message && error.message !== "Failed to fetch" ? error.message : "The connection failed.");
    } finally {
      clearTimeout(late);
      asking = false;
      banner();
      say();
    }
  };
  window.murderLive = { tick: () => tick(true) };

  timer = setInterval(tick, every);
  setInterval(() => { say(); if (lostWhy) banner(); }, 1000);
  document.addEventListener("visibilitychange", () => { if (!document.hidden) tick(); });
  say();

  // A menu closes on a click elsewhere, or Escape, as menus do.
  const menus = () => document.querySelectorAll("details[open]:has(> .pop), details.navmenu[open]");
  document.addEventListener("click", (event) => {
    for (const menu of menus()) if (!menu.contains(event.target)) menu.open = false;
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") for (const menu of menus()) menu.open = false;
  });
})();
