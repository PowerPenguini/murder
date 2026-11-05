// Murder panel: forms that help. A dangerous action's button waits for
// the name typed; a form sent once is not sent again while it works; a
// form that opens puts the caret in it; a table can be filtered. The
// forms work without this script: the agent checks everything again.
"use strict";
(() => {
  // As the server compares: spaces around and the case of letters aside,
  // as a phone may capitalise the first letter. The sentence above the
  // field says what to type, once; a line under it speaks only when what
  // is typed cannot become the name ("atlass is not atlas").
  const confirmed = (input) => {
    const want = input.dataset.confirm;
    const typed = input.value.trim();
    const ok = typed.toLowerCase() === want.toLowerCase();
    // A field may belong to a form elsewhere on the page (form="...").
    const buttons = [...input.form.elements].filter((el) => el.matches("button[type=submit]:not([formnovalidate]), button:not([type])"));
    for (const button of buttons) button.disabled = !ok;
    const wrong = typed !== "" && !want.toLowerCase().startsWith(typed.toLowerCase());
    let hint = input.nextElementSibling;
    if (!hint || !hint.classList.contains("typed")) {
      hint = document.createElement("p");
      hint.className = "typed hint";
      hint.setAttribute("aria-live", "polite");
      input.after(hint);
    }
    hint.textContent = wrong ? typed + " is not " + want + "." : "";
    hint.hidden = !wrong;
    input.toggleAttribute("aria-invalid", wrong);
  };
  // A filter hides what does not match as it is typed, and says how many
  // do; sent, the server filters the same way.
  const filter = (input) => {
    const list = document.getElementById(input.dataset.filter);
    const query = input.value.trim().toLowerCase();
    if (!list) return;
    const rows = [...(list.tBodies ? list.tBodies[0].rows : list.children)].filter((row) => !row.querySelector(".empty"));
    let shown = 0;
    for (const row of rows) {
      row.hidden = query !== "" && !row.textContent.toLowerCase().includes(query);
      if (!row.hidden) shown++;
    }
    const count = document.getElementById(input.dataset.filter + "-count");
    if (count) {
      if (!count.dataset.said) count.dataset.said = count.innerHTML;
      if (query === "") count.innerHTML = count.dataset.said;
      else count.textContent = shown + " of " + rows.length + " match “" + input.value.trim() + "”";
    }
    const none = document.getElementById(input.dataset.filter + "-none");
    if (none) none.hidden = shown > 0 || rows.length === 0;
  };
  const arm = () => {
    for (const input of document.querySelectorAll("input[data-confirm]")) confirmed(input);
  };
  document.addEventListener("input", (event) => {
    const input = event.target;
    if (input.matches("input[data-confirm]")) confirmed(input);
    if (input.matches("input[data-filter]")) filter(input);
  });
  // Sent, the button says so and takes no second click; the browser
  // still sends it, as the button is disabled only after.
  document.addEventListener("submit", (event) => {
    if (event.defaultPrevented || event.target.method === "get") return;
    const button = event.submitter || event.target.querySelector("button[type=submit]");
    if (!button) return;
    setTimeout(() => {
      button.disabled = true;
      button.dataset.label = button.textContent;
      button.textContent = "Working…";
    }, 0);
  });
  // Back to a page the browser kept: its buttons work again.
  window.addEventListener("pageshow", () => {
    for (const button of document.querySelectorAll("button[data-label]")) {
      button.textContent = button.dataset.label;
      button.disabled = false;
      delete button.dataset.label;
    }
    arm();
  });
  // On a phone a menu opens as a sheet at the foot of the screen: it gets
  // a title and a Close button, the page behind it is inert while it is
  // open, so focus stays in it, and focus goes back to its button after.
  const phone = matchMedia("(max-width: 760px)");
  let sheet = null, inerted = [];
  const release = () => {
    for (const element of inerted) element.inert = false;
    inerted = [];
  };
  const asSheet = (details) => {
    const pop = details.querySelector(":scope > .pop");
    const summary = details.querySelector(":scope > summary");
    if (!pop || !summary) return;
    if (!pop.querySelector(":scope > .sheet-head")) {
      const head = document.createElement("div");
      head.className = "sheet-head";
      const title = document.createElement("span");
      title.className = "sheet-title";
      const said = summary.getAttribute("aria-label") || summary.textContent.trim();
      title.textContent = said.charAt(0).toUpperCase() + said.slice(1);
      const close = document.createElement("button");
      close.type = "button";
      close.className = "sheet-close";
      close.textContent = "Close";
      close.addEventListener("click", () => { details.open = false; });
      head.append(title, close);
      pop.prepend(head);
      pop.setAttribute("role", "dialog");
      pop.setAttribute("aria-label", title.textContent);
    }
    release();
    summary.inert = true;
    inerted.push(summary);
    for (let node = details; node && node !== document.body; node = node.parentElement) {
      for (const sibling of node.parentElement ? node.parentElement.children : []) {
        if (sibling !== node && !sibling.inert && sibling.tagName !== "SCRIPT") {
          sibling.inert = true;
          inerted.push(sibling);
        }
      }
    }
    sheet = details;
  };
  // On a desktop a pop-over is wholly on screen: below its button if it
  // fits, else above it, else the page scrolls just enough to show it.
  // Again when an action inside it opens and it grows.
  const place = (details) => {
    if (phone.matches || !details.open) return;
    const pop = details.querySelector(":scope > .pop");
    if (!pop) return;
    pop.classList.remove("up");
    const box = pop.getBoundingClientRect();
    if (box.bottom <= innerHeight) return;
    const above = details.querySelector(":scope > summary").getBoundingClientRect().top - 6;
    if (box.height <= above) pop.classList.add("up");
    else pop.scrollIntoView({ block: "nearest" });
  };
  document.addEventListener("toggle", (event) => {
    const details = event.target;
    if (!(details instanceof HTMLDetailsElement)) return;
    const outer = details.parentElement && details.parentElement.closest("details");
    if (details.open) place(details);
    if (outer && outer.querySelector(":scope > .pop") && outer.querySelector(":scope > .pop").contains(details)) place(outer);
    if (!details.open) {
      if (details === sheet) {
        release();
        sheet = null;
        const summary = details.querySelector(":scope > summary");
        if (summary) summary.focus();
      }
      return;
    }
    if (phone.matches && details.querySelector(":scope > .pop")) asSheet(details);
    // A menu of actions puts focus on its first; an action opened, on its
    // form's first field; any other menu, on its first field.
    const act = details.classList.contains("act");
    const pop = details.querySelector(":scope > .pop");
    if (!act && !pop) return;
    const field = act
      ? details.querySelector("form input:not([type=hidden]), form select, form textarea, form button")
      : pop.querySelector(":scope > .act > summary") || pop.querySelector("input:not([type=hidden]), select, textarea") || pop.querySelector("button:not(.sheet-close)");
    if (field) field.focus();
  }, true);
  // An Override turned on starts at the value inherited, selected, so one
  // keystroke replaces it; it never sends an empty override.
  document.addEventListener("change", (event) => {
    const box = event.target;
    if (!box.matches || !box.matches("input.own-switch") || !box.checked) return;
    const row = box.closest("tr");
    const field = row && row.querySelector(".own-field input:not([type=hidden])");
    if (!field) return;
    if (field.value === "" && field.dataset.inherit) {
      field.value = field.dataset.inherit;
      field.dispatchEvent(new Event("input", { bubbles: true }));
    }
    requestAnimationFrame(() => { field.focus(); if (field.select) field.select(); });
  });
  arm();
  // What is shown once can be copied with a button, which says it did.
  const armCopy = (root) => {
  for (const button of root.querySelectorAll("button[data-copy]")) {
    if (!navigator.clipboard || button.dataset.armed) continue;
    button.dataset.armed = "1";
    button.hidden = false;
    button.addEventListener("click", async () => {
      const source = document.getElementById(button.dataset.copy);
      if (!source) return;
      const label = button.textContent;
      try {
        await navigator.clipboard.writeText(source.textContent);
        button.textContent = "Copied";
      } catch {
        button.textContent = "Copy failed: select the text";
      }
      setTimeout(() => { button.textContent = label; }, 2400);
    });
  }
  };
  armCopy(document);
  // A page that comes back with a problem beside a field goes to it, and
  // puts the caret on the line it names.
  const problem = document.querySelector("[data-focus]");
  // Its landing runs once the page has loaded and the browser has jumped
  // to the address's #fragment, which would take the focus away again: the
  // field's table heading and its row in view, the caret in the field.
  const land = () => {
    const field = document.querySelector("textarea[data-line]");
    if (field) {
      const line = Number(field.dataset.line), lines = field.value.split("\n");
      const at = lines.slice(0, Math.max(0, line - 1)).join("\n").length + (line > 1 ? 1 : 0);
      field.focus();
      field.setSelectionRange(at, at + (lines[line - 1] || "").length);
      problem.scrollIntoView({ block: "center" });
      return;
    }
    const row = problem.closest("tr");
    const table = row && row.closest("table");
    const heading = table && table.previousElementSibling && table.previousElementSibling.matches(".head") ? table.previousElementSibling : null;
    if (heading) {
      heading.scrollIntoView({ block: "start" });
      if (row.getBoundingClientRect().bottom > innerHeight - 16) row.scrollIntoView({ block: "center" });
    } else {
      problem.scrollIntoView({ block: "center" });
    }
    problem.focus({ preventScroll: true });
    if (problem.select && problem.value) problem.select();
  };
  if (problem) {
    // The script lands it: the browser's own jump to the #fragment, which
    // may come later and take the focus, is left nothing to jump to. The
    // fragment stays in the link, for a page without this script.
    if (location.hash) history.replaceState(history.state, "", location.pathname + location.search);
    if (document.readyState === "complete") requestAnimationFrame(land);
    else addEventListener("load", () => requestAnimationFrame(land), { once: true });
  }
  // A form marked as a draft (a tenant's settings) is not lost by leaving
  // the page, or by sending another form of it, without a word: the page
  // asks first. One opened with a proposal counts as changed already.
  for (const form of document.querySelectorAll("form[data-draft-form]")) {
    const state = () => new URLSearchParams(new FormData(form)).toString();
    const initial = state();
    let sending = false;
    const changed = () => form.hasAttribute("data-proposed") || state() !== initial;
    form.addEventListener("submit", () => { sending = true; });
    addEventListener("beforeunload", (event) => {
      // Leaving on purpose (the draft discarded) asks nothing.
      if (!sending && !document.body.dataset.leaving && changed()) event.preventDefault();
    });
    document.addEventListener("submit", (event) => {
      if (event.target === form || !changed() || event.target.method === "get") return;
      if (!confirm("Your changes to the settings are not saved. Send this other form, and leave them?")) {
        event.preventDefault();
        event.stopImmediatePropagation();
      } else {
        sending = true;
      }
    }, true);
  }
  // A draft is not lost by leaving the page by mistake.
  for (const draft of document.querySelectorAll("[data-draft]")) {
    let sending = false;
    draft.form.addEventListener("submit", () => { sending = true; });
    addEventListener("beforeunload", (event) => {
      if (!sending && !document.body.dataset.leaving && draft.value !== draft.defaultValue) event.preventDefault();
    });
  }
  // "Add a variable" (or a secret) shows a row to add one in, and another
  // each time, without sending the form; the empty row waits hidden until
  // asked for. Without this script the server adds a row, the draft kept.
  // A row added is removed with its own button.
  for (const button of document.querySelectorAll("[data-add-row]")) {
    const table = document.getElementById(button.dataset.addRow);
    const body = table && table.tBodies[0];
    if (!body) continue;
    const rows = () => [...body.querySelectorAll("tr.add")];
    const empty = (row) => [...row.querySelectorAll("input")].every((input) => input.value === "");
    const arm = (row) => {
      const remove = row.querySelector(".remove-row");
      if (!remove) return;
      remove.hidden = false;
      remove.addEventListener("click", () => {
        // The form is found before the row leaves it: the draft kept for
        // the tab is saved again without the row.
        const form = row.closest("form") || table.closest("form");
        if (rows().filter((r) => !r.hidden).length > 1) row.remove();
        else {
          for (const input of row.querySelectorAll("input")) input.value = "";
          row.hidden = true;
        }
        if (form) form.dispatchEvent(new Event("input", { bubbles: true }));
        button.focus();
      });
    };
    for (const row of rows()) {
      arm(row);
      if (empty(row)) row.hidden = true;
    }
    button.addEventListener("click", (event) => {
      event.preventDefault();
      const all = rows();
      let row = all.find((r) => r.hidden);
      if (row) row.hidden = false;
      else {
        row = all[all.length - 1].cloneNode(true);
        for (const input of row.querySelectorAll("input")) input.value = "";
        all[all.length - 1].after(row);
        arm(row);
      }
      row.querySelector("input").focus();
    });
  }
  // A draft, a tenant's settings or an app's spec, is kept in this tab as
  // it is typed: should the session end, it comes back once signed in
  // again, said so, and is reviewed again before it is saved. A secret's
  // lines, typed names and the form's own fields are never kept. A page
  // seen without signing in again drops what was kept for it.
  const keep = (() => { try { return window.sessionStorage; } catch { return null; } })();
  const skip = new Set(["secrets", "confirm", "generation", "updated", "owns", "flags", "envrows", "do", "reviewed"]);
  const drafted = [...document.querySelectorAll("form[data-draft-form]"), ...[...document.querySelectorAll("[data-draft]")].map((d) => d.form)];
  if (keep && drafted.length) {
    const form = drafted[0];
    const key = "murder-draft:" + location.pathname;
    const stamp = () => (form.elements.generation || form.elements.updated || {}).value || "";
    // Each field is kept by its name and its key: a variable's or a
    // secret's own name where it has one (rows are sorted by name, so a
    // variable added meanwhile moves the others), else its place; with
    // what the server said for it then, to tell a change made since.
    const keyOf = (el, seen) => el.dataset.key || "#" + (seen[el.name] = (seen[el.name] ?? -1) + 1);
    const valueOf = (el) => (el.type === "checkbox" || el.type === "radio" ? el.checked : el.value);
    const baseOf = (el) => (el.type === "checkbox" || el.type === "radio" ? el.defaultChecked
      : el.tagName === "SELECT" ? ([...el.options].find((o) => o.defaultSelected) || el.options[0] || {}).value : el.defaultValue);
    const fields = () => {
      const seen = {};
      const out = [];
      for (const el of form.elements) {
        if (!el.name || skip.has(el.name) || el.type === "hidden" || el.type === "password" || el.type === "submit" || el.type === "button") continue;
        if (el.form !== form) continue;
        out.push([el.name, keyOf(el, seen), valueOf(el), baseOf(el)]);
      }
      return out;
    };
    const save = () => {
      try { keep.setItem(key, JSON.stringify({ at: Date.now(), stamp: stamp(), fields: fields() })); } catch {}
    };
    let resume = null;
    try {
      resume = keep.getItem("murder-resume");
      keep.removeItem("murder-resume");
    } catch {}
    let kept = null;
    try { kept = JSON.parse(keep.getItem(key) || "null"); } catch {}
    if (resume === location.pathname && kept && Date.now() - kept.at < 12 * 3600 * 1000) {
      // New rows the draft had are added before it is put back.
      // Rows the draft added, named or not, come back as rows.
      for (const [field, table] of [["envnewname", "env"], ["secretnewname", "secrets"]]) {
        // Up to the last that holds something, so each comes back in its place.
        const wanted = Math.max(0, ...kept.fields.filter(([name, key, value]) => name === field && key.startsWith("#") && value !== "").map(([, key]) => Number(key.slice(1)) + 1));
        const add = document.querySelector('[data-add-row="' + table + '"]');
        const shown = () => [...form.querySelectorAll('input[name="' + field + '"]')].filter((i) => !i.closest("tr").hidden).length;
        while (add && shown() < wanted) add.click();
      }
      const seen = {};
      const here = new Map();
      for (const el of form.elements) {
        if (!el.name || el.type === "hidden" || el.form !== form) continue;
        here.set(el.name + "\u0000" + keyOf(el, seen), el);
      }
      let changedSince = 0;
      for (const [name, key, value, base] of kept.fields) {
        const el = here.get(name + "\u0000" + key);
        if (!el) continue;
        if (base !== undefined && String(baseOf(el)) !== String(base)) {
          // The server's value changed since the draft began: said beside it.
          changedSince++;
          const note = document.createElement("span");
          note.className = "caption changed-since";
          note.textContent = "Changed since your draft began: now " + (el.type === "checkbox" ? (baseOf(el) ? "on" : "off") : (baseOf(el) || "empty"));
          (el.closest("td") || el.parentElement).append(note);
        }
        if (el.type === "checkbox" || el.type === "radio") el.checked = !!value;
        else el.value = value;
      }
      const at = new Date(kept.at).toISOString().slice(11, 16);
      const note = document.createElement("div");
      note.className = "notice";
      note.setAttribute("role", "status");
      note.textContent = "Restored your draft from " + at + " UTC, kept in this tab while you signed in again. Review it again before saving." +
        (kept.stamp !== stamp() ? " It changed since you began" + (changedSince ? ": " + changedSince + " " + (changedSince === 1 ? "value is" : "values are") + " marked where they changed" : "") + "; the review shows what your draft changes now." : "") + " ";
      const discard = document.createElement("button");
      discard.type = "button";
      discard.className = "link";
      discard.textContent = "Discard the draft";
      discard.addEventListener("click", () => {
        try { keep.removeItem(key); } catch {}
        document.body.dataset.leaving = "1";
        location.replace(location.pathname + location.search);
      });
      note.append(discard);
      form.prepend(note);
      form.setAttribute("data-proposed", "");
    } else {
      try { keep.removeItem(key); } catch {}
    }
    form.addEventListener("input", save);
    form.addEventListener("change", save);
  }
  // Logs open at their newest line, at the end.
  for (const lines of document.querySelectorAll("div.logs")) lines.scrollTop = lines.scrollHeight;
  // The page following the cluster puts new forms in: they wait too.
  const live = document.getElementById("live");
  if (live) new MutationObserver(arm).observe(live, { childList: true, subtree: false });
  // An event a link points to, from a chart's mark: its record opened,
  // shown below the sticky bar, and marked; on arrival and on a new link.
  const target = () => {
    const id = decodeURIComponent(location.hash.slice(1));
    if (!id.startsWith("ev-")) return;
    const el = document.getElementById(id);
    if (!el) return;
    for (const old of document.querySelectorAll(".target")) old.classList.remove("target");
    const details = el.closest("details.event");
    if (details) details.open = true;
    el.classList.add("target");
    el.scrollIntoView({ block: "start" });
  };
  target();
  window.addEventListener("hashchange", target);
  // A form that only chooses what the page shows (a review's pace) shows
  // it as soon as it is chosen; without this script, its button does.
  // A review's pace: choosing one (by click, or the arrow keys) shows its
  // plan in place, fetched from the review's own address, the radio keeping
  // the focus; the address follows, so a reload shows the same. Without
  // this script, "Show this plan" does.
  for (const form of document.querySelectorAll("form[data-pace]")) {
    for (const hide of form.querySelectorAll(".js-hide")) hide.hidden = true;
    let asked = 0;
    form.addEventListener("change", async (event) => {
      const pace = event.target.value;
      const url = form.action.split("#")[0] + "?" + new URLSearchParams(new FormData(form)).toString();
      const n = ++asked;
      let html;
      try {
        const response = await fetch(url, { credentials: "same-origin" });
        if (!response.ok || response.redirected) throw new Error(response.status);
        html = await response.text();
      } catch {
        location.href = url + "#pace";
        return;
      }
      if (n !== asked) return;
      const fresh = new DOMParser().parseFromString(html, "text/html").getElementById("main");
      const main = document.getElementById("main");
      if (!fresh || !main) {
        location.href = url + "#pace";
        return;
      }
      // Everything but the pace's own form, which keeps its focus.
      const keepForm = main.querySelector("form[data-pace]");
      const freshForm = fresh.querySelector("form[data-pace]");
      if (keepForm && freshForm) freshForm.replaceWith(keepForm);
      main.replaceChildren(...fresh.childNodes);
      history.replaceState(null, "", url + "#pace");
      armCopy(main);
      const radio = main.querySelector('input[name=pace][value="' + CSS.escape(pace) + '"]');
      if (radio) radio.focus();
    });
  }
  // How logs are shown (wrapped, full times) is kept for the tab, across
  // their filters, containers and following.
  for (const id of ["wrap", "fulltimes"]) {
    const box = document.getElementById(id);
    if (!box) continue;
    try {
      const kept = sessionStorage.getItem("murder-logs-" + id);
      if (kept !== null) box.checked = kept === "on";
    } catch {}
    box.addEventListener("change", () => {
      try { sessionStorage.setItem("murder-logs-" + id, box.checked ? "on" : "off"); } catch {}
    });
  }
})();
