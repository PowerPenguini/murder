// Murder panel: forms that help. A dangerous action's button waits for
// the name typed; a form sent once is not sent again while it works; a
// form that opens puts the caret in it; a table can be filtered. The
// forms work without this script: the agent checks everything again.
"use strict";
(() => {
  const confirmed = (input) => {
    const ok = input.value.trim() === input.dataset.confirm;
    for (const button of input.form.querySelectorAll("button[type=submit]")) button.disabled = !ok;
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
  document.addEventListener("toggle", (event) => {
    const details = event.target;
    if (!(details instanceof HTMLDetailsElement)) return;
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
    const field = details.querySelector(".pop input:not([type=hidden]), .pop select, .pop textarea") || details.querySelector(".pop button:not(.sheet-close)");
    if (field) field.focus();
  }, true);
  arm();
  // What is shown once can be copied with a button, which says it did.
  for (const button of document.querySelectorAll("button[data-copy]")) {
    if (!navigator.clipboard) continue;
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
  // A page that comes back with a problem beside a field goes to it, and
  // puts the caret on the line it names.
  const problem = document.querySelector("[data-focus]");
  if (problem) {
    const field = document.querySelector("textarea[data-line]");
    if (field) {
      const line = Number(field.dataset.line), lines = field.value.split("\n");
      const at = lines.slice(0, Math.max(0, line - 1)).join("\n").length + (line > 1 ? 1 : 0);
      field.focus();
      field.setSelectionRange(at, at + (lines[line - 1] || "").length);
    }
    problem.scrollIntoView({ block: "center" });
    if (!field) problem.focus();
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
      if (!sending && changed()) event.preventDefault();
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
      if (!sending && draft.value !== draft.defaultValue) event.preventDefault();
    });
  }
  // Logs open at their newest line, at the end.
  for (const lines of document.querySelectorAll("div.logs")) lines.scrollTop = lines.scrollHeight;
  // The page following the cluster puts new forms in: they wait too.
  const live = document.getElementById("live");
  if (live) new MutationObserver(arm).observe(live, { childList: true, subtree: false });
})();
