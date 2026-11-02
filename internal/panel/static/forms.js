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
  const arm = () => {
    for (const input of document.querySelectorAll("input[data-confirm]")) confirmed(input);
  };
  document.addEventListener("input", (event) => {
    const input = event.target;
    if (input.matches("input[data-confirm]")) confirmed(input);
    if (input.matches("input[data-filter]")) {
      const table = document.getElementById(input.dataset.filter);
      const query = input.value.trim().toLowerCase();
      if (!table) return;
      for (const row of table.tBodies[0].rows) {
        row.hidden = query !== "" && !row.textContent.toLowerCase().includes(query);
      }
    }
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
  document.addEventListener("toggle", (event) => {
    const details = event.target;
    if (!(details instanceof HTMLDetailsElement) || !details.open) return;
    const field = details.querySelector("input:not([type=hidden]), select, textarea");
    if (field) field.focus();
  }, true);
  arm();
  // The page following the cluster puts new forms in: they wait too.
  const live = document.getElementById("live");
  if (live) new MutationObserver(arm).observe(live, { childList: true, subtree: false });
})();
