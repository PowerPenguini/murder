// Murder panel: the page follows the cluster. Every few seconds it asks
// for itself again and puts in the new content, unless someone is at
// work on it (typing, a choice made, a file chosen, a menu open, text
// selected), or it shows something once (a secret), or it holds still
// (logs), or is hidden.
"use strict";
(() => {
  const main = document.getElementById("live");
  if (!main) return;
  const indicator = document.querySelector(".live");
  const busy = () =>
    document.hidden ||
    document.querySelector(".once, [data-still], details[open]") ||
    (document.activeElement && document.activeElement.closest("form")) ||
    [...document.querySelectorAll("input[type=text], input[type=search], input[type=number]")].some((input) => input.value !== "" && input.defaultValue !== input.value) ||
    [...document.querySelectorAll("input[type=file]")].some((input) => input.files.length > 0) ||
    [...document.querySelectorAll("select")].some((select) => select.selectedIndex > 0 && !select.options[select.selectedIndex].defaultSelected) ||
    String(getSelection()) !== "";
  let timer;
  const tick = async () => {
    if (busy()) return;
    try {
      const response = await fetch(location.href, { credentials: "same-origin", headers: { "X-Murder-Live": "1" } });
      // Signed out, the session ended, or what the page shows is gone (a
      // job of a panel that restarted, a purged tenant): it stops following,
      // keeping what it showed last.
      if (response.redirected || response.status === 401 || response.status === 404) {
        clearInterval(timer);
        if (indicator) indicator.classList.add("off");
        return;
      }
      if (!response.ok) return;
      const page = new DOMParser().parseFromString(await response.text(), "text/html");
      const fresh = page.getElementById("live");
      if (!fresh || busy()) return;
      // What an action did is shown once, not again.
      for (const notice of fresh.querySelectorAll(".notice")) notice.remove();
      const into = fresh.querySelector("main") || fresh;
      for (const notice of [...main.querySelectorAll(".notice")].reverse()) into.prepend(notice);
      main.replaceChildren(...fresh.childNodes);
    } catch {
      // The next tick tries again.
    }
  };
  timer = setInterval(tick, 5000);
  // A menu closes on a click elsewhere, or Escape, as menus do.
  const menus = () => document.querySelectorAll("details[open]:has(> .pop)");
  document.addEventListener("click", (event) => {
    for (const menu of menus()) if (!menu.contains(event.target)) menu.open = false;
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") for (const menu of menus()) menu.open = false;
  });
})();
