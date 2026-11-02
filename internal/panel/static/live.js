// Murder panel: the page follows the cluster. Every few seconds it asks
// for itself again and puts in the new content, unless someone is at
// work on it (typing, a choice made, a file chosen, text selected), or it shows
// something once (a secret), or it holds still (logs), or is hidden.
"use strict";
(() => {
  const main = document.getElementById("live");
  if (!main) return;
  const indicator = document.querySelector(".live");
  const busy = () =>
    document.hidden ||
    document.querySelector(".once, [data-still]") ||
    (document.activeElement && document.activeElement.closest("form")) ||
    [...document.querySelectorAll("input[type=text]")].some((input) => input.value !== "") ||
    [...document.querySelectorAll("input[type=file]")].some((input) => input.files.length > 0) ||
    [...document.querySelectorAll("select")].some((select) => select.selectedIndex > 0 && !select.options[select.selectedIndex].defaultSelected) ||
    String(getSelection()) !== "";
  let timer;
  const tick = async () => {
    if (busy()) return;
    try {
      const response = await fetch(location.href, { credentials: "same-origin", headers: { "X-Murder-Live": "1" } });
      // Signed out, or the session ended: the page stops following.
      if (response.redirected || response.status === 401) {
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
      for (const notice of [...main.querySelectorAll(".notice")].reverse()) fresh.prepend(notice);
      main.replaceChildren(...fresh.childNodes);
      const meta = page.querySelector(".meta");
      if (meta) document.querySelector(".meta").replaceWith(meta);
    } catch {
      // The next tick tries again.
    }
  };
  timer = setInterval(tick, 5000);
})();
