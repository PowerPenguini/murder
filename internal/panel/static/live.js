// Murder panel: the page follows the cluster. Every few seconds it asks
// for itself again and puts in the new content, unless someone is typing,
// or the page shows something once (a secret), or the tab is hidden.
"use strict";
(() => {
  const main = document.getElementById("live");
  if (!main) return;
  const busy = () =>
    document.hidden ||
    document.querySelector(".once") ||
    (document.activeElement && document.activeElement.closest("form")) ||
    [...document.querySelectorAll("input[type=text]")].some((input) => input.value !== "");
  const tick = async () => {
    if (busy()) return;
    try {
      const response = await fetch(location.href, { credentials: "same-origin", headers: { "X-Murder-Live": "1" } });
      if (!response.ok || response.redirected) return;
      const page = new DOMParser().parseFromString(await response.text(), "text/html");
      const fresh = page.getElementById("live");
      if (!fresh || busy()) return;
      // What an action did is shown once, not again.
      for (const notice of fresh.querySelectorAll(".notice")) notice.remove();
      for (const notice of main.querySelectorAll(".notice")) fresh.prepend(notice);
      main.replaceChildren(...fresh.childNodes);
      const meta = page.querySelector(".meta");
      if (meta) document.querySelector(".meta").replaceWith(meta);
    } catch {
      // The next tick tries again.
    }
  };
  setInterval(tick, 5000);
})();
