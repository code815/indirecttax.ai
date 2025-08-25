(function () {
  function el(tag, attrs, ...children) {
    const node = document.createElement(tag);
    if (attrs) Object.entries(attrs).forEach(([k, v]) => node.setAttribute(k, v));
    for (const c of children) node.append(c);
    return node;
  }

  async function fetchChanges() {
    const base = (window.CONFIG && window.CONFIG.API_BASE_URL) || "";
    const url = `${base.replace(/\/+$/, "")}/changes?page=1&page_size=20`;
    const res = await fetch(url, { headers: { "Accept": "application/json" } });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
  }

  // Legacy-compatible name
  window.loadDashBulletins = async function loadDashBulletins() {
    const root = document.getElementById("bulletins");
    root.innerHTML = "";
    const status = el("div", { id: "status" }, "Loading…");
    root.append(status);
    try {
      const data = await fetchChanges();
      status.remove();
      const list = Array.isArray(data?.items) ? data.items : (Array.isArray(data) ? data : []);
      if (list.length === 0) {
        root.append(el("div", { class: "empty" }, "No bulletins yet."));
        return;
      }
      const ul = el("ul");
      list.slice(0, 20).forEach((x) => {
        const li = el("li", null,
          el("div", { class: "title" }, x.title || x.id || "Untitled"),
          el("div", { class: "meta" }, (x.source || x.url || ""), " ", (x.created_at || x.timestamp || ""))
        );
        ul.append(li);
      });
      root.append(ul);
    } catch (err) {
      status.textContent = `Failed to load: ${err.message}`;
    }
  };

  window.addEventListener("DOMContentLoaded", () => {
    if (typeof window.loadDashBulletins === "function") window.loadDashBulletins();
  });
})();
