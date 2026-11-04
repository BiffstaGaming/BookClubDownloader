/* Shared page behaviour for the desktop and phone UIs (loaded by base.html and mobile/base.html). */
(function () {
  // ── loading bar: counts requests you started; background polling is quiet ──
  // The mark goes on the request itself (not the element), because the element
  // is often replaced by its own response before htmx reports the request done.
  var inFlight = 0, bar = document.getElementById('global-indicator');
  document.body.addEventListener('htmx:beforeRequest', function (evt) {
    var elt = evt.detail.elt;
    if (elt && elt.hasAttribute && elt.hasAttribute('data-bc-quiet')) return;
    evt.detail.xhr.__bcCounted = true;
    inFlight++;
    bar.style.display = 'flex';
  });
  document.body.addEventListener('htmx:afterRequest', function (evt) {
    var xhr = evt.detail.xhr;
    if (!xhr || !xhr.__bcCounted) return;
    xhr.__bcCounted = false;
    inFlight = Math.max(0, inFlight - 1);
    if (!inFlight) bar.style.display = 'none';
  });

  // ── failed requests: say so instead of silently doing nothing ──
  var lastError = 0;
  function showError(text) {
    if (Date.now() - lastError < 3000) return;  // one message per burst (e.g. while restarting)
    lastError = Date.now();
    var box = document.getElementById('bc-errors');
    var el = document.createElement('div');
    el.className = 'alert alert-danger alert-dismissible d-flex align-items-start shadow';
    el.setAttribute('role', 'alert');
    el.innerHTML = '<i class="bi bi-exclamation-triangle-fill me-2 mt-1"></i><div class="flex-grow-1"></div>' +
      '<button type="button" class="btn-close btn-close-sm ms-2" data-bs-dismiss="alert" aria-label="Close"></button>';
    el.querySelector('.flex-grow-1').textContent = text;
    box.appendChild(el);
    setTimeout(function () { el.remove(); }, 15000);
  }
  document.body.addEventListener('htmx:responseError', function (evt) {
    if (evt.detail.elt && evt.detail.elt.hasAttribute && evt.detail.elt.hasAttribute('data-bc-quiet')) return;
    showError('Something went wrong (error ' + evt.detail.xhr.status + ') — the details are on the Logs page.');
  });
  document.body.addEventListener('htmx:sendError', function () {
    showError('BookClub is not reachable — it may be restarting. Try again in a moment.');
  });

  // A result reported below a long list (e.g. "Send to NZBGet"): bring it into view.
  document.body.addEventListener('htmx:afterSwap', function (evt) {
    if (evt.detail.target && evt.detail.target.id === 'download-status' && evt.detail.target.textContent.trim()) {
      evt.detail.target.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }
  });

  // ── alerts: success/info fade after a few seconds; every alert can be closed ──
  function tidyAlerts(root) {
    (root || document).querySelectorAll('.alert[data-autohide]:not([data-bc-timer])').forEach(function (el) {
      el.setAttribute('data-bc-timer', '1');
      setTimeout(function () {
        el.style.transition = 'opacity .4s';
        el.style.opacity = '0';
        setTimeout(function () {
          var panel = el.closest('[data-bc-clear-when-empty]');
          el.remove();
          if (panel && !panel.textContent.trim()) panel.innerHTML = '';
        }, 450);
      }, 6000);
    });
  }

  // ── times: <time class="bc-time" datetime="...Z"> shown in this browser's timezone ──
  var fmt = new Intl.DateTimeFormat(undefined, {
    year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit'
  });
  var fmtSeconds = new Intl.DateTimeFormat(undefined, {
    year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit'
  });
  function localTimes(root) {
    (root || document).querySelectorAll('time.bc-time:not([data-bc-done])').forEach(function (el) {
      var d = new Date(el.getAttribute('datetime'));
      var f = el.hasAttribute('data-bc-seconds') ? fmtSeconds : fmt;
      if (!isNaN(d)) { el.textContent = f.format(d); el.title = d.toString(); }
      el.setAttribute('data-bc-done', '1');
    });
  }

  function refresh(root) { tidyAlerts(root); localTimes(root); }
  document.addEventListener('DOMContentLoaded', function () { refresh(); });
  document.body.addEventListener('htmx:afterSettle', function (evt) { refresh(evt.detail.elt); });
  refresh();

  // ── highlight the current page in the nav ──
  var path = location.pathname.replace(/\/$/, '') || '/';
  document.querySelectorAll('.navbar .nav-link, .m-tab').forEach(function (a) {
    var href = a.getAttribute('href');
    var isRoot = href === '/' || href === '/m';  // home pages only match themselves
    if (href === path || (!isRoot && path.indexOf(href + '/') === 0)) a.classList.add('active');
  });

  // Phone UI: a full-screen sheet (.m-sheet) closes with Android's Back button
  // instead of leaving the page; closing it with its X drops the extra history entry.
  document.querySelectorAll('.m-sheet').forEach(function (sheet) {
    new MutationObserver(function () {
      var open = !!sheet.innerHTML.trim(), pushed = history.state && history.state.sheet === sheet.id;
      if (open && !pushed) history.pushState({ sheet: sheet.id }, '');
      if (!open && pushed) history.back();
    }).observe(sheet, { childList: true });
  });
  window.addEventListener('popstate', function () {
    document.querySelectorAll('.m-sheet').forEach(function (sheet) { sheet.innerHTML = ''; });
  });

  // Small persistence helpers used by the pages
  window.bcStore = {
    get: function (key, fallback) {
      try { var v = localStorage.getItem('bc:' + key); return v === null ? fallback : JSON.parse(v); }
      catch (e) { return fallback; }
    },
    set: function (key, value) { try { localStorage.setItem('bc:' + key, JSON.stringify(value)); } catch (e) {} },
    del: function (key) { try { localStorage.removeItem('bc:' + key); } catch (e) {} }
  };
})();
