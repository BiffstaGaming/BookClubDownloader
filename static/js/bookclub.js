/* Shared page behaviour for the desktop and phone UIs (loaded by base.html and mobile/base.html). */
(function () {
  // ── loading bar: counts requests you started; background polling is quiet ──
  // The mark goes on the request itself (not the element), because the element
  // is often replaced by its own response before htmx reports the request done.
  var inFlight = 0, bar = document.getElementById('global-indicator');
  document.body.addEventListener('htmx:beforeRequest', function (evt) {
    var elt = evt.detail.elt;
    if (elt && elt.hasAttribute && elt.hasAttribute('data-bc-quiet')) return;
    var xhr = evt.detail.xhr;
    xhr.__bcCounted = true;
    inFlight++;
    bar.style.display = 'flex';
    // Safety net: never leave the bar up for a request htmx didn't report back on.
    setTimeout(function () { if (xhr.__bcCounted) finish(xhr); }, 60000);
  });
  function finish(xhr) {
    xhr.__bcCounted = false;
    inFlight = Math.max(0, inFlight - 1);
    if (!inFlight) bar.style.display = 'none';
  }
  document.body.addEventListener('htmx:afterRequest', function (evt) {
    var xhr = evt.detail.xhr;
    if (!xhr || !xhr.__bcCounted) return;
    finish(xhr);
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

  // New content in a panel (e.g. "Saved." after Save, or the Audible list): show it from
  // the top. Without this you stay scrolled down at the buttons and the message is out of view.
  document.body.addEventListener('htmx:afterSwap', function (evt) {
    var t = evt.detail.target;
    if (!t || !t.innerHTML.trim()) return;
    if (t.classList.contains('m-sheet')) {
      t.scrollTop = 0;
    } else if (t.id === 'convert-panel' || t.id === 'saved-panel') {
      var top = t.getBoundingClientRect().top;
      if (top < 0 || top > window.innerHeight) t.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
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

  // Sections marked <details id="..." data-bc-keep-open> stay open when the list
  // around them refreshes (e.g. "Show more" in the requests list every 10 s).
  var keptOpen = new Set();
  document.body.addEventListener('htmx:beforeSwap', function () {
    document.querySelectorAll('details[data-bc-keep-open][id]').forEach(function (d) {
      if (d.open) keptOpen.add(d.id); else keptOpen.delete(d.id);
    });
  });
  document.body.addEventListener('htmx:afterSettle', function () {
    keptOpen.forEach(function (id) {
      var d = document.getElementById(id);
      if (d) d.open = true;
    });
  });

  // ── live updates ─────────────────────────────────────────────────────────
  // The server says what changed (GET /events, Server-Sent Events) and only those
  // parts of the page refresh, through these events on <body>:
  //   bcDownloads  bcRequests  bcLogs  bcWishlist  bcSaved
  // All BookClub tabs in this browser share one stream: the tab holding the lock
  // keeps it open and passes messages on, so many tabs don't use up the browser's
  // few connections to one server. Without a stream (old browser, a proxy that
  // buffers), the pages fall back to polling: hx-trigger="every 10s [bcShouldPoll()]".
  // While a tab is hidden nothing is refreshed; it catches up when you come back.
  var TOPIC_EVENTS = { downloads: 'bcDownloads', requests: 'bcRequests', logs: 'bcLogs',
                       wishlist: 'bcWishlist', saved: 'bcSaved' };
  // At most one refresh per topic this often; the log list refreshes less, so it can be read.
  var THROTTLE_MS = { logs: 10000 }, DEFAULT_THROTTLE_MS = 2000;
  var liveUntil = 0, pending = new Set(), lastFired = {}, timers = {};

  function fire(topic) {
    var name = TOPIC_EVENTS[topic];
    if (!name) return;
    if (document.hidden) { pending.add(topic); return; }
    var wait = (THROTTLE_MS[topic] || DEFAULT_THROTTLE_MS) - (Date.now() - (lastFired[topic] || 0));
    if (wait <= 0) {
      lastFired[topic] = Date.now();
      htmx.trigger(document.body, name);
    } else if (!timers[topic]) {
      timers[topic] = setTimeout(function () { timers[topic] = null; fire(topic); }, wait);
    }
  }
  function fireAll() { Object.keys(TOPIC_EVENTS).forEach(fire); }
  function received(topics) { topics.forEach(function (t) { if (t) fire(t.trim()); }); }
  function markLive() { liveUntil = Date.now() + 45000; }

  window.bcLive = function () { return Date.now() < liveUntil; };
  // Background polling only while visible and without a live stream.
  window.bcShouldPoll = function () { return !document.hidden && !window.bcLive(); };

  var streamOpen = function () { return false; };  // true in the tab holding the stream, while it's connected

  function openStream(broadcast) {
    var source, retry = 3000;
    streamOpen = function () { return !!source && source.readyState === EventSource.OPEN; };
    function connect() {
      source = new EventSource('/events');
      source.addEventListener('hello', function () {
        markLive();
        broadcast({ type: 'live' });
        fireAll();  // anything missed while disconnected
        retry = 3000;
      });
      source.addEventListener('change', function (e) {
        markLive();
        var topics = (e.data || '').split(',');
        broadcast({ type: 'change', topics: topics });
        received(topics);
      });
      source.onmessage = markLive;
      source.onerror = function () {
        liveUntil = 0;
        broadcast({ type: 'down' });
        if (source.readyState === EventSource.CLOSED) {  // the browser gave up: try again later
          setTimeout(connect, retry);
          retry = Math.min(retry * 2, 60000);
        }
      };
    }
    connect();
    // Keep the other tabs' "live" flag fresh between changes (the server pings every 20 s).
    setInterval(function () { if (source && source.readyState === EventSource.OPEN) { markLive(); broadcast({ type: 'live' }); } }, 15000);
  }

  if (window.EventSource) {
    if (window.BroadcastChannel && navigator.locks && navigator.locks.request) {
      var channel = new BroadcastChannel('bookclub-events');
      channel.onmessage = function (e) {
        var msg = e.data || {};
        if (msg.type === 'change') { markLive(); received(msg.topics || []); }
        else if (msg.type === 'live') markLive();
        else if (msg.type === 'down') liveUntil = 0;
        else if (msg.type === 'ping' && streamOpen()) channel.postMessage({ type: 'live' });  // a new tab asks
      };
      channel.postMessage({ type: 'ping' });
      // Held until this tab closes; then another tab takes over the stream.
      navigator.locks.request('bookclub-events', function () {
        openStream(function (msg) { channel.postMessage(msg); });
        return new Promise(function () {});
      });
    } else {
      openStream(function () {});
    }
  }

  document.addEventListener('visibilitychange', function () {
    if (document.hidden) return;
    var topics = pending.size ? Array.from(pending) : [];
    pending.clear();
    // Back on the page: catch up on what changed (everything, if the stream wasn't up).
    if (!window.bcLive()) fireAll(); else topics.forEach(fire);
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
