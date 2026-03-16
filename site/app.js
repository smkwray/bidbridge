/* BidBridge — interactive research site */
(async function () {
  "use strict";

  let D;
  try {
    const resp = await fetch("data/bidbridge.json");
    const text = await resp.text();
    D = JSON.parse(text);
  } catch (e) {
    console.error("Failed to load data:", e);
    document.body.insertAdjacentHTML("afterbegin",
      '<div style="background:#ef4444;color:#fff;padding:1rem;text-align:center">Data failed to load. Check console.</div>');
    return;
  }

  /* ===== THEME HELPERS ===== */
  const isDark = () => document.documentElement.getAttribute("data-theme") !== "light";
  const plotBg = () => (isDark() ? "#111827" : "#ffffff");
  const plotText = () => (isDark() ? "#e8ecf1" : "#1e293b");
  const plotGrid = () => (isDark() ? "#1e293b" : "#e2e8f0");
  const BLUE = "#3b82f6", RED = "#ef4444", GREEN = "#22c55e", ORANGE = "#f97316", PURPLE = "#a855f7";

  /* ===== CHART CONFIGS ===== */
  // Small charts inside finding cards: no toolbar at all
  const smallConfig = { responsive: true, displayModeBar: false, staticPlot: false };
  // Large full-width charts: show toolbar
  const bigConfig = { responsive: true, displayModeBar: true,
    modeBarButtonsToRemove: ["lasso2d", "select2d", "autoScale2d"], displaylogo: false };

  function baseLayout(title, xTitle, yTitle, extra) {
    return Object.assign({
      title: { text: title, font: { family: "Source Serif 4", size: 16, color: plotText() },
               x: 0.01, xanchor: "left" },
      paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: plotBg(),
      font: { family: "Inter", color: plotText(), size: 12 },
      xaxis: { title: { text: xTitle, standoff: 10 }, gridcolor: plotGrid(),
               linecolor: plotGrid(), tickfont: { color: plotText() } },
      yaxis: { title: { text: yTitle, standoff: 8 }, gridcolor: plotGrid(),
               linecolor: plotGrid(), tickfont: { color: plotText() } },
      margin: { t: 40, r: 30, b: 60, l: 70 },
      hovermode: "closest",
      hoverlabel: { bgcolor: isDark() ? "#1a2332" : "#fff",
                    bordercolor: isDark() ? "#334155" : "#cbd5e1",
                    font: { color: plotText(), family: "Inter", size: 12 } },
      legend: { orientation: "h", y: -0.22, font: { color: plotText() } },
    }, extra || {});
  }

  /* ===== HERO STATS ===== */
  document.getElementById("hero-stats").innerHTML = [
    { value: D.panel_stats.total_weeks, label: "Weeks" },
    { value: D.panel_stats.bridge_episodes, label: "Bridge Episodes" },
    { value: "5", label: "Data Sources" },
    { value: D.panel_stats.columns, label: "Panel Columns" },
  ].map(s => `<div class="hero-stat"><span class="hero-stat-value">${s.value}</span><span class="hero-stat-label">${s.label}</span></div>`).join("");

  /* ===== LP HEADLINE NUMBERS ===== */
  const fmt$ = v => (v >= 0 ? "+" : "") + Math.round(v).toLocaleString();
  const lp = D.lp_results;
  if (lp.full_sample?.length) document.getElementById("lp-h0-full").textContent = fmt$(lp.full_sample[0].beta);
  if (lp.qt_period?.length) document.getElementById("lp-h0-qt").textContent = fmt$(lp.qt_period[0].beta);
  if (lp.non_qt_period?.length) document.getElementById("lp-h0-nonqt").textContent = fmt$(lp.non_qt_period[0].beta);

  /* ===== CHART: LP IRF ===== */
  function plotLPIRF() {
    const full = lp.full_sample || [];
    const h = full.map(r => r.h), beta = full.map(r => r.beta);
    const ci_lo = full.map(r => r.ci_lo), ci_hi = full.map(r => r.ci_hi);
    Plotly.react("chart-lp-irf", [
      { x: h, y: ci_hi, mode: "lines", line: { width: 0 }, showlegend: false, hoverinfo: "skip" },
      { x: h, y: ci_lo, mode: "lines", line: { width: 0 }, fill: "tonexty",
        fillcolor: "rgba(59,130,246,0.15)", showlegend: false, hoverinfo: "skip" },
      { x: h, y: beta, mode: "lines+markers", name: "LP coefficient",
        line: { color: BLUE, width: 2.5 }, marker: { size: 7, color: BLUE },
        hovertemplate: "h=%{x}: <b>%{y:,.0f}</b> $M<extra></extra>" },
      { x: h, y: h.map(() => 0), mode: "lines", line: { color: plotGrid(), dash: "dash", width: 1 },
        showlegend: false, hoverinfo: "skip" },
    ], baseLayout("Cumulative Impulse Response", "Weeks after supply shock",
      "Inventory response ($M)"), smallConfig);
  }

  /* ===== CHART: LP REGIME ===== */
  function plotLPRegime() {
    const full = lp.full_sample || [], qt = lp.qt_period || [];
    Plotly.react("chart-lp-regime", [
      { x: full.map(r => r.h), y: full.map(r => r.ci_hi), mode: "lines", line: { width: 0 },
        showlegend: false, hoverinfo: "skip" },
      { x: full.map(r => r.h), y: full.map(r => r.ci_lo), fill: "tonexty",
        fillcolor: "rgba(34,197,94,0.1)", mode: "lines", line: { width: 0 },
        showlegend: false, hoverinfo: "skip" },
      { x: full.map(r => r.h), y: full.map(r => r.beta), mode: "lines+markers",
        name: "Full sample", line: { color: GREEN, width: 2 }, marker: { size: 5, color: GREEN },
        hovertemplate: "h=%{x}: <b>%{y:,.0f}</b> $M<extra></extra>" },
      { x: qt.map(r => r.h), y: qt.map(r => r.ci_hi), mode: "lines", line: { width: 0 },
        showlegend: false, hoverinfo: "skip" },
      { x: qt.map(r => r.h), y: qt.map(r => r.ci_lo), fill: "tonexty",
        fillcolor: "rgba(239,68,68,0.1)", mode: "lines", line: { width: 0 },
        showlegend: false, hoverinfo: "skip" },
      { x: qt.map(r => r.h), y: qt.map(r => r.beta), mode: "lines+markers",
        name: "QT period (\u03B2+\u03B4)", line: { color: RED, width: 2 },
        marker: { size: 5, symbol: "square", color: RED },
        hovertemplate: "h=%{x}: <b>%{y:,.0f}</b> $M<extra></extra>" },
      { x: [0, 12], y: [0, 0], mode: "lines", line: { color: plotGrid(), dash: "dash", width: 1 },
        showlegend: false, hoverinfo: "skip" },
    ], baseLayout("IRF by Monetary Policy Regime", "Weeks after supply shock",
      "Cumulative response ($M)"), smallConfig);
  }

  /* ===== CHART: SCATTER ===== */
  function plotScatter() {
    const s = D.scatter;
    Plotly.react("chart-scatter", [{
      x: s.supply_B, y: s.dealer_share_pct, text: s.weeks, mode: "markers",
      marker: { size: 5, color: s.weeks.map((_, i) => i), colorscale: "Viridis", opacity: 0.6,
                colorbar: { title: { text: "Time", font: { color: plotText(), size: 10 } },
                            tickfont: { color: plotText() }, len: 0.5 } },
      hovertemplate: "<b>%{text}</b><br>Supply: $%{x:.1f}B<br>Dealer: %{y:.1f}%<extra></extra>",
    }], baseLayout("Dealer Share vs Weekly Supply (r = \u22120.82)",
      "Weekly awarded ($B)", "Dealer share (%)"), smallConfig);
  }

  /* ===== CHART: MATURITY BARS ===== */
  function plotMaturity() {
    const buckets = Object.keys(D.maturity_buckets);
    const labels = { bills: "Bills", short_coupon: "Short<br>2\u20133Y", belly_coupon: "Belly<br>5\u20137Y",
      long_coupon: "Long<br>10\u201330Y", tips: "TIPS", frns: "FRN" };
    const x = buckets.map(b => labels[b] || b);
    const y = buckets.map(b => +(D.maturity_buckets[b].avg_dealer_share * 100).toFixed(1));
    const colors = [BLUE, ORANGE, GREEN, RED, PURPLE, "#64748b"];
    Plotly.react("chart-maturity", [{
      x, y, type: "bar", marker: { color: colors, line: { width: 0 } },
      text: y.map(v => v + "%"), textposition: "outside",
      textfont: { color: plotText(), size: 13, family: "JetBrains Mono" },
      hovertemplate: "<b>%{x}</b><br>Dealer share: %{y}%<extra></extra>",
    }], baseLayout("Avg Dealer Share by Maturity", "", "Dealer share (%)"), smallConfig);
  }

  /* ===== CHART: REFUNDING TEST ===== */
  function plotRefunding() {
    const rt = D.refunding_test || [];
    if (!rt.length) return;
    // Shorten labels to avoid overlap
    const shortLabels = {
      "Inventory change ($M)": "Inv Change",
      "Weekly awarded ($)": "Awarded",
      "Dealer share": "Dealer %",
      "Bid-to-cover": "Bid/Cover",
      "Tail (bp)": "Tail (bp)",
    };
    const vars = rt.map(r => shortLabels[r.variable] || r.variable);
    Plotly.react("chart-refunding", [
      { x: vars, y: rt.map(r => r.refunding_mean), name: "Refunding", type: "bar",
        marker: { color: RED }, hovertemplate: "%{x}: <b>%{y:,.2f}</b><extra>Refunding</extra>" },
      { x: vars, y: rt.map(r => r.ordinary_mean), name: "Ordinary", type: "bar",
        marker: { color: BLUE }, hovertemplate: "%{x}: <b>%{y:,.2f}</b><extra>Ordinary</extra>" },
    ], Object.assign(
      baseLayout("Refunding vs Ordinary Weeks", "", "Mean value"),
      { barmode: "group", margin: { t: 40, r: 30, b: 80, l: 70 },
        legend: { orientation: "h", y: -0.3, font: { color: plotText() } } }
    ), smallConfig);
  }

  /* ===== CHART: TIMESERIES ===== */
  const TS = D.timeseries;
  const bridgeWeeks = TS.weeks.filter((_, i) => TS.bridge[i] === 1);
  const bridgeSupply = TS.supply_B.filter((_, i) => TS.bridge[i] === 1);
  const bridgeInv = TS.inventory_B.filter((_, i) => TS.bridge[i] === 1);

  function plotTimeseries(mode) {
    let traces = [], layout;
    if (mode === "supply") {
      traces = [
        { x: TS.weeks, y: TS.supply_B, name: "Weekly awarded", line: { color: BLUE, width: 1 },
          opacity: 0.7, hovertemplate: "%{x}<br>$%{y:.1f}B<extra></extra>" },
        { x: bridgeWeeks, y: bridgeSupply, mode: "markers", name: "Bridge episode",
          marker: { color: RED, size: 5 },
          hovertemplate: "%{x}<br>Bridge: $%{y:.1f}B<extra></extra>" },
      ];
      layout = baseLayout("Weekly Treasury Supply", "", "Awarded ($B)");
    } else if (mode === "inventory") {
      traces = [
        { x: TS.weeks, y: TS.inventory_B, name: "Dealer inventory",
          line: { color: ORANGE, width: 1.5 },
          hovertemplate: "%{x}<br>$%{y:.1f}B<extra></extra>" },
        { x: bridgeWeeks, y: bridgeInv, mode: "markers", name: "Bridge episode",
          marker: { color: RED, size: 5 },
          hovertemplate: "%{x}<br>Bridge: $%{y:.1f}B<extra></extra>" },
      ];
      layout = baseLayout("Dealer Treasury Inventory", "", "Inventory ($B)");
    } else if (mode === "dealer_share") {
      traces = [{ x: TS.weeks, y: TS.dealer_share_pct, name: "Dealer share",
        line: { color: GREEN, width: 1.5 },
        hovertemplate: "%{x}<br>%{y:.1f}%<extra></extra>" }];
      layout = baseLayout("Dealer Allotment Share", "", "Dealer share (%)");
    } else if (mode === "soma") {
      traces = [
        { x: TS.weeks, y: TS.inventory_B, name: "Dealer ($B)",
          line: { color: BLUE, width: 1.5 },
          hovertemplate: "%{x}<br>Dealer: $%{y:.1f}B<extra></extra>" },
        { x: TS.weeks, y: TS.soma_T, name: "SOMA ($T)",
          line: { color: RED, width: 1.5 }, yaxis: "y2",
          hovertemplate: "%{x}<br>SOMA: $%{y:.3f}T<extra></extra>" },
      ];
      layout = Object.assign(baseLayout("Dealer vs Fed Holdings", "", "Dealer ($B)"), {
        yaxis2: { title: { text: "SOMA ($T)", standoff: 8 }, overlaying: "y", side: "right",
                  gridcolor: "transparent", tickfont: { color: plotText() } },
      });
    } else if (mode === "btc") {
      traces = [{ x: TS.weeks, y: TS.btc, name: "Bid-to-cover",
        line: { color: PURPLE, width: 1.5 },
        hovertemplate: "%{x}<br>BTC: %{y:.2f}<extra></extra>" }];
      layout = baseLayout("Weighted Bid-to-Cover Ratio", "", "Bid-to-cover");
    }
    Plotly.react("chart-timeseries", traces, layout, bigConfig);
  }

  // Toggle buttons with sequential glow
  const chartBtns = document.querySelectorAll(".chart-btn");
  chartBtns.forEach(btn => {
    btn.addEventListener("click", () => {
      chartBtns.forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      plotTimeseries(btn.dataset.series);
    });
  });

  // Sequential glow animation
  let glowIdx = 0;
  function advanceGlow() {
    chartBtns.forEach(b => b.classList.remove("glow"));
    const target = chartBtns[glowIdx % chartBtns.length];
    if (!target.classList.contains("active")) {
      target.classList.add("glow");
    }
    glowIdx++;
    setTimeout(advanceGlow, 2500);
  }
  setTimeout(advanceGlow, 3000);

  /* ===== CHART: ANNUAL ===== */
  function plotAnnual() {
    const ann = D.annual_summary;
    const years = ann.map(r => r.year);
    Plotly.react("chart-annual", [
      { x: years, y: ann.map(r => r.awarded_B), name: "Awarded ($B)", type: "bar",
        marker: { color: BLUE, opacity: 0.7 },
        hovertemplate: "%{x}<br>$%{y:,.0f}B<extra></extra>" },
      { x: years, y: ann.map(r => +(r.dealer_share * 100).toFixed(1)),
        name: "Dealer share (%)", yaxis: "y2",
        line: { color: ORANGE, width: 2.5 }, mode: "lines+markers",
        marker: { size: 6, color: ORANGE },
        hovertemplate: "%{x}<br>%{y:.1f}%<extra></extra>" },
    ], Object.assign(baseLayout("Annual Issuance & Dealer Share", "Year", "Awarded ($B)"), {
      yaxis2: { title: { text: "Dealer share (%)", standoff: 8 }, overlaying: "y", side: "right",
                gridcolor: "transparent", range: [20, 80], tickfont: { color: plotText() } },
    }), bigConfig);
  }

  /* ===== CHART: STRESS ===== */
  function plotStress() {
    const ss = D.stress_summary || [];
    if (!ss.length) return;
    const nice = { qt_period: "QT Period", tga_rebuild: "TGA Rebuild",
      weak_bank_absorption: "Weak Banks", risk_off_window: "Risk-Off" };
    const flags = ss.map(r => nice[r.stress_flag] || r.stress_flag);
    Plotly.react("chart-stress", [
      { x: flags, y: ss.map(r => +((r.bridge_rate_flagged || 0) * 100).toFixed(1)),
        name: "During regime", type: "bar", marker: { color: RED },
        hovertemplate: "%{x}: <b>%{y}%</b><extra>During</extra>" },
      { x: flags, y: ss.map(r => +((r.bridge_rate_unflagged || 0) * 100).toFixed(1)),
        name: "Outside regime", type: "bar", marker: { color: BLUE },
        hovertemplate: "%{x}: <b>%{y}%</b><extra>Outside</extra>" },
    ], Object.assign(baseLayout("Bridge Rate by Stress Regime", "", "Bridge rate (%)"),
      { barmode: "group" }), smallConfig);
  }

  /* ===== CHART: PERSISTENCE ===== */
  function plotPersistence() {
    const bs = D.bridge_summary || [];
    if (!bs.length) return;
    const years = bs.map(r => r.year);
    Plotly.react("chart-persistence", [
      { x: years, y: bs.map(r => r.episodes), name: "Episodes", type: "bar",
        marker: { color: ORANGE, opacity: 0.8 },
        hovertemplate: "%{x}: <b>%{y}</b> episodes<extra></extra>" },
      { x: years, y: bs.map(r => r.avg_inv_change_M / 1000), name: "Avg accum ($B)",
        yaxis: "y2", line: { color: RED, width: 2 }, mode: "lines+markers",
        marker: { size: 5, color: RED },
        hovertemplate: "%{x}: <b>$%{y:.1f}B</b><extra></extra>" },
    ], Object.assign(baseLayout("Bridge Episodes by Year", "Year", "Episodes"), {
      yaxis2: { title: { text: "Avg ($B)", standoff: 4 }, overlaying: "y", side: "right",
                gridcolor: "transparent", tickfont: { color: plotText() } },
    }), smallConfig);
  }

  /* ===== REGRESSION TABLE ===== */
  const termLabels = {
    intercept: "Intercept", supply_M: "Weekly supply ($M)", dealer_share: "Dealer share",
    refunding_week: "Refunding week", d_soma_B: "\u0394 SOMA ($B)",
    d_bank_holdings_M: "\u0394 Bank holdings ($M)", trend_years: "Time trend (years)",
  };
  const regBody = document.getElementById("reg-table-body");
  const reg = D.regression_extended;
  if (reg && typeof reg === "object") {
    for (const [term, v] of Object.entries(reg)) {
      if (!v || typeof v.coef !== "number") continue;
      const label = termLabels[term] || term;
      const stars = v.p < 0.001 ? "***" : v.p < 0.01 ? "**" : v.p < 0.05 ? "*" : "";
      const cls = v.p < 0.05 ? "sig" : "not-sig";
      regBody.insertAdjacentHTML("beforeend",
        `<tr>
          <td style="font-family:var(--font-sans)">${label}</td>
          <td>${v.coef.toLocaleString(undefined, { maximumFractionDigits: 2 })}</td>
          <td>${v.se.toLocaleString(undefined, { maximumFractionDigits: 2 })}</td>
          <td>${v.t.toFixed(2)}</td>
          <td class="${cls}">${v.p < 0.001 ? "&lt;0.001" : v.p.toFixed(4)}</td>
          <td class="${cls}" style="font-weight:700">${stars}</td>
        </tr>`);
    }
  }

  /* ===== DATA SOURCES ===== */
  const srcGrid = document.getElementById("source-grid");
  D.data_sources.forEach(src => {
    srcGrid.insertAdjacentHTML("beforeend",
      `<div class="source-card">
        <h4>${src.name}</h4>
        <div class="source-meta">
          <span class="source-badge">${src.provider}</span>
          <span class="source-badge">${src.freq}</span>
          <span class="source-badge">${src.records.toLocaleString()} records</span>
        </div>
        <p class="source-desc">${src.desc}</p>
        <p class="source-fields"><strong>Key fields:</strong> <code>${src.fields}</code></p>
        <a href="${src.url}" target="_blank" rel="noopener">View source &rarr;</a>
      </div>`);
  });

  /* ===== ANNUAL TABLE ===== */
  const annBody = document.getElementById("annual-table-body");
  D.annual_summary.forEach(row => {
    annBody.insertAdjacentHTML("beforeend",
      `<tr>
        <td>${row.year}</td><td>${row.auctions}</td><td>${row.awarded_B.toLocaleString()}</td>
        <td>${(row.dealer_share * 100).toFixed(1)}%</td>
        <td>${row.inventory_M ? Math.round(row.inventory_M).toLocaleString() : "\u2014"}</td>
        <td>${row.bridge_episodes}</td>
      </tr>`);
  });

  /* ===== RENDER ALL ===== */
  plotLPIRF(); plotLPRegime(); plotScatter(); plotMaturity(); plotRefunding();
  plotTimeseries("supply"); plotAnnual(); plotStress(); plotPersistence();

  /* ===== THEME TOGGLE ===== */
  const toggle = document.getElementById("theme-toggle");
  const themeIcon = toggle.querySelector(".theme-icon");
  function setTheme(t) {
    document.documentElement.setAttribute("data-theme", t);
    themeIcon.textContent = t === "dark" ? "\u263E" : "\u2600";
    localStorage.setItem("bb-theme", t);
    plotLPIRF(); plotLPRegime(); plotScatter(); plotMaturity(); plotRefunding();
    const active = document.querySelector(".chart-btn.active");
    if (active) plotTimeseries(active.dataset.series);
    plotAnnual(); plotStress(); plotPersistence();
  }
  toggle.addEventListener("click", () => setTheme(isDark() ? "light" : "dark"));
  const saved = localStorage.getItem("bb-theme");
  if (saved && saved !== "dark") setTheme(saved);
})();
