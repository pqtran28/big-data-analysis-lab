"""
dashboard.py  –  Web Dashboard (Flask)
=======================================
Hiển thị:
  - Live annotated frame từ camera
  - Số người đang hiện diện (real-time)
  - Biểu đồ thống kê theo thời gian
  - Bảng kết quả gần nhất

Chạy:
  python dashboard.py
  Truy cập: http://localhost:5000
"""

import json
import time
from flask import Flask, render_template_string

app = Flask(__name__)
STORAGE_API = "http://localhost:5001"

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>🎥 People Counter Dashboard – DS200</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  :root {
    --bg: #0f1117; --surface: #1a1d2e; --card: #1e2235;
    --accent: #4f8ef7; --green: #22c55e; --red: #ef4444;
    --text: #e2e8f0; --muted: #64748b; --border: #2d3250;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: 'Segoe UI', sans-serif; min-height: 100vh; }

  header {
    background: var(--surface); border-bottom: 1px solid var(--border);
    padding: 16px 32px; display: flex; align-items: center; gap: 16px;
  }
  header h1 { font-size: 1.4rem; font-weight: 700; }
  header .badge {
    background: var(--accent); color: white; padding: 3px 12px;
    border-radius: 20px; font-size: 0.78rem; font-weight: 600;
  }
  .status-dot {
    width: 10px; height: 10px; border-radius: 50%; background: var(--green);
    box-shadow: 0 0 8px var(--green); animation: pulse 2s infinite;
  }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.4} }

  .grid {
    display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px;
    padding: 24px 32px 0;
  }
  .stat-card {
    background: var(--card); border: 1px solid var(--border);
    border-radius: 12px; padding: 20px; text-align: center;
  }
  .stat-card .label { color: var(--muted); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 1px; }
  .stat-card .value { font-size: 3rem; font-weight: 800; color: var(--accent); margin: 8px 0; line-height: 1; }
  .stat-card .sub   { color: var(--muted); font-size: 0.82rem; }

  .main-grid {
    display: grid; grid-template-columns: 3fr 2fr; gap: 16px;
    padding: 16px 32px 24px;
  }

  .panel {
    background: var(--card); border: 1px solid var(--border);
    border-radius: 12px; overflow: hidden;
  }
  .panel-header {
    background: var(--surface); padding: 12px 20px;
    font-weight: 600; font-size: 0.9rem; border-bottom: 1px solid var(--border);
    display: flex; justify-content: space-between; align-items: center;
  }
  .panel-body { padding: 16px; }

  #live-frame {
    width: 100%; border-radius: 8px; background: #000;
    min-height: 300px; display: block; object-fit: contain;
  }
  #no-frame {
    min-height: 300px; display: flex; align-items: center; justify-content: center;
    color: var(--muted); flex-direction: column; gap: 12px;
  }

  .right-panels { display: flex; flex-direction: column; gap: 16px; }

  canvas { max-height: 220px; }

  table { width: 100%; border-collapse: collapse; font-size: 0.82rem; }
  th { color: var(--muted); text-align: left; padding: 6px 8px; border-bottom: 1px solid var(--border); font-weight: 500; }
  td { padding: 6px 8px; border-bottom: 1px solid var(--border); }
  tr:last-child td { border-bottom: none; }
  .count-badge {
    display: inline-block; background: var(--accent); color: white;
    border-radius: 10px; padding: 2px 10px; font-weight: 700; font-size: 0.85rem;
  }
  .count-badge.zero { background: var(--muted); }

  #refresh-info { color: var(--muted); font-size: 0.75rem; }
  #last-update  { color: var(--green); font-size: 0.82rem; }
</style>
</head>
<body>

<header>
  <div class="status-dot"></div>
  <h1>🎥 People Counter Dashboard</h1>
  <span class="badge">DS200 – Big Data</span>
  <span style="margin-left:auto; color:var(--muted); font-size:0.8rem;" id="last-update">Đang tải...</span>
</header>

<!-- Stat cards -->
<div class="grid">
  <div class="stat-card">
    <div class="label">👥 Hiện tại</div>
    <div class="value" id="stat-current">–</div>
    <div class="sub">người trong frame</div>
  </div>
  <div class="stat-card">
    <div class="label">📊 Trung bình</div>
    <div class="value" id="stat-avg" style="font-size:2rem; padding-top:8px;">–</div>
    <div class="sub">người / frame</div>
  </div>
  <div class="stat-card">
    <div class="label">🏆 Cao nhất</div>
    <div class="value" id="stat-max" style="color:var(--green)">–</div>
    <div class="sub">người trong 1 frame</div>
  </div>
</div>

<!-- Main content -->
<div class="main-grid">

  <!-- Live feed -->
  <div class="panel">
    <div class="panel-header">
      📹 Live Feed
      <span id="refresh-info">cập nhật mỗi 1s</span>
    </div>
    <div class="panel-body">
      <img id="live-frame" src="" alt="live" style="display:none">
      <div id="no-frame">
        <span style="font-size:3rem">📷</span>
        <span>Chờ frame từ camera...</span>
        <span style="font-size:0.75rem">Đảm bảo camera_server và detection_server đang chạy</span>
      </div>
    </div>
  </div>

  <!-- Right panels -->
  <div class="right-panels">

    <!-- Timeline chart -->
    <div class="panel">
      <div class="panel-header">📈 Người theo thời gian (10 phút)</div>
      <div class="panel-body">
        <canvas id="timelineChart"></canvas>
      </div>
    </div>

    <!-- Recent results table -->
    <div class="panel">
      <div class="panel-header">🕒 Kết quả gần nhất</div>
      <div class="panel-body" style="padding:0">
        <table>
          <thead><tr>
            <th>Frame</th>
            <th>Thời gian</th>
            <th>Người</th>
            <th>Latency</th>
          </tr></thead>
          <tbody id="results-table">
            <tr><td colspan="4" style="text-align:center; color:var(--muted); padding:20px">Đang tải...</td></tr>
          </tbody>
        </table>
      </div>
    </div>

  </div>
</div>

<script>
const STORAGE = "{{ storage_api }}";

// ── Chart setup ──────────────────────────────────────────────────────────────
const ctx = document.getElementById('timelineChart').getContext('2d');
const timelineChart = new Chart(ctx, {
  type: 'line',
  data: {
    labels: [],
    datasets: [{
      label: 'TB người/phút',
      data: [],
      borderColor: '#4f8ef7',
      backgroundColor: 'rgba(79,142,247,0.15)',
      tension: 0.4,
      fill: true,
      pointRadius: 3,
    }]
  },
  options: {
    responsive: true,
    plugins: { legend: { display: false } },
    scales: {
      x: { ticks: { color: '#64748b', maxTicksLimit: 6 }, grid: { color: '#2d3250' } },
      y: { ticks: { color: '#64748b' }, grid: { color: '#2d3250' }, min: 0, suggestedMax: 5 }
    }
  }
});

// ── Fetch & update ────────────────────────────────────────────────────────────
async function fetchSafe(url) {
  try {
    const r = await fetch(url);
    if (!r.ok) return null;
    return await r.json();
  } catch { return null; }
}

async function updateDashboard() {
  // Stats
  const stats = await fetchSafe(`${STORAGE}/api/stats`);
  if (stats) {
    document.getElementById('stat-current').textContent = stats.latest_count ?? '–';
    document.getElementById('stat-avg').textContent    = stats.avg_people?.toFixed(1) ?? '–';
    document.getElementById('stat-max').textContent    = stats.max_people ?? '–';

    // Timeline chart
    const tl = stats.timeline_10min || [];
    if (tl.length > 0) {
      timelineChart.data.labels = tl.map(r => r.minute);
      timelineChart.data.datasets[0].data = tl.map(r => r.avg_count);
      timelineChart.update('none');
    }
  }

  // Latest frame
  const latest = await fetchSafe(`${STORAGE}/api/latest`);
  if (latest && latest.annotated_b64) {
    const img = document.getElementById('live-frame');
    img.src = 'data:image/jpeg;base64,' + latest.annotated_b64;
    img.style.display = 'block';
    document.getElementById('no-frame').style.display = 'none';
  }

  // Recent results table
  const results = await fetchSafe(`${STORAGE}/api/results`);
  if (results && results.length > 0) {
    const tbody = document.getElementById('results-table');
    const rows = results.slice(0, 12).map(r => {
      const badgeClass = r.person_count === 0 ? 'zero' : '';
      return `<tr>
        <td style="color:var(--muted)">#${r.frame_id}</td>
        <td>${r.capture_time || '–'}</td>
        <td><span class="count-badge ${badgeClass}">${r.person_count}</span></td>
        <td style="color:var(--muted)">${r.latency_ms ? r.latency_ms.toFixed(0)+'ms' : '–'}</td>
      </tr>`;
    }).join('');
    tbody.innerHTML = rows;
  }

  // Update time
  document.getElementById('last-update').textContent = 
    'Cập nhật: ' + new Date().toLocaleTimeString('vi-VN');
}

// Refresh mỗi 1 giây
updateDashboard();
setInterval(updateDashboard, 1000);
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE, storage_api=STORAGE_API)


if __name__ == "__main__":
    print("🌐 Dashboard tại http://localhost:5000")
    print("   (Đảm bảo storage_server đang chạy tại port 5001)\n")
    app.run(host="0.0.0.0", port=5000, debug=False)
