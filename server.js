const WebSocket = require('ws');
const net = require('net');
const tls = require('tls');
const dgram = require('dgram');
const fetch = require('node-fetch');
const http = require('http');
const https = require('https');
const url = require('url');

// Constants
const horse = Buffer.from("dHJvamFu", 'base64').toString();
const flash = Buffer.from("dm1lc3M=", 'base64').toString();
const v2 = Buffer.from("djJyYXk=", 'base64').toString();
const neko = Buffer.from("Y2xhc2g=", 'base64').toString();

const KV_PRX_URL = "https://raw.githubusercontent.com/backup-heavenly-demons/gateway/refs/heads/main/kvProxyList.json";
const DNS_SERVER_ADDRESS = "8.8.8.8";
const DNS_SERVER_PORT = 53;
const WS_READY_STATE_OPEN = 1;
const WS_READY_STATE_CLOSING = 2;
const CORS_HEADER_OPTIONS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,HEAD,POST,OPTIONS",
  "Access-Control-Max-Age": "86400",
};

// Region Mapping
const REGION_MAP = {
  ASIA: ["ID", "SG", "MY", "PH", "TH", "VN", "JP", "KR", "CN", "HK", "TW"],
  SOUTHASIA: ["IN", "BD", "PK", "LK", "NP", "AF", "BT", "MV"],
  CENTRALASIA: ["KZ", "UZ", "TM", "KG", "TJ"],
  NORTHASIA: ["RU"],
  MIDDLEEAST: ["AE", "SA", "IR", "IQ", "JO", "IL", "YE", "SY", "OM", "KW", "QA", "BH", "LB"],
  CIS: ["RU", "UA", "BY", "KZ", "UZ", "AM", "GE", "MD", "TJ", "KG", "TM", "AZ"],
  WESTEUROPE: ["FR", "DE", "NL", "BE", "AT", "CH", "IE", "LU", "MC"],
  EASTEUROPE: ["PL", "CZ", "SK", "HU", "RO", "BG", "MD", "UA", "BY"],
  NORTHEUROPE: ["SE", "FI", "NO", "DK", "EE", "LV", "LT", "IS"],
  SOUTHEUROPE: ["IT", "ES", "PT", "GR", "HR", "SI", "MT", "AL", "BA", "RS", "ME", "MK"],
  EUROPE: ["FR", "DE", "NL", "BE", "AT", "CH", "IE", "LU", "MC", "PL", "CZ", "SK", "HU", "RO", "BG", "MD", "UA", "BY", "SE", "FI", "NO", "DK", "EE", "LV", "LT", "IS", "IT", "ES", "PT", "GR", "HR", "SI", "MT", "AL", "BA", "RS", "ME", "MK"],
  AFRICA: ["ZA", "NG", "EG", "MA", "KE", "DZ", "TN", "GH", "CI", "SN", "ET"],
  NORTHAMERICA: ["US", "CA", "MX"],
  SOUTHAMERICA: ["BR", "AR", "CL", "CO", "PE", "VE", "EC", "UY", "PY", "BO"],
  LATAM: ["MX", "BR", "AR", "CL", "CO", "PE", "VE", "EC", "UY", "PY", "BO", "CR", "GT", "PA", "DO", "HN", "NI", "SV"],
  AMERICA: ["US", "CA", "MX", "BR", "AR", "CL", "CO", "PE", "VE", "EC"],
  OCEANIA: ["AU", "NZ", "PG", "FJ"],
  GLOBAL: []
};

class GatewayServer {
  constructor() {
    this.prxIP = "";
    this.cachedPrxList = [];
    this.wss = null;
    this.httpServer = null;
    this.activeUDPConnections = new Map();
    this.CORS_HEADER_OPTIONS = CORS_HEADER_OPTIONS;
    this.connectionMode = "sni";
    this.sniHost = "business.whatsapp.com";
    this.useSNI = true;
  }

  // ==================== HTTP HANDLERS ====================

  handleHealthCheck(req, res) {
    const healthData = {
      status: 'healthy',
      timestamp: new Date().toISOString(),
      service: 'railway-gateway',
      uptime: process.uptime(),
      memory: process.memoryUsage(),
      version: process.env.npm_package_version || '1.0.0',
      features: {
        websocket: true,
        tcp: true,
        udp: true,
        protocols: ['trojan', 'vmess', 'ss']
      },
      network: {
        udp_supported: true,
        outbound_allowed: true
      }
    };

    res.writeHead(200, {
      'Content-Type': 'application/json',
      ...this.CORS_HEADER_OPTIONS
    });
    res.end(JSON.stringify(healthData, null, 2));
  }

  handleCorsPreflight(req, res) {
    res.writeHead(200, this.CORS_HEADER_OPTIONS);
    res.end();
  }

  async handleApiRequest(req, res, parsedUrl) {
    try {
      if (parsedUrl.pathname === '/api/proxies') {
        const proxies = await this.getPrxList(process.env.PRX_BANK_URL);
        const format = parsedUrl.query.format || 'json';
        
        if (format === 'text') {
          const proxyText = proxies.map(p => 
            `${p.country} - ${p.prxIP}:${p.prxPort}`
          ).join('\n');
          
          res.writeHead(200, {
            'Content-Type': 'text/plain',
            ...this.CORS_HEADER_OPTIONS
          });
          res.end(proxyText);
          return;
        }
        
        res.writeHead(200, {
          'Content-Type': 'application/json',
          ...this.CORS_HEADER_OPTIONS
        });
        res.end(JSON.stringify(proxies, null, 2));
        return;
      }
    } catch (error) {
      console.error('API error:', error);
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Internal server error' }));
    }
  }

  // Main HTTP request handler
  async handleHttpRequest(req, res) {
    const parsedUrl = url.parse(req.url, true);
    
    if (req.method === 'OPTIONS') {
      this.handleCorsPreflight(req, res);
      return;
    }
    
    if (parsedUrl.pathname === '/health') {
      this.handleHealthCheck(req, res);
      return;
    }
    
    if (parsedUrl.pathname.startsWith('/api/')) {
      await this.handleApiRequest(req, res, parsedUrl);
      return;
    }
    
    if (parsedUrl.pathname === '/') {
      const currentHost = req.headers.host || 'localhost:3000';
      const protocolWs = req.headers['x-forwarded-proto'] === 'https' ? 'wss' : 'ws';
      const protocolHttp = req.headers['x-forwarded-proto'] === 'https' ? 'https' : 'http';
      
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(this.getHTMLPage(currentHost, protocolWs, protocolHttp));
      return;
    }
    
    const targetReversePrx = process.env.REVERSE_PRX_TARGET;
    if (targetReversePrx) {
      await this.reverseWeb(req, res, targetReversePrx);
    } else {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('Not Found');
    }
  }

  getHTMLPage(currentHost, protocolWs, protocolHttp) {
    return `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>J1BTNL CONFIG SNI</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap');
    
    * {
      margin: 0;
      padding: 0;
      box-sizing: border-box;
    }
    
    body {
      font-family: 'Inter', sans-serif;
      background: #0a0a0f;
      min-height: 100vh;
      color: #e0e0e0;
    }

    .glass {
      background: rgba(16, 24, 40, 0.6);
      backdrop-filter: blur(10px);
      border: 1px solid rgba(59, 130, 246, 0.15);
    }

    .glass:hover {
      border-color: rgba(59, 130, 246, 0.3);
    }

    .gradient-text {
      background: linear-gradient(135deg, #60a5fa, #3b82f6, #1d4ed8);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
    }

    .neon-glow {
      box-shadow: 0 0 30px rgba(59, 130, 246, 0.1), inset 0 0 30px rgba(59, 130, 246, 0.05);
    }

    .border-glow {
      border: 1px solid rgba(59, 130, 246, 0.2);
      transition: all 0.3s ease;
    }

    .border-glow:hover {
      border-color: rgba(59, 130, 246, 0.5);
      box-shadow: 0 0 25px rgba(59, 130, 246, 0.08);
    }

    .btn-primary {
      background: linear-gradient(135deg, #3b82f6, #1d4ed8);
      transition: all 0.3s ease;
    }

    .btn-primary:hover {
      transform: translateY(-2px);
      box-shadow: 0 10px 30px rgba(59, 130, 246, 0.3);
    }

    .btn-primary:active {
      transform: scale(0.98);
    }

    .stat-card {
      background: rgba(16, 24, 40, 0.8);
      border: 1px solid rgba(59, 130, 246, 0.15);
      border-radius: 12px;
      padding: 20px;
      transition: all 0.3s ease;
    }

    .stat-card:hover {
      border-color: rgba(59, 130, 246, 0.3);
      transform: translateY(-2px);
    }

    .input-dark {
      background: rgba(16, 24, 40, 0.8);
      border: 1px solid rgba(59, 130, 246, 0.2);
      color: #e0e0e0;
      transition: all 0.3s ease;
      border-radius: 8px;
      padding: 10px 14px;
      width: 100%;
    }

    .input-dark:focus {
      outline: none;
      border-color: #3b82f6;
      box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
    }

    .input-dark::placeholder {
      color: rgba(224, 224, 224, 0.4);
    }

    select.input-dark {
      appearance: none;
      background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' fill='none' viewBox='0 0 20 20'%3E%3Cpath stroke='%236b7280' stroke-linecap='round' stroke-linejoin='round' stroke-width='1.5' d='M6 8l4 4 4-4'/%3E%3C/svg%3E");
      background-position: right 12px center;
      background-repeat: no-repeat;
      background-size: 20px;
      padding-right: 40px;
    }

    .output-box {
      background: rgba(11, 17, 29, 0.9);
      border: 1px solid rgba(59, 130, 246, 0.15);
      border-radius: 8px;
      padding: 14px;
      font-family: 'Courier New', monospace;
      font-size: 11px;
      word-break: break-all;
      max-height: 200px;
      overflow-y: auto;
      color: #93c5fd;
    }

    .output-box::-webkit-scrollbar {
      width: 4px;
    }

    .output-box::-webkit-scrollbar-track {
      background: rgba(16, 24, 40, 0.5);
    }

    .output-box::-webkit-scrollbar-thumb {
      background: #3b82f6;
      border-radius: 4px;
    }

    .toggle-switch {
      position: relative;
      width: 48px;
      height: 26px;
      background: rgba(75, 85, 99, 0.5);
      border-radius: 13px;
      cursor: pointer;
      transition: 0.3s;
      border: 1px solid rgba(59, 130, 246, 0.2);
    }

    .toggle-switch.active {
      background: linear-gradient(135deg, #3b82f6, #1d4ed8);
      border-color: #3b82f6;
    }

    .toggle-switch .toggle-dot {
      position: absolute;
      top: 2px;
      left: 2px;
      width: 20px;
      height: 20px;
      background: white;
      border-radius: 50%;
      transition: 0.3s;
    }

    .toggle-switch.active .toggle-dot {
      transform: translateX(22px);
    }

    .badge {
      padding: 4px 12px;
      border-radius: 20px;
      font-size: 10px;
      font-weight: 600;
      letter-spacing: 0.5px;
    }

    .badge-blue {
      background: rgba(59, 130, 246, 0.2);
      color: #60a5fa;
      border: 1px solid rgba(59, 130, 246, 0.2);
    }

    .badge-green {
      background: rgba(52, 211, 153, 0.15);
      color: #34d399;
      border: 1px solid rgba(52, 211, 153, 0.15);
    }

    .provider-tag {
      background: rgba(59, 130, 246, 0.1);
      border: 1px solid rgba(59, 130, 246, 0.15);
      border-radius: 6px;
      padding: 2px 10px;
      font-size: 10px;
      color: #93c5fd;
    }

    @keyframes pulse-dot {
      0%, 100% { opacity: 1; }
      50% { opacity: 0.3; }
    }

    .pulsing-dot {
      animation: pulse-dot 2s infinite;
    }

    @media (max-width: 768px) {
      .stat-grid {
        grid-template-columns: 1fr 1fr;
      }
      .main-grid {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
<body>
  <div class="min-h-screen flex flex-col">
    <!-- Header -->
    <header class="glass border-b border-blue-500/10 sticky top-0 z-50">
      <div class="max-w-7xl mx-auto px-4 py-4 flex items-center justify-between">
        <div class="flex items-center gap-3">
          <div class="w-10 h-10 rounded-xl bg-gradient-to-br from-blue-600 to-blue-800 flex items-center justify-center">
            <i class="fas fa-shield-halved text-white text-lg"></i>
          </div>
          <div>
            <h1 class="text-lg font-bold gradient-text">J1BTNL CONFIG</h1>
            <p class="text-[10px] text-blue-400/60 tracking-wider">SNI INJECTION PROTOCOL</p>
          </div>
        </div>
        <div class="flex items-center gap-3">
          <div class="flex items-center gap-2 bg-blue-500/10 px-3 py-1.5 rounded-full border border-blue-500/20">
            <div class="w-2 h-2 rounded-full bg-green-400 pulsing-dot"></div>
            <span class="text-xs font-medium text-blue-300">ONLINE</span>
          </div>
        </div>
      </div>
    </header>

    <main class="flex-1 max-w-7xl w-full mx-auto px-4 py-6">
      <!-- Stats -->
      <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
        <div class="stat-card">
          <p class="text-xs text-blue-400/60 font-medium mb-1">UPTIME</p>
          <p id="uptime-val" class="text-lg font-bold text-blue-300">--</p>
        </div>
        <div class="stat-card">
          <p class="text-xs text-blue-400/60 font-medium mb-1">MEMORY</p>
          <p class="text-lg font-bold text-blue-300">${Math.round(process.memoryUsage().heapUsed / 1024 / 1024)} MB</p>
        </div>
        <div class="stat-card">
          <p class="text-xs text-blue-400/60 font-medium mb-1">PROVIDER</p>
          <p id="provider-val" class="text-lg font-bold text-blue-300">--</p>
        </div>
        <div class="stat-card">
          <p class="text-xs text-blue-400/60 font-medium mb-1">SERVER</p>
          <p id="server-val" class="text-lg font-bold text-blue-300">--</p>
        </div>
      </div>

      <!-- Main Content -->
      <div class="glass rounded-xl p-6 neon-glow">
        <div class="flex items-center gap-3 border-b border-blue-500/10 pb-4 mb-6">
          <i class="fas fa-key text-blue-400"></i>
          <h2 class="text-base font-bold text-blue-300">VLESS / TROJAN GENERATOR</h2>
        </div>

        <div class="grid md:grid-cols-2 gap-8">
          <!-- Left Panel -->
          <div class="space-y-4">
            <!-- UUID -->
            <div>
              <label class="text-xs text-blue-400/60 font-medium block mb-1.5">UUID / PASSWORD</label>
              <div class="flex gap-2">
                <input id="uuidInput" type="text" value="853b8456-0c0b-4bfa-b3b4-b2619248a9bc" class="input-dark">
                <button id="randomUuidBtn" class="px-3 py-2 bg-blue-600/20 border border-blue-500/20 rounded-lg text-blue-400 hover:bg-blue-600 hover:text-white transition text-sm whitespace-nowrap">
                  <i class="fas fa-shuffle"></i>
                </button>
              </div>
            </div>

            <!-- Host -->
            <div>
              <label class="text-xs text-blue-400/60 font-medium block mb-1.5">HOST / DOMAIN</label>
              <input id="hostInput" type="text" value="${currentHost}" class="input-dark">
            </div>

            <!-- Port -->
            <div>
              <label class="text-xs text-blue-400/60 font-medium block mb-1.5">PORT</label>
              <input id="portInput" type="text" value="443" class="input-dark">
            </div>

            <!-- Path -->
            <div>
              <label class="text-xs text-blue-400/60 font-medium block mb-1.5">PATH</label>
              <div class="flex gap-2">
                <select id="pathSelect" class="input-dark flex-1">
                  <option value="/ID">🇮🇩 /ID (Indonesia)</option>
                  <option value="/SG">🇸🇬 /SG (Singapore)</option>
                  <option value="/JP">🇯🇵 /JP (Japan)</option>
                  <option value="/US">🇺🇸 /US (USA)</option>
                  <option value="/EUROPE">🇪🇺 /EUROPE</option>
                  <option value="/ASIA">🌏 /ASIA (Asia)</option>
                  <option value="/ALL">🌍 /ALL (Global)</option>
                  <option value="/AMERICA">🌎 /AMERICA</option>
                </select>
                <input id="pathInput" type="text" value="/ID" class="input-dark flex-1">
              </div>
            </div>

            <!-- SNI Toggle -->
            <div>
              <label class="text-xs text-blue-400/60 font-medium block mb-1.5">SNI (Server Name Indication)</label>
              <div class="flex items-center gap-4">
                <div class="flex items-center gap-3">
                  <span class="text-sm text-blue-300/60">OFF</span>
                  <div id="sniToggle" class="toggle-switch active">
                    <div class="toggle-dot"></div>
                  </div>
                  <span class="text-sm text-blue-300/60">ON</span>
                </div>
                <span id="sniStatus" class="text-xs text-blue-400 font-medium">Enabled</span>
              </div>
              <div id="sniInputContainer" class="mt-2">
                <select id="sniSelect" class="input-dark">
                  <option value="business.whatsapp.com">📱 business.whatsapp.com</option>
                  <option value="media-sin6-3.cdn.whatsapp.net">📡 media-sin6-3.cdn.whatsapp.net</option>
                  <option value="c.whatsapp.com">💬 c.whatsapp.com</option>
                  <option value="web.whatsapp.com">🌐 web.whatsapp.com</option>
                  <option value="v.whatsapp.net">📞 v.whatsapp.net</option>
                  <option value="live.iflix.com">🎬 live.iflix.com</option>
                  <option value="custom">✏️ CUSTOM SNI</option>
                </select>
                <input id="sniInput" type="text" value="business.whatsapp.com" class="input-dark mt-2 hidden" placeholder="Ketik SNI Custom...">
              </div>
            </div>

            <!-- Mode Connection -->
            <div>
              <label class="text-xs text-blue-400/60 font-medium block mb-1.5">
                <i class="fas fa-plug text-blue-400 mr-1"></i> MODE KONEKSI
              </label>
              <select id="modeSelect" class="input-dark">
                <option value="sni">🔒 TLS + SNI (Default)</option>
                <option value="tls">🔐 TLS tanpa SNI</option>
                <option value="tcp">📡 TCP Biasa</option>
              </select>
            </div>

            <!-- Remark -->
            <div>
              <label class="text-xs text-blue-400/60 font-medium block mb-1.5">REMARK</label>
              <input id="remarkInput" type="text" value="J1BTNL" class="input-dark">
            </div>

            <button id="generateBtn" class="btn-primary w-full text-white font-semibold py-3 px-4 rounded-lg text-sm flex items-center justify-center gap-2">
              <i class="fas fa-bolt"></i> GENERATE CONFIG
            </button>
          </div>

          <!-- Right Panel -->
          <div class="space-y-4">
            <div class="flex items-center gap-2 border-b border-blue-500/10 pb-2">
              <i class="fas fa-code text-blue-400"></i>
              <span class="text-sm font-semibold text-blue-300">HASIL GENERATE</span>
            </div>

            <!-- VLESS -->
            <div>
              <div class="flex items-center justify-between mb-1.5">
                <span class="badge badge-blue">VLESS PROTOCOL</span>
                <button onclick="copyText(document.getElementById('vlessOutput').textContent)" class="text-xs text-blue-400/60 hover:text-blue-400 transition flex items-center gap-1">
                  <i class="far fa-copy"></i> COPY
                </button>
              </div>
              <div id="vlessOutput" class="output-box">Loading...</div>
            </div>

            <!-- TROJAN -->
            <div>
              <div class="flex items-center justify-between mb-1.5">
                <span class="badge badge-blue">TROJAN PROTOCOL</span>
                <button onclick="copyText(document.getElementById('trojanOutput').textContent)" class="text-xs text-blue-400/60 hover:text-blue-400 transition flex items-center gap-1">
                  <i class="far fa-copy"></i> COPY
                </button>
              </div>
              <div id="trojanOutput" class="output-box">Loading...</div>
            </div>

            <!-- CLASH META -->
            <div>
              <div class="flex items-center justify-between mb-1.5">
                <span class="badge badge-blue">CLASH META / V2RAY</span>
                <button onclick="copyText(document.getElementById('clashOutput').textContent)" class="text-xs text-blue-400/60 hover:text-blue-400 transition flex items-center gap-1">
                  <i class="far fa-copy"></i> COPY
                </button>
              </div>
              <pre id="clashOutput" class="output-box" style="max-height:150px;">Loading...</pre>
            </div>

            <!-- Provider Info -->
            <div class="flex flex-wrap gap-2 pt-2">
              <span class="provider-tag"><i class="fas fa-server mr-1"></i> Provider: <span id="providerDisplay">Detecting...</span></span>
              <span class="provider-tag"><i class="fas fa-globe mr-1"></i> Server: <span id="serverDisplay">Detecting...</span></span>
              <span class="provider-tag"><i class="fas fa-signal mr-1"></i> Mode: <span id="modeDisplay">SNI</span></span>
            </div>
          </div>
        </div>
      </div>
    </main>

    <footer class="border-t border-blue-500/10 bg-[#0a0a0f] px-4 py-4">
      <div class="max-w-7xl mx-auto flex flex-col sm:flex-row items-center justify-between gap-2 text-xs text-blue-400/40">
        <p>&copy; ${new Date().getFullYear()} J1BTNL CONFIG SNI. ALL SYSTEMS OPERATIONAL.</p>
        <p class="flex items-center gap-2"><i class="fas fa-shield-check text-blue-500/60"></i> SECURED BY END-TO-END KERNEL TUNNEL</p>
      </div>
    </footer>
  </div>

  <div id="toast" class="fixed bottom-6 right-6 bg-blue-600 text-white font-semibold px-5 py-3 rounded-xl shadow-2xl opacity-0 pointer-events-none transition-all duration-300 transform translate-y-4 text-sm flex items-center gap-2 border border-blue-400/30">
    <i class="fas fa-check-circle"></i> COPIED TO CLIPBOARD
  </div>

  <script>
    // ==================== COPY FUNCTION ====================
    function copyText(text) {
      navigator.clipboard.writeText(text).then(() => {
        const toast = document.getElementById('toast');
        toast.classList.remove('opacity-0', 'pointer-events-none', 'translate-y-4');
        toast.classList.add('opacity-100', 'translate-y-0');
        setTimeout(() => {
          toast.classList.remove('opacity-100', 'translate-y-0');
          toast.classList.add('opacity-0', 'pointer-events-none', 'translate-y-4');
        }, 2500);
      });
    }

    // ==================== DETECT PROVIDER & SERVER ====================
    async function detectProviderAndServer() {
      try {
        const response = await fetch('https://ipapi.co/json/');
        const data = await response.json();
        
        const provider = data.org || data.asn || 'Unknown';
        const server = data.city ? data.city + ', ' + data.country_name : data.country_name || 'Unknown';
        
        document.getElementById('provider-val').textContent = provider;
        document.getElementById('server-val').textContent = server;
        document.getElementById('providerDisplay').textContent = provider;
        document.getElementById('serverDisplay').textContent = server;
      } catch (error) {
        console.error('Error detecting provider:', error);
        document.getElementById('provider-val').textContent = '--';
        document.getElementById('server-val').textContent = '--';
        document.getElementById('providerDisplay').textContent = 'N/A';
        document.getElementById('serverDisplay').textContent = 'N/A';
      }
    }

    // ==================== UPTIME & CLOCK ====================
    let totalSeconds = ${Math.floor(process.uptime())};

    function updateDashboard() {
      const d = Math.floor(totalSeconds / 86400);
      const h = Math.floor((totalSeconds % 86400) / 3600);
      const m = Math.floor((totalSeconds % 3600) / 60);
      const s = totalSeconds % 60;

      const uptimeStr = (d > 0 ? d + 'd ' : '') + 
                        String(h).padStart(2, '0') + 'h ' + 
                        String(m).padStart(2, '0') + 'm ' + 
                        String(s).padStart(2, '0') + 's';
      
      document.getElementById('uptime-val').textContent = uptimeStr;
    }

    updateDashboard();
    setInterval(() => {
      totalSeconds++;
      updateDashboard();
    }, 1000);

    // ==================== GENERATOR ====================
    function generateUUID() {
      return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        const r = Math.random() * 16 | 0;
        const v = c === 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
      });
    }

    function generateTrojanPass() {
      const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
      let pass = '';
      for (let i = 0; i < 36; i++) {
        if (i === 8 || i === 13 || i === 18 || i === 23) {
          pass += '-';
        } else {
          pass += chars.charAt(Math.floor(Math.random() * chars.length));
        }
      }
      return pass;
    }

    function generateAccounts() {
      try {
        const uuid = document.getElementById('uuidInput').value.trim() || '853b8456-0c0b-4bfa-b3b4-b2619248a9bc';
        const host = document.getElementById('hostInput').value.trim() || '${currentHost}';
        const port = document.getElementById('portInput').value.trim() || '443';
        const path = document.getElementById('pathInput').value.trim() || '/ID';
        const sni = document.getElementById('sniInput').value.trim() || 'business.whatsapp.com';
        const mode = document.getElementById('modeSelect').value || 'sni';
        const remark = document.getElementById('remarkInput').value.trim() || 'J1BTNL';
        const useSNI = document.getElementById('sniToggle').classList.contains('active');

        const encodedPath = encodeURIComponent(path);
        const encodedRemark = encodeURIComponent(remark);

        let flowParam = '';
        let securityParam = '';
        let sniParam = '';

        if (mode === 'sni' && useSNI) {
          securityParam = '&security=tls';
          sniParam = '&sni=' + encodeURIComponent(sni);
          flowParam = '&fp=randomized';
        } else if (mode === 'tls') {
          securityParam = '&security=tls';
          sniParam = '';
          flowParam = '&fp=randomized';
        } else {
          securityParam = '';
          sniParam = '';
          flowParam = '';
        }

        // VLESS
        const vlessUrl = 'vless://' + uuid + '@' + host + ':' + port +
                         '?encryption=none' + securityParam + sniParam +
                         flowParam + '&type=ws&host=' + host +
                         '&path=' + encodedPath + '#' + encodedRemark;

        // TROJAN
        const trojanPass = generateTrojanPass();
        let trojanUrl = 'trojan://' + trojanPass + '@' + host + ':' + port;

        if (mode === 'sni' && useSNI) {
          trojanUrl += '?security=tls&sni=' + encodeURIComponent(sni) + '&type=ws&host=' + host + '&path=' + encodedPath + '#' + encodedRemark;
        } else if (mode === 'tls') {
          trojanUrl += '?security=tls&type=ws&host=' + host + '&path=' + encodedPath + '#' + encodedRemark;
        } else {
          trojanUrl += '?type=ws&host=' + host + '&path=' + encodedPath + '#' + encodedRemark;
        }

        document.getElementById('vlessOutput').textContent = vlessUrl;
        document.getElementById('trojanOutput').textContent = trojanUrl;

        // CLASH META
        let clashConfig = '- name: "' + remark + ' VLESS"\\n' +
                          '  type: vless\\n' +
                          '  server: ' + host + '\\n' +
                          '  port: ' + port + '\\n' +
                          '  uuid: ' + uuid + '\\n' +
                          '  network: ws\\n';

        if (mode === 'sni' && useSNI) {
          clashConfig += '  tls: true\\n' +
                         '  sni: "' + sni + '"\\n' +
                         '  client-fingerprint: randomized\\n';
        } else if (mode === 'tls') {
          clashConfig += '  tls: true\\n' +
                         '  client-fingerprint: randomized\\n';
        } else {
          clashConfig += '  tls: false\\n';
        }

        clashConfig += '  udp: true\\n' +
                       '  ws-opts:\\n' +
                       '    path: "' + path + '"\\n' +
                       '    headers:\\n' +
                       '      host: "' + host + '"\\n\\n' +
                       '- name: "' + remark + ' TROJAN"\\n' +
                       '  type: trojan\\n' +
                       '  server: ' + host + '\\n' +
                       '  port: ' + port + '\\n' +
                       '  password: ' + trojanPass + '\\n' +
                       '  network: ws\\n';

        if (mode === 'sni' && useSNI) {
          clashConfig += '  tls: true\\n' +
                         '  sni: "' + sni + '"\\n';
        } else if (mode === 'tls') {
          clashConfig += '  tls: true\\n';
        } else {
          clashConfig += '  tls: false\\n';
        }

        clashConfig += '  udp: true\\n' +
                       '  ws-opts:\\n' +
                       '    path: "' + path + '"\\n' +
                       '    headers:\\n' +
                       '      host: "' + host + '"';

        document.getElementById('clashOutput').textContent = clashConfig;
        document.getElementById('modeDisplay').textContent = mode.toUpperCase();
      } catch (err) {
        console.error('Generator Error:', err);
      }
    }

    // ==================== EVENT LISTENERS ====================
    // SNI Toggle
    const sniToggle = document.getElementById('sniToggle');
    const sniStatus = document.getElementById('sniStatus');
    const sniSelect = document.getElementById('sniSelect');
    const sniInput = document.getElementById('sniInput');
    const sniInputContainer = document.getElementById('sniInputContainer');

    sniToggle.addEventListener('click', function() {
      this.classList.toggle('active');
      const isActive = this.classList.contains('active');
      sniStatus.textContent = isActive ? 'Enabled' : 'Disabled';
      sniStatus.style.color = isActive ? '#60a5fa' : '#9ca3af';
      document.getElementById('modeDisplay').textContent = isActive ? 'SNI' : 'NO-SNI';
      generateAccounts();
    });

    // SNI Select
    sniSelect.addEventListener('change', function() {
      if (this.value === 'custom') {
        sniInput.classList.remove('hidden');
        sniInput.value = '';
        sniInput.focus();
      } else {
        sniInput.classList.add('hidden');
        sniInput.value = this.value;
        generateAccounts();
      }
    });

    // All inputs
    ['uuidInput', 'hostInput', 'portInput', 'pathInput', 'remarkInput', 'modeSelect'].forEach(id => {
      document.getElementById(id).addEventListener('input', generateAccounts);
      document.getElementById(id).addEventListener('change', generateAccounts);
    });

    // Path Select
    document.getElementById('pathSelect').addEventListener('change', function() {
      document.getElementById('pathInput').value = this.value;
      generateAccounts();
    });

    // Random UUID
    document.getElementById('randomUuidBtn').addEventListener('click', function() {
      document.getElementById('uuidInput').value = generateUUID();
      generateAccounts();
    });

    // Generate Button
    document.getElementById('generateBtn').addEventListener('click', function(e) {
      e.preventDefault();
      generateAccounts();
    });

    // ==================== INIT ====================
    detectProviderAndServer();
    setTimeout(generateAccounts, 500);
  </script>
</body>
</html>
    `;
  }

  // ==================== PROXY LIST MANAGEMENT ====================

  async getKVPrxList(kvPrxUrl = KV_PRX_URL) {
    if (!kvPrxUrl) {
      throw new Error("No URL Provided!");
    }

    try {
      const kvPrx = await fetch(kvPrxUrl);
      if (kvPrx.status == 200) {
        return await kvPrx.json();
      } else {
        console.error(`Failed to fetch KV proxy list: ${kvPrx.status}`);
        return {};
      }
    } catch (error) {
      console.error('Error fetching KV proxy list:', error);
      return {};
    }
  }

  async getPrxList(prxBankUrl) {
    if (!prxBankUrl) {
      return [];
    }

    try {
      const response = await fetch(prxBankUrl);
      if (response.status === 200) {
        const data = await response.json();
        
        return data.map(proxy => {
          const ip = proxy.prxIP || proxy.ip || proxy.server;
          const port = proxy.prxPort || proxy.port;
          const country = proxy.country || proxy.cc || 'XX';
          
          if (!ip || !port) {
            console.warn('Invalid proxy format:', proxy);
            return null;
          }
          
          return {
            prxIP: ip,
            prxPort: port,
            country: country.toUpperCase()
          };
        }).filter(Boolean);
      } else {
        console.error(`Failed to fetch proxy list: ${response.status}`);
        return [];
      }
    } catch (error) {
      console.error('Error fetching proxy list:', error);
      return [];
    }
  }

  // ==================== REVERSE PROXY ====================

  async reverseWeb(request, response, target, targetPath) {
    try {
      const targetUrl = new URL(request.url);
      const targetChunk = target.split(":");

      targetUrl.hostname = targetChunk[0];
      targetUrl.port = targetChunk[1]?.toString() || "443";
      targetUrl.pathname = targetPath || targetUrl.pathname;

      const options = {
        hostname: targetUrl.hostname,
        port: targetUrl.port,
        path: targetUrl.pathname + targetUrl.search,
        method: request.method,
        headers: { ...request.headers }
      };

      options.headers['host'] = targetUrl.hostname;
      options.headers['x-forwarded-host'] = request.headers.host;

      const proxyReq = (targetUrl.protocol === 'https:' ? https : http).request(options, (proxyRes) => {
        response.writeHead(proxyRes.statusCode, {
          ...Object.fromEntries(Object.entries(this.CORS_HEADER_OPTIONS)),
          ...Object.fromEntries(Object.entries(proxyRes.headers)),
          'x-proxied-by': 'Railway Gateway'
        });

        proxyRes.pipe(response);
      });

      proxyReq.on('error', (err) => {
        console.error('Proxy error:', err);
        response.writeHead(500);
        response.end('Proxy error');
      });

      if (request.method !== 'GET' && request.method !== 'HEAD') {
        let body = [];
        request.on('data', (chunk) => {
          body.push(chunk);
        }).on('end', () => {
          proxyReq.write(Buffer.concat(body));
          proxyReq.end();
        });
      } else {
        proxyReq.end();
      }
    } catch (err) {
      console.error('Reverse web error:', err);
      response.writeHead(500);
      response.end('Internal server error');
    }
  }

  // ==================== WEBSOCKET HANDLERS ====================

  async handleWebSocketConnection(ws, request) {
    try {
      const parsedUrl = url.parse(request.url, true);
      const path = parsedUrl.pathname;
      const host = request.headers.host || 'localhost';

      const queryParams = parsedUrl.query || {};
      if (queryParams.mode) {
        this.connectionMode = queryParams.mode;
      }
      if (queryParams.sni) {
        this.sniHost = queryParams.sni;
      }
      if (queryParams.useSNI) {
        this.useSNI = queryParams.useSNI === 'true';
      }

      console.log(`WebSocket request path: ${path} from ${request.socket.remoteAddress}`);
      console.log(`Connection Mode: ${this.connectionMode}, SNI: ${this.sniHost}, Use SNI: ${this.useSNI}`);

      const proxyListMatch = path.match(/^\/PROXYLIST\/([A-Z]{2}(,[A-Z]{2})*)$/i);
      if (proxyListMatch) {
        const countryCodes = proxyListMatch[1].toUpperCase().split(",");
        const proxies = await this.getPrxList(process.env.PRX_BANK_URL);

        if (proxies.length === 0) {
          const kvPrx = await this.getKVPrxList();
          const availableCountries = countryCodes.filter(code => kvPrx[code] && kvPrx[code].length > 0);
          if (availableCountries.length === 0) {
            ws.close(1000, `No proxies available for countries: ${countryCodes.join(",")}`);
            return;
          }
          const prxKey = availableCountries[Math.floor(Math.random() * availableCountries.length)];
          this.prxIP = kvPrx[prxKey][Math.floor(Math.random() * kvPrx[prxKey].length)];
        } else {
          const filteredProxies = proxies.filter(proxy => countryCodes.includes(proxy.country));
          if (filteredProxies.length === 0) {
            ws.close(1000, `No proxies available for countries: ${countryCodes.join(",")}`);
            return;
          }
          const randomProxy = filteredProxies[Math.floor(Math.random() * filteredProxies.length)];
          this.prxIP = `${randomProxy.prxIP}:${randomProxy.prxPort}`;
        }

        console.log(`Selected Proxy (/PROXYLIST/${countryCodes.join(",")}): ${this.prxIP}`);
        await this.websocketHandler(ws);
        return;
      }

      const allMatch = path.match(/^\/ALL(\d+)?$/i);
      if (allMatch) {
        const index = allMatch[1] ? parseInt(allMatch[1], 10) - 1 : null;
        const proxies = await this.getPrxList(process.env.PRX_BANK_URL);

        if (proxies.length === 0) {
          const kvPrx = await this.getKVPrxList();
          const allProxies = Object.values(kvPrx).flat();
          if (allProxies.length === 0) {
            ws.close(1000, `No proxies available for /ALL${index !== null ? index + 1 : ""}`);
            return;
          }
          this.prxIP = allProxies[Math.floor(Math.random() * allProxies.length)];
        } else {
          let selectedProxy;
          
          if (index === null) {
            selectedProxy = proxies[Math.floor(Math.random() * proxies.length)];
          } else {
            const groupedByCountry = proxies.reduce((acc, proxy) => {
              if (!acc[proxy.country]) acc[proxy.country] = [];
              acc[proxy.country].push(proxy);
              return acc;
            }, {});

            const proxiesByIndex = [];
            for (const country in groupedByCountry) {
              const countryProxies = groupedByCountry[country];
              if (index < countryProxies.length) {
                proxiesByIndex.push(countryProxies[index]);
              }
            }

            if (proxiesByIndex.length === 0) {
              ws.close(1000, `No proxy at index ${index + 1} for any country`);
              return;
            }

            selectedProxy = proxiesByIndex[Math.floor(Math.random() * proxiesByIndex.length)];
          }

          this.prxIP = `${selectedProxy.prxIP}:${selectedProxy.prxPort}`;
        }

        console.log(`Selected Proxy (/ALL${index !== null ? index + 1 : ""}): ${this.prxIP}`);
        await this.websocketHandler(ws);
        return;
      }

      const putarMatch = path.match(/^\/PUTAR(\d+)?$/i);
      if (putarMatch) {
        const countryCount = putarMatch[1] ? parseInt(putarMatch[1], 10) : null;
        const proxies = await this.getPrxList(process.env.PRX_BANK_URL);

        if (proxies.length === 0) {
          const kvPrx = await this.getKVPrxList();
          const countries = Object.keys(kvPrx).filter(code => kvPrx[code] && kvPrx[code].length > 0);
          
          if (countries.length === 0) {
            ws.close(1000, `No proxies available for /PUTAR${countryCount || ""}`);
            return;
          }

          let selectedCountries;
          if (countryCount === null) {
            selectedCountries = countries;
          } else {
            const shuffled = [...countries].sort(() => Math.random() - 0.5);
            selectedCountries = shuffled.slice(0, Math.min(countryCount, countries.length));
          }

          const prxKey = selectedCountries[Math.floor(Math.random() * selectedCountries.length)];
          this.prxIP = kvPrx[prxKey][Math.floor(Math.random() * kvPrx[prxKey].length)];
        } else {
          const groupedByCountry = proxies.reduce((acc, proxy) => {
            if (!acc[proxy.country]) acc[proxy.country] = [];
            acc[proxy.country].push(proxy);
            return acc;
          }, {});

          const countries = Object.keys(groupedByCountry);
          if (countries.length === 0) {
            ws.close(1000, `No proxies available`);
            return;
          }

          let selectedCountries;
          if (countryCount === null) {
            selectedCountries = countries;
          } else {
            const shuffled = [...countries].sort(() => Math.random() - 0.5);
            selectedCountries = shuffled.slice(0, Math.min(countryCount, countries.length));
          }

          const selectedProxies = selectedCountries.map(country => {
            const countryProxies = groupedByCountry[country];
            return countryProxies[Math.floor(Math.random() * countryProxies.length)];
          });

          const randomProxy = selectedProxies[Math.floor(Math.random() * selectedProxies.length)];
          this.prxIP = `${randomProxy.prxIP}:${randomProxy.prxPort}`;
        }

        console.log(`Selected Proxy (/PUTAR${countryCount || ""}): ${this.prxIP}`);
        await this.websocketHandler(ws);
        return;
      }

      const regionMatch = path.match(/^\/([A-Z]+)(\d+)?$/i);
      if (regionMatch) {
        const regionKey = regionMatch[1].toUpperCase();
        const index = regionMatch[2] ? parseInt(regionMatch[2], 10) - 1 : null;
        
        if (REGION_MAP[regionKey] !== undefined) {
          const countries = REGION_MAP[regionKey];
          const proxies = await this.getPrxList(process.env.PRX_BANK_URL);

          if (proxies.length === 0) {
            const kvPrx = await this.getKVPrxList();
            let availableProxies = [];
            
            if (regionKey === "GLOBAL") {
              availableProxies = Object.values(kvPrx).flat();
            } else {
              for (const country of countries) {
                if (kvPrx[country] && kvPrx[country].length > 0) {
                  availableProxies.push(...kvPrx[country]);
                }
              }
            }

            if (availableProxies.length === 0) {
              ws.close(1000, `No proxies available for region: ${regionKey}`);
              return;
            }

            if (index === null) {
              this.prxIP = availableProxies[Math.floor(Math.random() * availableProxies.length)];
            } else {
              if (index < 0 || index >= availableProxies.length) {
                ws.close(1000, `Index ${index + 1} out of range for region ${regionKey}`);
                return;
              }
              this.prxIP = availableProxies[index];
            }
          } else {
            const filteredProxies = regionKey === "GLOBAL" 
              ? proxies
              : proxies.filter(p => countries.includes(p.country));

            if (filteredProxies.length === 0) {
              ws.close(1000, `No proxies available for region: ${regionKey}`);
              return;
            }

            let selectedProxy;
            if (index === null) {
              selectedProxy = filteredProxies[Math.floor(Math.random() * filteredProxies.length)];
            } else {
              if (index < 0 || index >= filteredProxies.length) {
                ws.close(1000, `Index ${index + 1} out of range for region ${regionKey}`);
                return;
              }
              selectedProxy = filteredProxies[index];
            }

            this.prxIP = `${selectedProxy.prxIP}:${selectedProxy.prxPort}`;
          }

          console.log(`Selected Proxy (/${regionKey}${index !== null ? index + 1 : ""}): ${this.prxIP}`);
          await this.websocketHandler(ws);
          return;
        }
      }

      const countryMatch = path.match(/^\/([A-Z]{2})(\d+)?$/);
      if (countryMatch) {
        const countryCode = countryMatch[1].toUpperCase();
        const index = countryMatch[2] ? parseInt(countryMatch[2], 10) - 1 : null;
        const proxies = await this.getPrxList(process.env.PRX_BANK_URL);
        
        if (proxies.length === 0) {
          const kvPrx = await this.getKVPrxList();
          if (!kvPrx[countryCode] || kvPrx[countryCode].length === 0) {
            ws.close(1000, `No proxies available for country: ${countryCode}`);
            return;
          }

          if (index === null) {
            this.prxIP = kvPrx[countryCode][Math.floor(Math.random() * kvPrx[countryCode].length)];
          } else {
            if (index < 0 || index >= kvPrx[countryCode].length) {
              ws.close(1000, `Index ${index + 1} out of range for country ${countryCode}`);
              return;
            }
            this.prxIP = kvPrx[countryCode][index];
          }
        } else {
          const filteredProxies = proxies.filter(proxy => proxy.country === countryCode);
          if (filteredProxies.length === 0) {
            ws.close(1000, `No proxies available for country: ${countryCode}`);
            return;
          }

          let selectedProxy;
          if (index === null) {
            selectedProxy = filteredProxies[Math.floor(Math.random() * filteredProxies.length)];
          } else {
            if (index < 0 || index >= filteredProxies.length) {
              ws.close(1000, `Index ${index + 1} out of range for country ${countryCode}`);
              return;
            }
            selectedProxy = filteredProxies[index];
          }

          this.prxIP = `${selectedProxy.prxIP}:${selectedProxy.prxPort}`;
        }

        console.log(`Selected Proxy (/${countryCode}${index !== null ? index + 1 : ""}): ${this.prxIP}`);
        await this.websocketHandler(ws);
        return;
      }

      const ipPortMatch = path.match(/^\/(.+[:=-]\\d+)$/);
      if (ipPortMatch) {
        this.prxIP = ipPortMatch[1].replace(/[=:-]/, ":");
        console.log(`Direct Proxy IP: ${this.prxIP}`);
        await this.websocketHandler(ws);
        return;
      }

      if (path.length === 4 || path.includes(',')) {
        const prxKeys = path.replace("/", "").toUpperCase().split(",");
        const prxKey = prxKeys[Math.floor(Math.random() * prxKeys.length)];
        const kvPrx = await this.getKVPrxList();

        if (kvPrx[prxKey] && kvPrx[prxKey].length > 0) {
          this.prxIP = kvPrx[prxKey][Math.floor(Math.random() * kvPrx[prxKey].length)];
          console.log(`Legacy Proxy (/${prxKeys.join(",")}): ${this.prxIP}`);
          await this.websocketHandler(ws);
          return;
        } else {
          ws.close(1000, `No proxies available for country: ${prxKey}`);
          return;
        }
      }

      ws.close(1000, "Invalid WebSocket path format");
    } catch (err) {
      console.error('WebSocket connection error:', err);
      ws.close(1011, 'Internal server error');
    }
  }

  async websocketHandler(ws) {
    let addressLog = "";
    let portLog = "";
    const log = (info, event) => {
      console.log(`[${addressLog}:${portLog}] ${info}`, event || "");
    };

    let remoteSocketWrapper = { value: null };

    ws.on('message', async (message) => {
      try {
        const chunk = Buffer.from(message);

        if (remoteSocketWrapper.value) {
          remoteSocketWrapper.value.write(chunk);
          return;
        }

        const protocol = await this.protocolSniffer(chunk);
        let protocolHeader;

        if (protocol === horse) {
          protocolHeader = this.readHorseHeader(chunk);
        } else if (protocol === flash) {
          protocolHeader = this.readFlashHeader(chunk);
        } else if (protocol === "ss") {
          protocolHeader = this.readSsHeader(chunk);
        } else {
          throw new Error("Unknown Protocol!");
        }

        addressLog = protocolHeader.addressRemote;
        portLog = `${protocolHeader.portRemote} -> ${protocolHeader.isUDP ? "UDP" : "TCP"}`;

        if (protocolHeader.hasError) {
          throw new Error(protocolHeader.message);
        }

        if (protocolHeader.isUDP) {
          return await this.handleUDPOutbound(
            protocolHeader.addressRemote,
            protocolHeader.portRemote,
            chunk.slice(protocolHeader.rawDataIndex),
            ws,
            protocolHeader.version,
            log
          );
        }

        this.handleTCPOutBound(
          remoteSocketWrapper,
          protocolHeader.addressRemote,
          protocolHeader.portRemote,
          protocolHeader.rawClientData,
          ws,
          protocolHeader.version,
          log
        );
      } catch (err) {
        console.error('Error processing WebSocket message:', err);
        ws.close(1011, err.message);
      }
    });

    ws.on('close', () => {
      if (remoteSocketWrapper.value) {
        remoteSocketWrapper.value.end();
      }
      this.cleanupUDPConnections(ws);
      log('WebSocket closed');
    });

    ws.on('error', (err) => {
      console.error('WebSocket error:', err);
      this.cleanupUDPConnections(ws);
    });
  }

  // ==================== PROTOCOL SNIFFERS ====================

  async protocolSniffer(buffer) {
    if (buffer.length >= 62) {
      const horseDelimiter = buffer.slice(56, 60);
      if (horseDelimiter[0] === 0x0d && horseDelimiter[1] === 0x0a) {
        if (horseDelimiter[2] === 0x01 || horseDelimiter[2] === 0x03 || horseDelimiter[2] === 0x7f) {
          if (horseDelimiter[3] === 0x01 || horseDelimiter[3] === 0x03 || horseDelimiter[3] === 0x04) {
            return horse;
          }
        }
      }
    }

    const flashDelimiter = buffer.slice(1, 17);
    const hex = flashDelimiter.toString('hex');
    if (hex.match(/^[0-9a-f]{8}[0-9a-f]{4}4[0-9a-f]{3}[89ab][0-9a-f]{3}[0-9a-f]{12}$/i)) {
      return flash;
    }

    return "ss";
  }

  // ==================== TCP OUTBOUND HANDLER ====================

  async handleTCPOutBound(remoteSocket, addressRemote, portRemote, rawClientData, webSocket, responseHeader, log) {
    const connectAndWrite = (address, port) => {
      return new Promise((resolve, reject) => {
        let tcpSocket;
        const mode = this.connectionMode || "sni";
        const useSNI = this.useSNI !== false;
        const sniHost = this.sniHost || "business.whatsapp.com";

        log(`Connecting with mode: ${mode}, SNI: ${useSNI ? sniHost : 'Disabled'}`);

        try {
          if (mode === "sni" && useSNI) {
            tcpSocket = tls.connect({
              host: address,
              port: port,
              servername: sniHost,
              rejectUnauthorized: false
            }, () => {
              log(`TLS + SNI connected to ${address}:${port} with SNI: ${sniHost}`);
              tcpSocket.write(rawClientData);
              resolve(tcpSocket);
            });
          } else if (mode === "tls" || (mode === "sni" && !useSNI)) {
            tcpSocket = tls.connect({
              host: address,
              port: port,
              rejectUnauthorized: false
            }, () => {
              log(`TLS (no SNI) connected to ${address}:${port}`);
              tcpSocket.write(rawClientData);
              resolve(tcpSocket);
            });
          } else {
            tcpSocket = net.createConnection({
              host: address,
              port: port
            }, () => {
              log(`TCP connected to ${address}:${port}`);
              tcpSocket.write(rawClientData);
              resolve(tcpSocket);
            });
          }

          tcpSocket.on('error', (err) => {
            log(`Connection error: ${err.message}`);
            reject(err);
          });

        } catch (err) {
          log(`Connection creation error: ${err.message}`);
          reject(err);
        }
      });
    };

    const retry = async () => {
      try {
        const proxyAddress = this.prxIP.split(/[:=-]/)[0] || addressRemote;
        const proxyPort = parseInt(this.prxIP.split(/[:=-]/)[1]) || portRemote;
        
        log(`Retrying with proxy: ${proxyAddress}:${proxyPort}`);
        const tcpSocket = await connectAndWrite(proxyAddress, proxyPort);
        remoteSocket.value = tcpSocket;
        
        tcpSocket.on('close', () => { 
          log('TCP socket closed');
          webSocket.close(); 
        });
        tcpSocket.on('error', (error) => { 
          log(`TCP socket error: ${error.message}`);
          webSocket.close(); 
        });

        this.remoteSocketToWS(tcpSocket, webSocket, responseHeader, null, log);
      } catch (error) {
        log(`Retry failed: ${error.message}`);
        webSocket.close();
      }
    };

    try {
      log(`Connecting directly to ${addressRemote}:${portRemote}`);
      const tcpSocket = await connectAndWrite(addressRemote, portRemote);
      remoteSocket.value = tcpSocket;
      
      tcpSocket.on('close', () => { 
        log('TCP socket closed');
        webSocket.close(); 
      });
      tcpSocket.on('error', (error) => { 
        log(`TCP socket error: ${error.message}`);
        webSocket.close(); 
      });

      this.remoteSocketToWS(tcpSocket, webSocket, responseHeader, retry, log);
    } catch (error) {
      log(`Direct connection failed: ${error.message}, retrying...`);
      await retry();
    }
  }

  // ==================== UDP NATIVE HANDLER ====================

  async handleUDPOutbound(targetAddress, targetPort, dataChunk, webSocket, responseHeader, log) {
    return new Promise((resolve) => {
      try {
        let protocolHeader = responseHeader;
        const connectionKey = `${targetAddress}:${targetPort}:${Date.now()}`;
        const udpSocket = dgram.createSocket('udp4');
        
        this.activeUDPConnections.set(connectionKey, {
          socket: udpSocket,
          webSocket: webSocket
        });
        
        udpSocket.on('error', (error) => {
          console.error(`[UDP Socket Error] ${targetAddress}:${targetPort} ->`, error.message);
          try {
            udpSocket.close();
          } catch (_) {}
          this.activeUDPConnections.delete(connectionKey);
        });

        udpSocket.send(dataChunk, targetPort, targetAddress, (error) => {
          if (error) {
            console.error(`[UDP Send Error]`, error.message);
            try { udpSocket.close(); } catch (_) {}
            this.activeUDPConnections.delete(connectionKey);
            return;
          }
        });
        
        udpSocket.on('message', (message, rinfo) => {
          if (webSocket.readyState === WebSocket.OPEN) {
            if (protocolHeader) {
              const combined = Buffer.concat([Buffer.from(protocolHeader), message]);
              webSocket.send(combined);
              protocolHeader = null;
            } else {
              webSocket.send(message);
            }
          }
        });
        
        udpSocket.on('close', () => {
          this.activeUDPConnections.delete(connectionKey);
        });
        
        let idleTimeout = setTimeout(() => {
          if (udpSocket) {
            try { udpSocket.close(); } catch (_) {}
            this.activeUDPConnections.delete(connectionKey);
          }
        }, 30000);
        
        udpSocket.on('message', () => {
          clearTimeout(idleTimeout);
          idleTimeout = setTimeout(() => {
            if (udpSocket) {
              try { udpSocket.close(); } catch (_) {}
              this.activeUDPConnections.delete(connectionKey);
            }
          }, 30000);
        });
        
      } catch (e) {
        console.error(`Error in UDP handler execution: ${e.message}`);
      }
    });
  }

  cleanupUDPConnections(webSocket) {
    for (const [key, connection] of this.activeUDPConnections.entries()) {
      if (connection.webSocket === webSocket) {
        try {
          connection.socket.close();
        } catch (_) {}
        this.activeUDPConnections.delete(key);
      }
    }
  }

  readSsHeader(ssBuffer) {
    const addressType = ssBuffer[0];
    let addressLength = 0;
    let addressValueIndex = 1;
    let addressValue = "";

    switch (addressType) {
      case 1:
        addressLength = 4;
        addressValue = Array.from(ssBuffer.slice(addressValueIndex, addressValueIndex + addressLength)).join(".");
        break;
      case 3:
        addressLength = ssBuffer[addressValueIndex];
        addressValueIndex += 1;
        addressValue = ssBuffer.slice(addressValueIndex, addressValueIndex + addressLength).toString();
        break;
      case 4:
        addressLength = 16;
        const ipv6 = [];
        for (let i = 0; i < 8; i++) {
          ipv6.push(ssBuffer.readUInt16BE(addressValueIndex + i * 2).toString(16));
        }
        addressValue = ipv6.join(":");
        break;
      default:
        return { hasError: true, message: `Invalid addressType for SS: ${addressType}` };
    }

    if (!addressValue) {
      return { hasError: true, message: `Destination address empty, address type is: ${addressType}` };
    }

    const portIndex = addressValueIndex + addressLength;
    const portRemote = ssBuffer.readUInt16BE(portIndex);
    return {
      hasError: false,
      addressRemote: addressValue,
      addressType: addressType,
      portRemote: portRemote,
      rawDataIndex: portIndex + 2,
      rawClientData: ssBuffer.slice(portIndex + 2),
      version: null,
      isUDP: portRemote == 53,
    };
  }

  readFlashHeader(buffer) {
    const version = buffer[0];
    let isUDP = false;

    const optLength = buffer[17];
    const cmd = buffer[18 + optLength];
    
    if (cmd === 2) {
      isUDP = true;
    } else if (cmd !== 1) {
      return { hasError: true, message: `command ${cmd} is not supported` };
    }
    
    const portIndex = 18 + optLength + 1;
    const portRemote = buffer.readUInt16BE(portIndex);

    let addressIndex = portIndex + 2;
    const addressType = buffer[addressIndex];
    
    let addressLength = 0;
    let addressValueIndex = addressIndex + 1;
    let addressValue = "";
    
    switch (addressType) {
      case 1:
        addressLength = 4;
        addressValue = Array.from(buffer.slice(addressValueIndex, addressValueIndex + addressLength)).join(".");
        break;
      case 2:
        addressLength = buffer[addressValueIndex];
        addressValueIndex += 1;
        addressValue = buffer.slice(addressValueIndex, addressValueIndex + addressLength).toString();
        break;
      case 3:
        addressLength = 16;
        const ipv6 = [];
        for (let i = 0; i < 8; i++) {
          ipv6.push(buffer.readUInt16BE(addressValueIndex + i * 2).toString(16));
        }
        addressValue = ipv6.join(":");
        break;
      default:
        return { hasError: true, message: `invalid addressType is ${addressType}` };
    }
    
    if (!addressValue) {
      return { hasError: true, message: `addressValue is empty, addressType is ${addressType}` };
    }

    return {
      hasError: false,
      addressRemote: addressValue,
      addressType: addressType,
      portRemote: portRemote,
      rawDataIndex: addressValueIndex + addressLength,
      rawClientData: buffer.slice(addressValueIndex + addressLength),
      version: Buffer.from([version, 0]),
      isUDP: isUDP,
    };
  }

  readHorseHeader(buffer) {
    const dataBuffer = buffer.slice(58);
    if (dataBuffer.length < 6) {
      return { hasError: true, message: "invalid request data" };
    }

    let isUDP = false;
    const cmd = dataBuffer[0];
    if (cmd == 3) {
      isUDP = true;
    } else if (cmd != 1) {
      throw new Error("Unsupported command type!");
    }

    let addressType = dataBuffer[1];
    let addressLength = 0;
    let addressValueIndex = 2;
    let addressValue = "";
    
    switch (addressType) {
      case 1:
        addressLength = 4;
        addressValue = Array.from(dataBuffer.slice(addressValueIndex, addressValueIndex + addressLength)).join(".");
        break;
      case 3:
        addressLength = dataBuffer[addressValueIndex];
        addressValueIndex += 1;
        addressValue = dataBuffer.slice(addressValueIndex, addressValueIndex + addressLength).toString();
        break;
      case 4:
        addressLength = 16;
        const ipv6 = [];
        for (let i = 0; i < 8; i++) {
          ipv6.push(dataBuffer.readUInt16BE(addressValueIndex + i * 2).toString(16));
        }
        addressValue = ipv6.join(":");
        break;
      default:
        return { hasError: true, message: `invalid addressType is ${addressType}` };
    }

    if (!addressValue) {
      return { hasError: true, message: `address is empty, addressType is ${addressType}` };
    }

    const portIndex = addressValueIndex + addressLength;
    const portRemote = dataBuffer.readUInt16BE(portIndex);
    return {
      hasError: false,
      addressRemote: addressValue,
      addressType: addressType,
      portRemote: portRemote,
      rawDataIndex: portIndex + 4,
      rawClientData: dataBuffer.slice(portIndex + 4),
      version: null,
      isUDP: isUDP,
    };
  }

  remoteSocketToWS(remoteSocket, webSocket, responseHeader, retry, log) {
    let header = responseHeader;
    let hasIncomingData = false;

    remoteSocket.on('data', (chunk) => {
      hasIncomingData = true;
      if (webSocket.readyState !== WS_READY_STATE_OPEN) {
        remoteSocket.destroy();
        return;
      }
      if (header) {
        const combined = Buffer.concat([Buffer.from(header), chunk]);
        webSocket.send(combined);
        header = null;
      } else {
        webSocket.send(chunk);
      }
    });

    remoteSocket.on('close', () => {
      if (hasIncomingData === false && retry) {
        retry();
      }
    });

    remoteSocket.on('error', (error) => {
      console.error(`remoteSocket error:`, error);
    });
  }

  // ==================== SERVER START ====================

  start(port = process.env.PORT || 3000) {
    const server = http.createServer((req, res) => {
      this.handleHttpRequest(req, res).catch(error => {
        console.error('HTTP handler error:', error);
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        res.end('Internal Server Error');
      });
    });

    this.wss = new WebSocket.Server({ 
      server,
      perMessageDeflate: false
    });

    this.wss.on('connection', (ws, req) => {
      this.handleWebSocketConnection(ws, req);
    });

    const gracefulShutdown = () => {
      console.log('Shutting down gracefully...');
      if (this.wss) {
        this.wss.clients.forEach((client) => {
          if (client.readyState === WebSocket.OPEN) {
            client.close();
          }
        });
        this.wss.close();
      }
      
      for (const [key, connection] of this.activeUDPConnections.entries()) {
        try {
          connection.socket.close();
        } catch (err) {}
      }
      this.activeUDPConnections.clear();
      
      if (this.httpServer) {
        this.httpServer.close(() => {
          console.log('HTTP server closed');
          process.exit(0);
        });
      }
      setTimeout(() => { process.exit(1); }, 10000);
    };

    process.on('SIGTERM', gracefulShutdown);
    process.on('SIGINT', gracefulShutdown);

    server.listen(port, '0.0.0.0', () => {
      console.log(`✅ Gateway server running on port ${port}`);
      console.log(`📡 Default connection mode: ${this.connectionMode}`);
      console.log(`🔑 Default SNI: ${this.sniHost}`);
      console.log(`🔒 SNI Enabled: ${this.useSNI}`);
    });

    this.httpServer = server;
    
    server.on('error', (error) => {
      console.error('Server error:', error);
      if (error.code === 'EADDRINUSE') {
        console.error(`Port ${port} is already in use`);
        process.exit(1);
      }
    });
  }
}

if (require.main === module) {
  const server = new GatewayServer();
  try {
    require('dotenv').config();
  } catch (e) {}
  const port = process.env.PORT || 3000;
  server.start(port);
}

module.exports = GatewayServer;
