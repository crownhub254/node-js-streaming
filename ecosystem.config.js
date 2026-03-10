/**
 * PM2 Ecosystem Config — Crypto Streaming 24/7
 *
 * Start:   pm2 start ecosystem.config.js
 * Stop:    pm2 stop crypto-streaming
 * Restart: pm2 restart crypto-streaming
 * Logs:    pm2 logs crypto-streaming
 * Monitor: pm2 monit
 * Status:  pm2 list
 *
 * To run indefinitely (no test timer), pass 0 as the minutes arg:
 *   pm2 start ecosystem.config.js  (uses args below — 0 = infinite loop)
 *
 * Auto-start on OS reboot (Windows):
 *   npm install -g pm2-windows-service   (run as Administrator)
 *   pm2-service-install
 *   pm2 save
 */

module.exports = {
  apps: [
    {
      name: 'crypto-streaming',
      script: 'compare-v7-enhanced.js',
      args: '99999',                 // v9.9.16: 99999 min = effectively infinite; PM2 auto-restarts on memory threshold
      node_args: '--max-old-space-size=4096',  // v9.9.16: 4GB heap (raised from 2048 — CCXT Pro + native WS can spike)
      max_memory_restart: '1600M',   // v9.9.13: earlier restart before RSS spikes (was 1800M)
      restart_delay: 5000,           // 5s pause between restarts
      exp_backoff_restart_delay: 100, // exponential backoff on repeated crashes
      max_restarts: 200,             // v9.9.13: increased for more restarts per day (was 50)
      min_uptime: '60s',             // must run ≥ 60s to count as stable (CCXT preload takes ~90s)
      log_date_format: 'YYYY-MM-DD HH:mm:ss',
      error_file: './logs/err.log',
      out_file: './logs/out.log',
      merge_logs: true,
      watch: false,
      env: {
        NODE_ENV: 'production',
        VALKEY_HOST: '127.0.0.1',  // v9.9.20: change per deployment (e.g. 'redis.myserver.com', 'valkey', '10.0.0.5')
        VALKEY_PORT: '6379',
        VALKEY_ENABLED: 'true',    // set 'false' to disable Valkey entirely
      },
    },
  ],
};
