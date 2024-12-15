module.exports = {
  apps: [{
    name: "chicago-dig-daily",
    script: "src/scripts/run_daily_update.py",
    interpreter: "python3",
    cron_restart: "0 10 * * *",  // 10am daily
    autorestart: false,
    env: {
      NODE_ENV: "production"
    }
  },
  {
    name: "chicago-dig-roulette",
    script: "src/scripts/post_random_permit.py",
    interpreter: "python3",
    cron_restart: "0 */3 * * *",  // Every 3 hours
    autorestart: false,
    env: {
      NODE_ENV: "production"
    }
  }]
}
