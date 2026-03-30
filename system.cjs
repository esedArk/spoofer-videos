module.exports = {
  apps: [
    {
      name: "spoofer-api",
      cwd: __dirname,
      script: "api_server.py",
      interpreter: "python",
      watch: false,
      autorestart: true,
      max_restarts: 10,
      restart_delay: 2000,
      env: {
        NODE_ENV: "production",
      },
      out_file: "./logs/api.out.log",
      error_file: "./logs/api.err.log",
      time: true,
    },
    {
      name: "spoofer-worker",
      cwd: __dirname,
      script: "worker.py",
      interpreter: "python",
      watch: false,
      autorestart: true,
      max_restarts: 10,
      restart_delay: 2000,
      env: {
        NODE_ENV: "production",
      },
      out_file: "./logs/worker.out.log",
      error_file: "./logs/worker.err.log",
      time: true,
    },
  ],
};
