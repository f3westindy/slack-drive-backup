require("dotenv").config();
const express = require("express");
const { WebClient } = require("@slack/web-api");
const { google } = require("googleapis");
const axios = require("axios");
const { Pool } = require("pg");
const fs = require("fs");
const path = require("path");
const pRetry = require("p-retry");
const cron = require("node-cron");

const app = express();
app.use(express.json());

const slack = new WebClient(process.env.SLACK_BOT_TOKEN);

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// Create required tables
async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS exported_files (
      slack_file_id TEXT PRIMARY KEY,
      slack_file_name TEXT,
      drive_file_id TEXT,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS backup_lock (
      id INT PRIMARY KEY,
      is_running BOOLEAN DEFAULT FALSE,
      started_at TIMESTAMP
    );
  `);

  await pool.query(`
    INSERT INTO backup_lock (id, is_running)
    VALUES (1, FALSE)
    ON CONFLICT (id) DO NOTHING;
  `);
}

const auth = new google.auth.GoogleAuth({
  credentials: {
    client_email: process.env.GOOGLE_CLIENT_EMAIL,
    private_key: process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n'),
  },
  scopes: ["https://www.googleapis.com/auth/drive"],
});

const drive = google.drive({ version: "v3", auth });

async function alreadyExported(id) {
  const res = await pool.query(
    "SELECT 1 FROM exported_files WHERE slack_file_id=$1",
    [id]
  );
  return res.rowCount > 0;
}

async function markExported(id, name, driveId) {
  await pool.query(
    "INSERT INTO exported_files(slack_file_id, slack_file_name, drive_file_id) VALUES ($1,$2,$3)",
    [id, name, driveId]
  );
}

async function download(url, name) {
  const filePath = path.join("/tmp", name);

  const response = await axios.get(url, {
    responseType: "stream",
    headers: {
      Authorization: `Bearer ${process.env.SLACK_BOT_TOKEN}`,
    },
  });

  const writer = fs.createWriteStream(filePath);
  response.data.pipe(writer);

  return new Promise((resolve, reject) => {
    writer.on("finish", () => resolve(filePath));
    writer.on("error", reject);
  });
}

// Lock functions
async function acquireLock() {
  const res = await pool.query(`
    UPDATE backup_lock
    SET is_running = TRUE, started_at = NOW()
    WHERE id = 1 AND is_running = FALSE
    RETURNING *;
  `);
  return res.rowCount > 0;
}

async function releaseLock() {
  await pool.query(`
    UPDATE backup_lock
    SET is_running = FALSE
    WHERE id = 1;
  `);
}

async function exportImages() {
  if (!(await acquireLock())) {
    console.log("Backup already running. Skipping.");
    return;
  }

  console.log("Backup started:", new Date().toISOString());

  try {
    let cursor;

    do {
      const result = await slack.conversations.history({
        channel: process.env.SLACK_CHANNEL_ID,
        cursor,
        limit: 200,
      });

      for (const message of result.messages) {
        if (!message.files) continue;

        for (const file of message.files) {
          if (!file.mimetype.startsWith("image/")) continue;
          if (await alreadyExported(file.id)) continue;

          await pRetry(async () => {
            const filePath = await download(file.url_private, file.name);

            const uploadRes = await drive.files.create({
              requestBody: {
                name: file.name,
                parents: [process.env.GOOGLE_DRIVE_FOLDER_ID],
              },
              media: {
                body: fs.createReadStream(filePath),
              },
              fields: "id",
            });

            await markExported(file.id, file.name, uploadRes.data.id);
            fs.unlinkSync(filePath);
          }, { retries: 3 });
        }
      }

      cursor = result.response_metadata?.next_cursor;
    } while (cursor);

    console.log("Backup completed:", new Date().toISOString());
  } catch (err) {
    console.error("Backup failed:", err);
  } finally {
    await releaseLock();
  }
}

// Manual trigger
app.post("/export", async (req, res) => {
  exportImages();
  res.json({ status: "Export started" });
});

// Health check
app.get("/health", (req, res) => res.send("OK"));

// Schedule Nightly Backup
const schedule = process.env.CRON_SCHEDULE || "0 2 * * *"; // default 2am

cron.schedule(schedule, () => {
  console.log("Scheduled backup triggered");
  exportImages();
});

initDB().then(() => {
  app.listen(process.env.PORT || 3000, () => {
    console.log("Server running");
  });
});
