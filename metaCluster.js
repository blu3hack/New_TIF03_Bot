// index.js
const TelegramBot = require('node-telegram-bot-api');
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

// 🔹 MENGAMBIL KONEKSI DARI connection.js
const pool = require('./connection');

// === KONFIG TELEGRAM BOT ===
const token = '7235593002:AAGjkzbxH-fCUYBXV6qXFoKGocUHIkh4XRo';
const bot = new TelegramBot(token, { polling: true });

// === TEST KONEKSI AWAL ===
(async () => {
  try {
    const conn = await pool.getConnection();
    console.log('✅ Terkoneksi ke database MySQL via connection.js');
    conn.release();
  } catch (err) {
    console.error('❌ Gagal terkoneksi ke database:', err.message);
  }
})();

// === STATE MANAGEMENT ===
const runningProcesses = {}; // key: command, value: child process

// === DAFTAR COMMAND YANG DIPERBOLEHKAN ===
const availableCommands = [
  'assurance_ebis_running',
  'assurance_wifi_running',
  'assurance_wsa_running',
  'fulfillment_running',
  'newexpro_running',
  'ps_re_running',
  'service_sugar_running',
  'simarvel_running',
  'telkomcare_running',
  'wifi_revi_running',
  'wisaquery',
  'loaddatawisa',
  'delete_all_data',
];

// === LOGIKA UTAMA BOT ===
bot.on('message', async (msg) => {
  const { chat, from, text } = msg;
  const chatId = chat.id;
  const username = from.username || from.first_name || 'Unknown';
  const command = text ? text.trim() : '';

  // 1. Otorisasi User
  if (username !== 'nheq_12') {
    return bot.sendMessage(chatId, '❌ Anda tidak diizinkan menggunakan bot ini.');
  }

  // 2. Fungsi Pembantu: Simpan Log ke Database
  async function saveMessageToDB(cmd) {
    try {
      // Menghapus data lama sesuai logika asli Anda
      await pool.query('DELETE FROM get_otp_for_download');

      const now = new Date().toISOString().slice(0, 19).replace('T', ' ');
      await pool.query('INSERT INTO get_otp_for_download (username, pesan, otp_for) VALUES (?, ?, ?)', [username, cmd, now]);
      console.log(`💾 Log [${cmd}] berhasil disimpan ke database.`);
    } catch (err) {
      console.error('❌ Error Database:', err.message);
    }
  }

  // --- HANDLING COMMANDS ---

  // A. STOP Command
  if (command.startsWith('stop ')) {
    const toStop = command.split(' ')[1];
    if (runningProcesses[toStop]) {
      runningProcesses[toStop].kill('SIGTERM');
      delete runningProcesses[toStop];
      return bot.sendMessage(chatId, `🛑 Proses [${toStop}] berhasil dihentikan.`);
    }
    return bot.sendMessage(chatId, `⚠️ Tidak ada proses aktif bernama: ${toStop}`);
  }

  // B. LIST Command
  if (command === 'list') {
    const active = Object.keys(runningProcesses);
    return bot.sendMessage(chatId, active.length ? `🔎 Proses Aktif:\n${active.join('\n')}` : '✅ Tidak ada proses berjalan.');
  }

  // C. Navigasi: CD (Change Directory)
  if (command.startsWith('cd ')) {
    const targetDir = command.split(' ')[1];
    try {
      const newPath = path.resolve(process.cwd(), targetDir);
      process.chdir(newPath);
      return bot.sendMessage(chatId, `📂 Direktori saat ini: ${process.cwd()}`);
    } catch (err) {
      return bot.sendMessage(chatId, `❌ Gagal pindah direktori: ${err.message}`);
    }
  }

  // D. Navigasi: DIR (List Files)
  if (command === 'dir') {
    try {
      const files = fs.readdirSync(process.cwd(), { withFileTypes: true });
      const list = files.map((f) => (f.isDirectory() ? `📁 ${f.name}` : `📄 ${f.name}`)).join('\n');
      return bot.sendMessage(chatId, `📂 Folder: ${process.cwd()}\n\n${list || '(kosong)'}`);
    } catch (err) {
      return bot.sendMessage(chatId, `❌ Gagal membaca direktori: ${err.message}`);
    }
  }

  // E. EKSEKUSI SCRIPT (.js atau .bat)
  if (availableCommands.includes(command) || command.startsWith('cpt')) {
    if (runningProcesses[command]) {
      return bot.sendMessage(chatId, `⚠️ Proses [${command}] masih berjalan.`);
    }

    await saveMessageToDB(command);

    const isBat = command.endsWith('.bat') || command === 'delete' || command === 'loadfile';
    let scriptPath;
    let child;

    // Tentukan path dan jenis file
    if (isBat) {
      // Mencari file .bat jika command tidak pakai ekstensi
      const batFile = command.endsWith('.bat') ? command : `${command}.bat`;
      scriptPath = path.join(process.cwd(), batFile);
    } else {
      scriptPath = path.join(process.cwd(), `${command}.js`);
    }

    if (!fs.existsSync(scriptPath)) {
      return bot.sendMessage(chatId, `❌ File script tidak ditemukan: ${path.basename(scriptPath)}`);
    }

    // Eksekusi berdasarkan tipe file
    if (isBat) {
      child = spawn(scriptPath, [], { cwd: process.cwd(), shell: true });
    } else {
      child = spawn('node', [scriptPath], { cwd: process.cwd() });
    }

    runningProcesses[command] = child;
    bot.sendMessage(chatId, `▶️ Menjalankan: ${path.basename(scriptPath)}...`);

    // Handle Output
    child.stdout.on('data', (data) => {
      const output = data.toString().trim();
      if (output) bot.sendMessage(chatId, `📤 [${command}]:\n${output}`);
    });

    child.stderr.on('data', (data) => {
      bot.sendMessage(chatId, `⚠️ Error [${command}]:\n${data.toString()}`);
    });

    child.on('close', (code) => {
      delete runningProcesses[command];
      bot.sendMessage(chatId, `✅ Proses [${command}] selesai (Exit Code: ${code})`);
    });

    // Handle Captcha Khusus
    const captchaTriggers = ['fulfillment', 'telkomcare', 'ps_re', 'unspec_datin', 'ttr_ffg_non_hsi'];
    if (captchaTriggers.includes(command)) {
      setTimeout(() => {
        const captchaFile = path.join(process.cwd(), 'captcha/cpt.png');
        if (fs.existsSync(captchaFile)) {
          bot.sendPhoto(chatId, captchaFile, { caption: '🖼️ Captcha terdeteksi.' });
        }
      }, 10000);
    }
  } else {
    // Command tidak dikenal
    if (command !== '') {
      bot.sendMessage(chatId, `❌ Command Tidak Dikenal.\n\nGunakan 'list' untuk melihat proses aktif.`);
    }
  }
});
