const puppeteer = require('puppeteer');
const { user_aribi, pass_aribi } = require('./login');
const { exec } = require('child_process');
const path = require('path');
const fs = require('fs');

(async () => {
  const browser = await puppeteer.launch({
    headless: false,
    slowMo: 50,
    executablePath: 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
    defaultViewport: null,
  });

  const page = await browser.newPage();

  // ================= SET DOWNLOAD DIRECTORY =================
  const downloadDir = path.join(__dirname, 'loaded_file', 'mttr_mso');

  if (!fs.existsSync(downloadDir)) {
    fs.mkdirSync(downloadDir, { recursive: true });
  }

  // Gunakan cara stabil untuk set download
  const client = await page.target().createCDPSession();
  await client.send('Page.setDownloadBehavior', {
    behavior: 'allow',
    downloadPath: downloadDir,
  });

  // ================= FUNCTION TUNGGU DOWNLOAD =================
  async function waitForDownloadComplete(dir) {
    return new Promise((resolve) => {
      const interval = setInterval(() => {
        const files = fs.readdirSync(dir);

        const downloading = files.find((f) => f.endsWith('.crdownload'));

        if (!downloading && files.length > 0) {
          clearInterval(interval);
          resolve(files);
        }
      }, 1000);
    });
  }

  // ================= FUNCTION AMBIL OTP =================
  function getOtpFromDatabase() {
    return new Promise((resolve, reject) => {
      exec('python otp_simarvel.py', (error, stdout, stderr) => {
        if (error) return reject(error);
        if (stderr) return reject(stderr);
        resolve(stdout.trim());
      });
    });
  }

  try {
    // ================= LOGIN =================
    await page.goto('https://simarvel.telkom.co.id/dashboard', {
      waitUntil: 'networkidle2',
    });

    await page.waitForSelector('[placeholder="Enter LDAP ID"]', { visible: true });

    await page.type('[placeholder="Enter LDAP ID"]', user_aribi);
    await page.type('[placeholder="Enter password"]', pass_aribi);

    await Promise.all([page.click('form button[type="submit"]'), page.waitForNavigation({ waitUntil: 'networkidle2' })]);

    // ================= OTP =================
    const otp = await getOtpFromDatabase();
    console.log('OTP:', otp);

    await page.waitForSelector('[placeholder="Enter code"]', { visible: true });
    await page.type('[placeholder="Enter code"]', otp);

    await Promise.all([page.click('#modalRSA button[type="submit"]'), page.waitForNavigation({ waitUntil: 'networkidle2' })]);

    // ================= MASUK HALAMAN DOWNLOAD =================
    await page.goto('https://simarvel.telkom.co.id/cnop3/mttri-comply', {
      waitUntil: 'networkidle2',
    });

    await page.waitForSelector('#exportAllTicketCsv', { visible: true });

    console.log('Memulai proses download...');

    await page.click('#exportAllTicketCsv');

    // ================= TUNGGU SAMPAI FILE BENAR-BENAR SELESAI =================
    const files = await waitForDownloadComplete(downloadDir);

    console.log('Download selesai.');
    console.log('File di folder:', files);
  } catch (err) {
    console.error('Terjadi kesalahan:', err);
  } finally {
    await browser.close();
  }
})();
