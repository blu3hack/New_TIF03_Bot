const puppeteer = require('puppeteer');
const fs = require('fs');
const { user_care, pass_care } = require('./login');
const { exec } = require('child_process');
const path = require('path');
const mysql = require('./connection'); // Menggunakan pool dari connection.js

(async () => {
  // 1. Launch Browser
  const browser = await puppeteer.launch({
    headless: false,
    defaultViewport: null,
    args: ['--start-maximized'],
  });
  const page = await browser.newPage();

  // Helper untuk navigasi XPath
  async function goToLinkByXPath(xpath) {
    await page.waitForXPath(xpath);
    const links = await page.$x(xpath);
    if (links.length > 0) {
      const linkUrl = await page.evaluate((link) => link.href, links[0]);
      await page.goto(linkUrl, { waitUntil: 'networkidle2', timeout: 60000 }).catch(() => void 0);
    }
  }

  // Helper untuk mendapatkan OTP dari Python
  async function getOTPFromPython() {
    return new Promise((resolve, reject) => {
      exec('python otp.py', (error, stdout, stderr) => {
        if (error || stderr) return reject(error || stderr);
        resolve(stdout.trim());
      });
    });
  }

  try {
    // 2. Login Page
    await page.goto('https://nonatero.telkom.co.id/wsa/index.php/dashboard/', { waitUntil: 'networkidle2' });

    await page.type('[placeholder="Enter your Nik or username"]', user_care);
    await page.type('[placeholder="············"]', pass_care);

    const checkboxSelector = '#ck-terms-of-use';
    await page.waitForSelector(checkboxSelector);
    const isChecked = await page.$eval(checkboxSelector, (el) => el.checked);
    if (!isChecked) await page.click(checkboxSelector);

    await page.click('#formAuthentication > button');

    // 3. Input OTP
    console.log('🔄 Menunggu OTP...');
    await page.waitForTimeout(3000);
    const code_otp = await getOTPFromPython();
    const codes = code_otp.split(''); // [ '1', '2', '3', '4', '5', '6' ]

    for (let i = 0; i < 6; i++) {
      await page.type(`#gg_otp > div > input.sscrt.scrt-${i + 1}`, codes[i]);
      await page.waitForTimeout(100);
    }

    await page.click('#gg_otp > input.btn.btn-primary.mb-3');
    await page.waitForNavigation({ waitUntil: 'networkidle2' });

    // 4. Proses Download
    await goToLinkByXPath("//a[contains(., 'File Downlod')]");

    // Tentukan Filter
    await page.waitForSelector('#company_dwld');
    await page.select('#company_dwld', 'TELKOM-OLD');
    await page.waitForTimeout(1000);
    await page.select('#periode', '202603');
    await page.waitForTimeout(1000);

    const [filterBtn] = await page.$x("//button[contains(., 'Filter')]");
    if (filterBtn) {
      await Promise.all([filterBtn.click(), page.waitForNavigation({ waitUntil: 'load', timeout: 0 }).catch(() => void 0)]);
    }

    // Set Download Path
    const downloadPath = path.join(__dirname, 'file_download');
    if (!fs.existsSync(downloadPath)) fs.mkdirSync(downloadPath, { recursive: true });

    await page._client().send('Page.setDownloadBehavior', {
      behavior: 'allow',
      downloadPath: downloadPath,
    });

    // 5. Loop Download & Save OTP ke DB
    const varname = ['open', 'close'];
    const regional = ['reg4', 'reg5'];
    const rows = [5, 6]; // Index baris table
    const cols = [3, 4]; // Index kolom table

    for (let i = 0; i < rows.length; i++) {
      for (let j = 0; j < cols.length; j++) {
        const tombol = `table tbody tr:nth-child(${rows[i]}) td:nth-child(${cols[j]}) a`;
        await page.waitForSelector(tombol);
        await page.click(tombol);

        const fileName = `${varname[j]}_${regional[i]}`;
        console.log(`📥 Downloading: ${fileName}`);

        // Tunggu download dan ambil password dari DB (bot_message)
        await page.waitForTimeout(15000);

        try {
          const keyword = 'password file anda';
          const [results] = await mysql.query('SELECT message FROM bot_message WHERE message LIKE ? ORDER BY id DESC LIMIT 1', [`%${keyword}%`]);

          if (results.length > 0) {
            const match = results[0].message.match(/\d+/);
            if (match) {
              const otpVal = match[0];
              await mysql.query('INSERT INTO otp_for_extract (message, otp) VALUES (?, ?)', [fileName, otpVal]);
              console.log(`✅ OTP Saved for ${fileName}: ${otpVal}`);
            }
          }
        } catch (dbErr) {
          console.error('❌ Database Error:', dbErr.message);
        }
      }
    }

    console.log('🏁 Semua proses selesai.');
    await page.waitForTimeout(5000);
  } catch (err) {
    console.error('💥 Terjadi kesalahan fatal:', err);
  } finally {
    await browser.close();
    // Pool connection tidak perlu ditutup jika script ini bagian dari bot yang stand-by,
    // Namun jika script sekali jalan, gunakan: await mysql.end();
  }
})();
