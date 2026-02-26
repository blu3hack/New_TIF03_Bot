const puppeteer = require('puppeteer');
const { user_aribi, pass_aribi } = require('./login');
const { exec } = require('child_process');

(async () => {
  const browser = await puppeteer.launch({
    headless: false,
    slowMo: 50,
    executablePath: 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  });

  const page = await browser.newPage();

  // ================= SET FOLDER DOWNLOAD =================
  const downloadPath = path.join(__dirname, 'loaded_file/mttr_mso', `${fileName}.csv`);

  const client = await page.target().createCDPSession();
  await client.send('Page.setDownloadBehavior', {
    behavior: 'allow',
    downloadPath: downloadPath,
  });

  try {
    // ================= LOGIN =================
    await page.goto('https://simarvel.telkom.co.id/dashboard', {
      waitUntil: 'networkidle2',
    });

    await page.type('[placeholder="Enter LDAP ID"]', user_aribi);
    await page.type('[placeholder="Enter password"]', pass_aribi);

    await Promise.all([page.click('form button[type="submit"]'), page.waitForNavigation({ waitUntil: 'networkidle2' })]);

    // ================= OTP =================
    function getCaptchaFromDatabase() {
      return new Promise((resolve, reject) => {
        exec('python otp_simarvel.py', (error, stdout, stderr) => {
          if (error || stderr) return reject(error || stderr);
          resolve(stdout.trim());
        });
      });
    }

    const otp = await getCaptchaFromDatabase();

    await page.waitForSelector('[placeholder="Enter code"]', { visible: true });
    await page.type('[placeholder="Enter code"]', otp);
    await page.click('#modalRSA button[type="submit"]');

    // ================= DOWNLOAD =================
    await page.goto('https://simarvel.telkom.co.id/cnop3/mttri-comply', {
      waitUntil: 'networkidle2',
    });

    await page.waitForSelector('#exportAllTicketCsv', { visible: true });
    await page.click('#exportAllTicketCsv');

    console.log('?? Download dimulai ke:', downloadPath);
  } catch (err) {
    console.error('? Ada kesalahan:', err.message);
  } finally {
    await page.waitForTimeout(20000);
    await browser.close();
  }
})();
