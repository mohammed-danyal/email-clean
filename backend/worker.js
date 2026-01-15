const fs = require('fs');
const path = require('path');
const admin = require('firebase-admin');
const csv = require('csv-parser');
const { format } = require('@fast-csv/format');
const emailValidator = require('email-validator');
// If you are using 'deep-email-validator' or custom logic, keep that import here
// For this MVP, we likely used simple regex or a library. 
// I will assume simple validation for the base code, but if you installed 
// specific libraries for the check, ensure they are imported.

// --- FIREBASE SETUP (Cloud & Local Compatible) ---
let serviceAccount;

// OPTION 1: Production (Render Environment Variable)
if (process.env.FIREBASE_SERVICE_KEY) {
  try {
    serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_KEY);
    console.log("👷 Worker: Loaded Firebase key from Environment Variable");
  } catch (err) {
    console.error("❌ Worker: Failed to parse FIREBASE_SERVICE_KEY:", err);
  }
} 
// OPTION 2: Development (Local File)
else {
  const serviceAccountPath = path.join(__dirname, 'service-key.json');
  try {
    serviceAccount = require(serviceAccountPath);
    console.log("👷 Worker: Loaded Firebase key from local file");
  } catch (err) {
    console.error("❌ Worker: Could not find service-key.json OR env var.");
  }
}

// Initialize Firebase if not already initialized
if (!admin.apps.length && serviceAccount) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

/**
 * The main background function to process the file.
 * @param {string} jobId - The ID of the job in Firestore
 * @param {string} filePath - The path to the uploaded CSV file
 */
async function processCsvJob(jobId, filePath) {
  console.log(`Starting job: ${jobId}`);

  const results = [];
  let validCount = 0;
  let invalidCount = 0;
  let riskyCount = 0;

  // 1. Setup Output File
  const outputFilename = `results-${jobId}.csv`;
  const outputPath = path.join(__dirname, 'temp', outputFilename);
  
  // Ensure temp folder exists
  if (!fs.existsSync(path.join(__dirname, 'temp'))) {
    fs.mkdirSync(path.join(__dirname, 'temp'));
  }

  const csvStream = format({ headers: true });
  const writeStream = fs.createWriteStream(outputPath);
  csvStream.pipe(writeStream);

  // 2. Read & Process CSV
  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (row) => {
        // Find the email column (case-insensitive)
        const emailKey = Object.keys(row).find(k => k.toLowerCase().includes('email'));
        const email = row[emailKey];

        let status = 'Invalid';
        
        if (email && emailValidator.validate(email)) {
             // For MVP Cloud deployment, simple syntax check is safest first.
             // (Deep SMTP checks usually blocked on cloud without paid proxies)
             status = 'Valid'; 
             validCount++;
        } else {
             status = 'Invalid';
             invalidCount++;
        }

        // Add status column
        row['Validation Status'] = status;
        
        // Write to new CSV
        csvStream.write(row);
      })
      .on('end', async () => {
        csvStream.end();
        console.log(`Job ${jobId} finished processing.`);

        // 3. Update Firestore with Final Stats & Download URL
        try {
          await db.collection('jobs').doc(jobId).update({
            status: 'completed',
            processedCount: validCount + invalidCount + riskyCount,
            totalEmails: validCount + invalidCount + riskyCount,
            stats: {
              valid: validCount,
              invalid: invalidCount,
              risky: riskyCount
            },
            downloadUrl: `/api/download/${outputFilename}`
          });
          
          // Cleanup uploaded raw file (save disk space)
          fs.unlink(filePath, (err) => {
            if (err) console.error("Error deleting upload:", err);
          });

          resolve();
        } catch (err) {
          console.error("Error updating Firestore:", err);
          reject(err);
        }
      })
      .on('error', async (error) => {
        console.error("CSV Processing Error:", error);
        await db.collection('jobs').doc(jobId).update({ status: 'failed' });
        reject(error);
      });
  });
}

module.exports = { processCsvJob };