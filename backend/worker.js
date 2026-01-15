const fs = require('fs');
const path = require('path');
const admin = require('firebase-admin');
const csv = require('csv-parser');
const { format } = require('@fast-csv/format');
const emailValidator = require('email-validator');

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
    // Only log error if env var is also missing, otherwise it might just be prod mode
    if (!process.env.FIREBASE_SERVICE_KEY) {
        console.error("❌ Worker: Could not find service-key.json OR env var.");
    }
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

  let validCount = 0;
  let invalidCount = 0;
  // We don't use "Risky" anymore for this cloud version
  const riskyCount = 0; 

  // 1. Setup Output File
  const outputFilename = `results-${jobId}.csv`;
  // Ensure the temp directory exists
  const tempDir = path.join(__dirname, 'temp');
  if (!fs.existsSync(tempDir)) {
    fs.mkdirSync(tempDir);
  }
  const outputPath = path.join(tempDir, outputFilename);

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
        
        // --- UPDATED VALIDATION LOGIC FOR CLOUD ---
        // We use pure syntax validation here. 
        // This runs locally on the server and does NOT require Port 25.
        if (email && emailValidator.validate(email)) {
             status = 'Valid'; 
             validCount++;
        } else {
             status = 'Invalid';
             invalidCount++;
        }

        // Add status column to the row
        row['Validation Status'] = status;
        
        // Write the row to the new CSV
        csvStream.write(row);
      })
      .on('end', async () => {
        csvStream.end();
        console.log(`Job ${jobId} finished. Valid: ${validCount}, Invalid: ${invalidCount}`);

        // 3. Update Firestore with Final Stats & Download URL
        try {
          await db.collection('jobs').doc(jobId).update({
            status: 'completed',
            processedCount: validCount + invalidCount,
            totalEmails: validCount + invalidCount,
            stats: {
              valid: validCount,
              invalid: invalidCount,
              risky: riskyCount
            },
            downloadUrl: `/api/download/${outputFilename}`
          });
          
          // Cleanup the uploaded raw file (save disk space)
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