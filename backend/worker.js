const fs = require('fs-extra');
const csv = require('csv-parser');
const { format } = require('@fast-csv/format');
const emailValidator = require('deep-email-validator');
const admin = require('firebase-admin');

// Initialize Firebase Admin (Firestore only)
// --- FIREBASE SETUP (Worker) ---
const path = require('path');
const serviceAccountPath = path.join(__dirname, 'service-key.json');

if (!admin.apps.length) {
  try {
    const serviceAccount = require(serviceAccountPath);
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount)
    });
    console.log("👷 Worker: Firebase Initialized with Service Key");
  } catch (error) {
    console.error("❌ Worker Error: Could not load service-key.json", error.message);
  }
}
const db = admin.firestore();


async function processCsvJob(jobId, filePath) {
  // Ensure temp directory exists
  await fs.ensureDir('./temp');

  const resultsFilename = `results-${jobId}.csv`;
  const resultsPath = `./temp/${resultsFilename}`;
  const jobRef = db.collection('jobs').doc(jobId);

  let processedCount = 0;
  let validCount = 0;
  let invalidCount = 0;
  let riskyCount = 0;

  const outputStream = fs.createWriteStream(resultsPath);
  const csvStream = format({ headers: true });
  csvStream.pipe(outputStream);

  try {
    const readStream = fs.createReadStream(filePath).pipe(csv());

    for await (const row of readStream) {
      const email = row.email || row.Email || Object.values(row)[0];
      if (!email) continue;

      const result = await validateEmail(email); // (Same validation function as before)
      
      if (result.status === 'Valid') validCount++;
      else if (result.status === 'Invalid') invalidCount++;
      else riskyCount++;

      processedCount++;

      csvStream.write({
        ...row,
        validation_status: result.status,
        validation_reason: result.reason
      });

      if (processedCount % 10 === 0) {
        await jobRef.update({
          processedCount,
          stats: { valid: validCount, invalid: invalidCount, risky: riskyCount }
        });
      }
    }

    csvStream.end();
    await new Promise((resolve) => outputStream.on('finish', resolve));

    // --- CHANGE: Point to our local server endpoint instead of Firebase Storage ---
    // We assume the server is running at the same domain
    const downloadUrl = `/api/download/${resultsFilename}`;

    await jobRef.update({
      status: 'completed',
      processedCount,
      downloadUrl: downloadUrl, // Frontend will use this link
      stats: { valid: validCount, invalid: invalidCount, risky: riskyCount },
      completedAt: admin.firestore.FieldValue.serverTimestamp()
    });

  } catch (error) {
    console.error(`Job ${jobId} failed:`, error);
    await jobRef.update({ status: 'failed', error: error.message });
  } finally {
    // Delete the ORIGINAL upload to save space, but KEEP the result file
    await fs.remove(filePath).catch(console.error);
  }
}

// Copy the validateEmail function from the previous step here...
async function validateEmail(email) {
    // ... use the same code as before ...
    // Placeholder for context:
    return { status: 'Risky', reason: 'Mock Logic' }; 
}

module.exports = { processCsvJob };