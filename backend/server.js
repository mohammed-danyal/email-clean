require('dotenv').config();
const express = require('express');
const cors = require('cors');
const multer = require('multer');
const admin = require('firebase-admin');
const fs = require('fs-extra');
const path = require('path');

// Import the worker logic
const { processCsvJob } = require('./worker');

// Initialize Express
const app = express();
app.use(cors({ origin: true })); // Allow all origins for MVP
app.use(express.json());

// --- FIREBASE SETUP (Firestore Only) ---
// --- FIREBASE SETUP ---


let serviceAccount;

// OPTION 1: Check if the key is in the Environment (Production/Render)
if (process.env.FIREBASE_SERVICE_KEY) {
  try {
    // Parse the string back into an object
    serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_KEY);
    console.log("✅ Loaded Firebase key from Environment Variable");
  } catch (err) {
    console.error("❌ Failed to parse FIREBASE_SERVICE_KEY:", err);
  }
} 
// OPTION 2: Fallback to local file (Development)
else {
  const serviceAccountPath = path.join(__dirname, 'service-key.json');
  try {
    serviceAccount = require(serviceAccountPath);
    console.log("✅ Loaded Firebase key from local file");
  } catch (err) {
    console.error("❌ Could not find service-key.json OR env var.");
  }
}

// Initialize
if (!admin.apps.length && serviceAccount) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}


console.log("---------------------------------------------------");

const db = admin.firestore();


// --- CONFIGURATION ---

// 1. Setup Multer for Temporary Uploads
// Files land in 'uploads/' first, then worker moves results to 'temp/'
const upload = multer({
  dest: 'uploads/', 
  limits: { fileSize: 50 * 1024 * 1024 } // 50MB limit
});

// 2. Auth Middleware
const authenticateUser = async (req, res, next) => {
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const idToken = authHeader.split('Bearer ')[1];
  try {
    const decodedToken = await admin.auth().verifyIdToken(idToken);
    req.user = decodedToken;
    next();
  } catch (error) {
    console.error("Auth Error:", error);
    res.status(403).json({ error: 'Invalid Token' });
  }
};

// --- ROUTES ---

// Health Check
app.get('/', (req, res) => res.send('EmailClean API is Running (Local Storage Mode) 🚀'));

/**
 * POST /api/upload
 * - Receives file
 * - Creates Job in Firestore
 * - Triggers Worker (Fire-and-Forget)
 */
app.post('/api/upload', authenticateUser, upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    const userId = req.user.uid;
    const filePath = req.file.path;
    const originalName = req.file.originalname;

    // 1. Create Job Reference
    const jobRef = db.collection('jobs').doc();
    const jobId = jobRef.id;

    // 2. Save Initial Job State
    await jobRef.set({
      id: jobId,
      userId: userId,
      status: 'processing',
      fileName: originalName,
      processedCount: 0,
      totalEmails: 0, 
      stats: { valid: 0, invalid: 0, risky: 0 },
      createdAt: admin.firestore.FieldValue.serverTimestamp()
    });

    // 3. Trigger Worker asynchronously (Do NOT await this)
    processCsvJob(jobId, filePath).catch(err => {
      console.error(`Background Job ${jobId} Failed:`, err);
    });

    // 4. Respond to client immediately
    res.json({ 
      success: true, 
      jobId: jobId, 
      message: 'Validation started' 
    });

  } catch (error) {
    console.error("Upload Error:", error);
    if (req.file) await fs.remove(req.file.path).catch(() => {});
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

/**
 * GET /api/download/:filename
 * - Serves the processed file from local disk
 * - Includes security checks to prevent directory traversal
 */
app.get('/api/download/:filename', async (req, res) => {
  const filename = req.params.filename;
  
  // Security: Prevent accessing files outside 'temp' (e.g., ../.env)
  if (filename.includes('..') || !filename.startsWith('results-')) {
    return res.status(403).send('Access Denied');
  }

  const filePath = path.join(__dirname, 'temp', filename);

  // Check if file exists
  if (!fs.existsSync(filePath)) {
    return res.status(404).send('File not found. It may have been deleted or expired.');
  }

  // Send file
  res.download(filePath, filename, (err) => {
    if (err) {
      console.error("Download Error:", err);
      if (!res.headersSent) res.status(500).send("Could not download file");
    }
  });
});

/**
 * GET /api/jobs
 * - List 20 most recent jobs for the user
 */
app.get('/api/jobs', authenticateUser, async (req, res) => {
  try {
    const snapshot = await db.collection('jobs')
      .where('userId', '==', req.user.uid)
      .orderBy('createdAt', 'desc')
      .limit(20)
      .get();

    const jobs = [];
    snapshot.forEach(doc => jobs.push(doc.data()));

    res.json(jobs);
  } catch (error) {
    console.error("Fetch Jobs Error:", error);
    res.status(500).json({ error: 'Failed to fetch jobs' });
  }
});

/**
 * GET /api/jobs/:id
 * - Polling endpoint for progress bar
 */
app.get('/api/jobs/:id', authenticateUser, async (req, res) => {
  try {
    const doc = await db.collection('jobs').doc(req.params.id).get();
    if (!doc.exists) return res.status(404).json({ error: 'Job not found' });
    
    const data = doc.data();
    
    // Security check
    if (data.userId !== req.user.uid) return res.status(403).json({ error: 'Forbidden' });

    res.json(data);
  } catch (error) {
    res.status(500).json({ error: 'Error fetching job' });
  }
});

// --- START SERVER ---
const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  // Ensure temp directory exists on startup
  fs.ensureDirSync('./temp');
});