import "dotenv/config";
import express from "express";
import path from "path";
import cors from "cors";
import userRoutes from "./routes/userRoutes.js";
import authRoutes from "./routes/authRoutes.js";
import authMiddleware from "./middleware/authMiddleware.js";
import { startBatchTimer } from "./services/batchProcessor.js";

const app = express();
const PORT = 5000;


// Middleware
app.use(cors());
// Increased limit to 50mb to allow massive bulk file uploads from frontend
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));

// Routes
app.use("/api/auth", authRoutes);
app.use("/api/users", authMiddleware, userRoutes); // Protected routes!

const __dirname = path.resolve();
// 1. Serve static files from the React 'dist' folder
app.use(express.static(path.join(__dirname, 'dist')));

// 2. Catch-all route to serve React's index.html for client-side routing
app.use((req, res) => {
  res.sendFile(path.join(__dirname, 'dist', 'index.html'));
});




// Start server
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
  
  // Start the background batch execution timer
  startBatchTimer();
});