import express from "express";
import cors from "cors";
import userRoutes from "./routes/userRoutes.js";
import { startBatchTimer } from "./services/batchProcessor.js";

const app = express();
const PORT = 5000;

// Middleware
app.use(cors());
// Increased limit to 50mb to allow massive bulk file uploads from frontend
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));

// Routes
app.use("/api/users", userRoutes);

// Root health check
app.get("/", (req, res) => {
  res.send("Backend API is running 🚀");
});

// Start server
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
  
  // Start the background batch execution timer
  startBatchTimer();
});