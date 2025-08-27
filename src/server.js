import express from "express";
import cors from "cors";
import userRoutes from "./routes/userRoutes.js";

const app = express();
const PORT = 5000;

// Middleware
app.use(cors());
app.use(express.json()); // Use the built-in express.json() middleware

// Routes
app.use("/api/users", userRoutes);

// Root health check
app.get("/", (req, res) => {
  res.send("Backend API is running 🚀");
});

// Start server
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});