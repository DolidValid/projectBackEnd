import { Router } from "express";
import { uploadBatchHandler, active4GHandler, resultBatchHandler, getHistoryHandler, deleteHistoryHandler } from "../controllers/userController.js";

/**
 * Express router instance for handling user-related routes.
 * @type {import('express').Router}
 */
const router = Router();

// Unified Bulk Upload API
router.post("/upload-batch", uploadBatchHandler);

// New active4G API
router.post("/active4G", active4GHandler);

// Global Search API (ESB_LOG results)
router.post("/Search", resultBatchHandler);

// Specific results for a batch
router.post("/resultBatch", resultBatchHandler);

// History and deletion
router.get("/batch-history", getHistoryHandler);
router.delete("/batch-history/:id", deleteHistoryHandler);

export default router;
