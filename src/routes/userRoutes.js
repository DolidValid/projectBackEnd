import { Router } from "express";
import { 
  uploadBatchHandler, 
  active4GHandler, 
  resultBatchHandler, 
  getHistoryHandler, 
  deleteHistoryHandler,
  pauseBatchHandler,
  resumeBatchHandler,
  cancelBatchHandler,
  batchQueueHandler
} from "../controllers/userController.js";

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

// Batch control: Pause / Resume / Cancel
router.post("/batch-control/pause", pauseBatchHandler);
router.post("/batch-control/resume", resumeBatchHandler);
router.post("/batch-control/cancel", cancelBatchHandler);

// Batch queue: View active/pending batches
router.get("/batch-queue", batchQueueHandler);

export default router;
