import { insertInfoFile, InsertSet3g, getBatchHistory, deleteBatchHistory } from "../models/userModel.js";
import { getResultBatch } from "../services/resultBatchService.js";
import { pauseBatch, resumeBatch, cancelBatch, getActiveBatches, cancelPendingBatch, getBatchStates } from "../services/batchProcessor.js";

/**
 * Controller for receiving BULK batch files from frontend.
 */
async function uploadBatchHandler(req, res) {
  try {
    const { executionDate, lineCount, fileId, operationType, data } = req.body;

    if (!executionDate || !fileId || !operationType || !data) {
      return res.status(400).json({ error: "Missing required fields (executionDate, fileId, operationType, data)" });
    }

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(400).json({ error: "Data must be a non-empty array of records" });
    }

    // Call model function to save to the filesystem and track in batch_info.txt
    const result = await insertInfoFile({ 
      executionDate, 
      lineCount: lineCount || data.length, 
      fileId, 
      operationType, 
      fileData: data 
    });

    res.json(result);
  } catch (err) {
    console.error("uploadBatchHandler failed:", err);
    res.status(500).json({ error: "Failed to upload batch file locally" });
  }
}

/**
 * Controller for active4G API.
 */
async function active4GHandler(req, res) {
  try {
    console.log("Request Body:req", req.body );

    const {
      id,
      msisdn,
      action,
      signContractDate,
      templateName,
      userLogin,
      fileId,
      notificationMsisdn,
      notificationTemplate,
      coId,
      promo,
    } = req.body;

    // Basic validation
    if ( !msisdn || !action ) {
      return res.status(400).json({ error: "Missing required fields" });
    }

    // Call model function
    const result = await InsertSet3g({
      id,
      msisdn,
      action,
      signContractDate,
      templateName,
      userLogin,
      fileId,
      notificationMsisdn,
      notificationTemplate,
      promo,
      coId,
    });

    console.log("result issssss", result);
    res.json(result);
  } catch (err) {
    console.error("active4GHandler failed:", err);
    res.status(500).json({ error: "Failed to activate 4G profile" });
  }
}


/**
 * Controller for fetching batch results from ESB_LOG.
 */
async function resultBatchHandler(req, res) {
  try {
    const { 
      fileId, 
      searchMsisdn, msisdn, 
      searchTransactionId, transactionId 
    } = req.body;

    const results = await getResultBatch({ 
      fileId,
      searchMsisdn: searchMsisdn || msisdn,
      searchTransactionId: searchTransactionId || transactionId,
      page: req.body.page,
      limit: req.body.pageSize || req.body.limit
    });
    res.json(results);
  } catch (err) {
    console.error("resultBatchHandler failed:", err);
    res.status(500).json({ error: err.message || "Failed to fetch batch results" });
  }
}

export async function getHistoryHandler(req, res) {
  try {
    const history = await getBatchHistory();
    res.json(history);
  } catch(err) {
    console.error("getHistoryHandler failed:", err);
    res.status(500).json({ error: "Failed to fetch history" });
  }
}

export async function deleteHistoryHandler(req, res) {
  try {
    const { id } = req.params;
    const result = await deleteBatchHistory(id);
    if(result.success) {
      res.json({ message: "Deleted successfully" });
    } else {
      res.status(404).json({ error: result.message });
    }
  } catch(err) {
      console.error("deleteHistoryHandler failed:", err);
      res.status(500).json({ error: "Failed to delete from history" });
  }
}

// ============================================================
// BATCH CONTROL HANDLERS: Pause / Resume / Cancel
// ============================================================

/**
 * Pause a currently running batch.
 * POST /api/users/batch-control/pause  { fileId }
 */
export async function pauseBatchHandler(req, res) {
  try {
    const { fileId } = req.body;
    if (!fileId) return res.status(400).json({ error: "fileId is required" });
    const result = pauseBatch(fileId);
    if (result.success) {
      res.json(result);
    } else {
      res.status(400).json(result);
    }
  } catch (err) {
    console.error("pauseBatchHandler failed:", err);
    res.status(500).json({ error: "Failed to pause batch" });
  }
}

/**
 * Resume a paused batch.
 * POST /api/users/batch-control/resume  { fileId }
 */
export async function resumeBatchHandler(req, res) {
  try {
    const { fileId } = req.body;
    if (!fileId) return res.status(400).json({ error: "fileId is required" });
    const result = resumeBatch(fileId);
    if (result.success) {
      res.json(result);
    } else {
      res.status(400).json(result);
    }
  } catch (err) {
    console.error("resumeBatchHandler failed:", err);
    res.status(500).json({ error: "Failed to resume batch" });
  }
}

/**
 * Cancel a running, paused, or pending batch.
 * POST /api/users/batch-control/cancel  { fileId }
 */
export async function cancelBatchHandler(req, res) {
  try {
    const { fileId } = req.body;
    if (!fileId) return res.status(400).json({ error: "fileId is required" });

    // Try cancelling a running/paused batch first
    const runtimeResult = cancelBatch(fileId);
    if (runtimeResult.success) {
      return res.json(runtimeResult);
    }

    // If not running, try cancelling a PENDING batch in the tracking file
    const pendingResult = await cancelPendingBatch(fileId);
    if (pendingResult.success) {
      return res.json(pendingResult);
    }

    res.status(400).json({ error: pendingResult.message || runtimeResult.message || "Cannot cancel this batch" });
  } catch (err) {
    console.error("cancelBatchHandler failed:", err);
    res.status(500).json({ error: "Failed to cancel batch" });
  }
}

/**
 * Get all active/pending batches (the queue view).
 * GET /api/users/batch-queue
 */
export async function batchQueueHandler(req, res) {
  try {
    const activeBatches = await getActiveBatches();
    const runtimeStates = getBatchStates();
    
    // Merge runtime states with batch data
    const enriched = activeBatches.map(batch => ({
      ...batch,
      runtimeState: runtimeStates[batch.fileId] || null
    }));

    res.json(enriched);
  } catch (err) {
    console.error("batchQueueHandler failed:", err);
    res.status(500).json({ error: "Failed to fetch batch queue" });
  }
}

export { uploadBatchHandler, active4GHandler, resultBatchHandler };
