import { insertInfoFile, InsertSet3g, fetchJobs } from "../models/userModel.js";
import { getResultBatch } from "../services/resultBatchService.js";

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
 * Controller for fetching Jobs.
 */
async function fetchJobsHandler(req, res) {
  try {
    console.log("fetchJobsHandler Request Body:", req.body);

    const { fileId, infoFileId, msisdn } = req.body;

    // Require at least one param
    if (!fileId && !infoFileId && !msisdn) {
      return res
        .status(400)
        .json({ error: "At least one of fileId, infoFileId, or msisdn is required" });
    }

    const jobs = await fetchJobs({ fileId, infoFileId, msisdn });

    res.json(jobs);
  } catch (err) {
    console.error("fetchJobsHandler failed:", err);
    res.status(500).json({ error: "Failed to fetch jobs" });
  }
}

/**
 * Controller for fetching batch results from ESB_LOG.
 */
async function resultBatchHandler(req, res) {
  try {
    const { fileId } = req.body;

    if (!fileId) {
      return res.status(400).json({ error: "fileId is required" });
    }

    const results = await getResultBatch(fileId);
    res.json(results);
  } catch (err) {
    console.error("resultBatchHandler failed:", err);
    res.status(500).json({ error: err.message || "Failed to fetch batch results" });
  }
}

export { uploadBatchHandler, active4GHandler, fetchJobsHandler, resultBatchHandler };
