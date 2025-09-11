import { insertInfoFile, InsertSet3g, fetchJobs } from "../models/userModel.js";

/**
 * Controller for inserting user info.
 */
async function addInfoFiles(req, res) {
  try {
    console.log(req.body);

    const { firstName, lastName, email, phone } = req.body;

    if (!firstName || !lastName || !email || !phone) {
      return res.status(400).json({ error: "All fields are required" });
    }

    const result = await insertInfoFile({ firstName, lastName, email, phone });

    res.json(result);
  } catch (err) {
    res.status(500).json({ error: "Failed to insert user" });
  }
}

/**
 * Controller for active4G API.
 */
async function active4GHandler(req, res) {
  try {
    console.log("Request Body:");

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
      jobId,
      promo,
    } = req.body;

    // Basic validation
    if (!id || !msisdn || !action || !fileId || !jobId) {
      const a = res.status(400).json({ error: "Missing required fields" });
      console.log("a issssss", a);
      return a;
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

export { addInfoFiles, active4GHandler, fetchJobsHandler };
