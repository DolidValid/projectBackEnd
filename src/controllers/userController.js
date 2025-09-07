import  {insertInfoFile,InsertSet3g}  from "../models/userModel.js";

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
    console.log("Request Body:", req.body);

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
      jobId,
      promo,
    });

    res.json(result);
  } catch (err) {
    console.error("active4GHandler failed:", err);
    res.status(500).json({ error: "Failed to activate 4G profile" });
  }
}


export { addInfoFiles, active4GHandler };
