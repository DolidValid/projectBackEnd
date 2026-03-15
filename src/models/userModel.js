import getConnection from "../config/db.js";
import fs from "fs/promises";
import path from "path";

// FUNCTION TO GENERATE A UNIQUE ID (e.g., '2-UM8VDBJ')
function generateUniqueId() {
  const prefix = '2-';
  // Base32 character set (uppercase letters and numbers 2-7) to avoid ambiguity
  const charset = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  const length = 7; // Length of the random part
  let randomPart = '';

  for (let i = 0; i < length; i++) {
    const randomIndex = Math.floor(Math.random() * charset.length);
    randomPart += charset[randomIndex];
  }
  return prefix + randomPart;
}

async function insertInfoFile({ executionDate, lineCount, fileId, operationType, fileData }) {
  console.info(`[insertInfoFile] Starting bulk insertion for ${operationType} with params:`, { executionDate, lineCount, fileId });

  try {
    // 1. Generate a unique ID for this insertion
    const newId = generateUniqueId();
    console.debug("[insertInfoFile] Generated new ID:", newId);

    // 2. Prepare data object for batch_info.txt tracking
    const dataObj = {
      id: newId,
      fileId: fileId,
      operationType: operationType, // e.g. "CREATE_CONTRACT"
      recordNumber: lineCount,
      executionDate: executionDate,
      uploadDate: new Date().toISOString(),
      etat: "PENDING" // Timer looks for this to process it
    };

    // 3. Append to the master tracking text file
    const trackingFilePath = path.join(process.cwd(), 'batch_info.txt');
    const lineToWrite = JSON.stringify(dataObj) + '\n';
    await fs.appendFile(trackingFilePath, lineToWrite, 'utf8');

    // 4. Create operation-specific folder
    const batchDirectory = path.join(process.cwd(), "batches", operationType);
    await fs.mkdir(batchDirectory, { recursive: true });

    // 5. Write the massive JSON payload to `[fileId].txt`
    const payloadFilePath = path.join(batchDirectory, `${fileId}.txt`);
    // Alternatively, writing as JSON string for easy parsing later
    await fs.writeFile(payloadFilePath, JSON.stringify(fileData, null, 2), 'utf8');

    console.info(`[insertInfoFile] Successfully saved bulk data to ${payloadFilePath} and tracked in batch_info.txt with ID:`, newId);
    return { success: true, message: "Batch payload saved locally and tracked", generatedId: newId, folder: operationType };

  } catch (err) {
    console.error("[insertInfoFile] Insert to file failed:", err);
    throw err; // Re-throw the error for the caller to handle
  }
}



// FUNCTION TO GENERATE A UNIQUE ID (e.g., '2-UM8VDBJ')
function generateUnicId() {
  const prefix = '2-';
  // Base32 character set (uppercase letters and numbers 2-7) to avoid ambiguity
  const charset = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  const length = 9; // Length of the random part
  let randomPart = '';

  for (let i = 0; i < length; i++) {
    const randomIndex = Math.floor(Math.random() * charset.length);
    randomPart += charset[randomIndex];
  }
  return prefix + randomPart;
}

async function InsertSet3g({
  id, // This parameter is now optional. If not provided, it will be generated.
  msisdn,
  action,
  signContractDate,
  templateName,
  userLogin,
  fileId,
  notificationMsisdn,
  notificationTemplate,
}) {
  let connection;
  console.info("[InsertSet3g] Starting insertion...");

  try {
    connection = await getConnection();
    console.log("[InsertSet3g] DB connection established");

    // 1. Generate a unique ID if one was not provided
    const finalId = id ?? generateUnicId();
    console.debug("[InsertSet3g] Using ID:", finalId);

    const sql = `
      INSERT INTO SET_3G_PROFILE_BATCH
        (ID, MSISDN, ACTION, SIGN_CONTRACT_3G_DATE, TEMPLATE_NAME, 
         USER_LOGIN, FILE_ID, NOTIFICATION_MSISDN, NOTIFICATION_TEMPLATE)
      VALUES
        (:id, :msisdn, :actionVal, :signContractDate, :templateName,
         :userLogin, :fileId, :notificationMsisdn, :notificationTemplate)
    `;

    // NOTICE: changed :action → :actionVal to avoid reserved keyword conflict
    const binds = {
      id: finalId, // Use the generated or provided ID here
      msisdn: msisdn ?? null,
      actionVal: action ?? null,
      signContractDate: signContractDate ?? null,
      templateName: templateName || "promo",
      userLogin: userLogin || "crmesb",
      fileId: fileId ?? null,
      notificationMsisdn: notificationMsisdn ?? null,
      notificationTemplate: notificationTemplate ?? null,
    };

    //console.debug("[InsertSet3g] Executing SQL:", sql);
    //console.debug("[InsertSet3g] With binds:", binds);

    // 2. Use explicit transaction control (commit/rollback) for better reliability
    await connection.execute(sql, binds);
    await connection.commit(); // Commit the transaction

    console.info("[InsertSet3g] Set3G record inserted successfully with ID:", finalId);
    return { 
      success: true, 
      message: "Set3G record inserted successfully",
      generatedId: finalId // Return the ID that was used
    };
  } catch (err) {
    console.error("[InsertSet3g] Insert failed:", err);
    // Rollback in case of any error
    if (connection) await connection.rollback();
    throw err;
  } finally {
    if (connection) {
      try {
        await connection.close();
        console.log("[InsertSet3g] DB connection closed");
      } catch (err) {
        console.error("[InsertSet3g] Error closing DB connection:", err);
      }
    }
  }
}




async function fetchJobs({ fileId = null, infoFileId = null, msisdn = null }) {
  let connection;
  console.info("[fetchJobs] Starting fetch with params:", { fileId, infoFileId, msisdn });

  try {
    connection = await getConnection();
    console.log("[fetchJobs] DB connection established");

    let sql = "";
    let binds = {};

    if (fileId) {
      // CASE A: direct fileId
      sql = `
        SELECT J.TRANSACTIONID, J.MSISDN, J.STATUS
        FROM JOBS J
        WHERE J.TRANSACTIONID IN (
          SELECT S.JOB_ID
          FROM SET_3G_PROFILE_BATCH S
          WHERE S.FILE_ID = :fileId
        )
      `;
      binds = { fileId };

    } else if (infoFileId) {
      // CASE B: infoFileId → lookup file_name from INFO_FILE
      sql = `
        SELECT J.TRANSACTIONID, J.MSISDN, J.STATUS
        FROM JOBS J
        WHERE J.TRANSACTIONID IN (
          SELECT S.JOB_ID
          FROM SET_3G_PROFILE_BATCH S
          WHERE S.FILE_ID IN (
            SELECT I.FILE_NAME
            FROM INFO_FILE I
            WHERE I.ID = :infoFileId
          )
        )
      `;
      binds = { infoFileId };

    } else if (msisdn) {
      // CASE C: direct msisdn search
      sql = `
        SELECT J.TRANSACTIONID, J.CREATIONDATE,J.MSISDN, J.STATUS,J.FILE_LINE_ID
        FROM JOBS J
        WHERE J.MSISDN = :msisdn
      `;
      binds = { msisdn };

    } else {
      throw new Error("At least one parameter (fileId, infoFileId, or msisdn) must be provided ❗");
    }

    console.debug("[fetchJobs] Executing SQL:", sql);
    console.debug("[fetchJobs] With binds:", binds);

    const result = await connection.execute(sql, binds, { outFormat: 4002 }); 
    // 4002 = oracledb.OUT_FORMAT_OBJECT → rows as array of objects

    console.info("[fetchJobs] Query executed successfully, rows:", result.rows.length);
    
    return result.rows; // Tableau (array of rows)
  } catch (err) {
    console.error("[fetchJobs] Query failed:", err);
    throw err;
  } finally {
    if (connection) {
      try {
        await connection.close();
        console.log("[fetchJobs] DB connection closed");
      } catch (err) {
        console.error("[fetchJobs] Error closing DB connection:", err);
      }
    }
  }
}




export { fetchJobs,insertInfoFile, InsertSet3g };
