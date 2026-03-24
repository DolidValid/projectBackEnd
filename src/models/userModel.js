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











export async function getBatchHistory() {
  const trackingFilePath = path.join(process.cwd(), 'src', 'batch_info.txt'); // Adjusted to 'src' based on where I found the file initially, or maybe just check if it exists in src or cwd
  // Let's actually check process.cwd. The original code does `path.join(process.cwd(), 'batch_info.txt')`. Let's stick to their pattern but also try checking if `batch_info.txt` is in src.
  let validPath = path.join(process.cwd(), 'batch_info.txt');
  try { await fs.access(validPath); } catch (e) { validPath = path.join(process.cwd(), 'src', 'batch_info.txt'); }
  
  try {
    const data = await fs.readFile(validPath, 'utf8');
    const lines = data.split('\n').filter(line => line.trim() !== '');
    const history = lines.map(line => {
      try { return JSON.parse(line); } catch(e) { return null; }
    }).filter(item => item !== null);
    
    // Filter by last month
    const oneMonthAgo = new Date();
    oneMonthAgo.setMonth(oneMonthAgo.getMonth() - 1);
    
    return history.filter(item => {
      if (!item.uploadDate) return false;
      const uploadDate = new Date(item.uploadDate);
      return uploadDate >= oneMonthAgo;
    });
  } catch(err) {
    if(err.code === 'ENOENT') return [];
    throw err;
  }
}

export async function deleteBatchHistory(id) {
  let validPath = path.join(process.cwd(), 'batch_info.txt');
  try { await fs.access(validPath); } catch (e) { validPath = path.join(process.cwd(), 'src', 'batch_info.txt'); }
  
  try {
    const data = await fs.readFile(validPath, 'utf8');
    const lines = data.split('\n').filter(line => line.trim() !== '');
    
    const newLines = [];
    let deletedItem = null;
    
    for(const line of lines) {
      try {
        const item = JSON.parse(line);
        if(item.id === id) {
          deletedItem = item;
        } else {
          newLines.push(line);
        }
      } catch(e) {
         newLines.push(line);
      }
    }
    
    if(deletedItem) {
      await fs.writeFile(validPath, newLines.join('\n') + (newLines.length > 0 ? '\n' : ''), 'utf8');
      
      if(deletedItem.operationType && deletedItem.fileId) {
        let payloadFilePath = path.join(process.cwd(), "batches", deletedItem.operationType, `${deletedItem.fileId}.txt`);
        try { await fs.access(payloadFilePath); } catch (e) { payloadFilePath = path.join(process.cwd(), 'src', "batches", deletedItem.operationType, `${deletedItem.fileId}.txt`); }
        try {
          await fs.unlink(payloadFilePath);
        } catch(e) {
          console.error("Failed to delete payload file:", e);
        }
      }
      return { success: true };
    } else {
       return { success: false, message: "Batch not found" };
    }
  } catch(err) {
    throw err;
  }
}

export { insertInfoFile, InsertSet3g };
