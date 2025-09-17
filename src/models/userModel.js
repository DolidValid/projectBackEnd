import getConnection from "../config/db.js";

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

async function insertInfoFile({ executionDate, lineCount, fileId }) {
  let connection;
  console.info("[insertInfoFile] Starting insertion with params:", { executionDate, lineCount, fileId });

  try {
    connection = await getConnection();
    console.log("[insertInfoFile] DB connection established");

    // 1. Generate a unique ID for this insertion
    const newId = generateUniqueId();
    console.debug("[insertInfoFile] Generated new ID:", newId);

    // 2. SQL using bind variables for ALL parameters
    const sql = `      
      INSERT INTO INFO_FILE
        (ID, FILE_NAME, SOURCE_FILE, RECORD_NUMBER, UPLOAD_DATE,
         INSERT_DATE, EXECUTION_DATE, EXECUTION_DELAY, ETAT, IS_LOCKED,
         OPERATION_FILE, NBR_ERROR_LINES, USER_BATCH, USER_AD)
      VALUES
        (:id, :fileId, 'FILE', :lineCount,
         SYSDATE,
         SYSDATE,
         TO_DATE(:executionDate, 'DD/MM/YYYY HH24:MI:SS'),
         :lineCount, 'C', 0, 'FILE', 0, 'SOA_test', 'SOA_test')`;

    // 3. Prepare bind parameters object including the new ID
    const binds = {
      id: newId,
      fileId: fileId,
      lineCount: lineCount,
      executionDate: executionDate
    };

    console.debug("[insertInfoFile] Executing SQL with binds:", { sql, binds });

    // 4. Execute without autoCommit to control the transaction manually
    await connection.execute(sql, binds);
    await connection.commit(); // Explicitly commit the transaction

    console.info("[insertInfoFile] Record inserted successfully with ID:", newId);
    return { success: true, message: "Record inserted successfully", generatedId: newId };

  } catch (err) {
    console.error("[insertInfoFile] Insert failed:", err);
    // Rollback any changes in case of error
    if (connection) {
      await connection.rollback();
    }
    throw err; // Re-throw the error for the caller to handle
  } finally {
    if (connection) {
      try {
        await connection.close();
        console.log("[insertInfoFile] DB connection closed");
      } catch (err) {
        console.error("[insertInfoFile] Error closing DB connection:", err);
      }
    }
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
