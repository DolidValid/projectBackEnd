import getConnection from "../config/db.js";

async function insertInfoFile({ firstName, lastName, email, phone }) {
  let connection;
  console.info("[insertInfoFile] Starting insertion with params:", { firstName, lastName, email, phone });

  try {
    connection = await getConnection();
    console.log("[insertInfoFile] DB connection established");

    const sql = `      
      INSERT INTO INFO_FILE
        (ID, FILE_NAME, SOURCE_FILE, RECORD_NUMBER, UPLOAD_DATE, 
         INSERT_DATE, EXECUTION_DATE, EXECUTION_DELAY, ETAT, IS_LOCKED, 
         OPERATION_FILE, NBR_ERROR_LINES, USER_BATCH, USER_AD)
      VALUES
        (:email, 'Set3GProfile_192025_13919', 'FILE', :phone, 
         TO_DATE('01/09/2025 13:09:01', 'DD/MM/YYYY HH24:MI:SS'), 
         TO_DATE('01/09/2025 13:09:01', 'DD/MM/YYYY HH24:MI:SS'), 
         TO_DATE('01/09/2025 13:09:14', 'DD/MM/YYYY HH24:MI:SS'), 
         5, 'C', 0, 'FILE', 0, :firstName, :lastName)`;

    console.debug("[insertInfoFile] Executing SQL:", sql);

    await connection.execute(sql, { firstName, lastName, email, phone }, { autoCommit: true });

    console.info("[insertInfoFile] User inserted successfully");
    return { success: true, message: "User inserted successfully" };
  } catch (err) {
    console.error("[insertInfoFile] Insert failed:", err);
    throw err;
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

async function InsertSet3g({
  id,
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
      id: id ?? null,
      msisdn: msisdn ?? null,
      actionVal: action ?? null,
      signContractDate: signContractDate ?? null,
      templateName: templateName || "promo",
      userLogin: userLogin || "crmesb",
      fileId: fileId ?? null,
      notificationMsisdn: notificationMsisdn ?? null,
      notificationTemplate: notificationTemplate ?? null,
    };

    console.debug("[InsertSet3g] Executing SQL:", sql);
    console.debug("[InsertSet3g] With binds:", binds);

    await connection.execute(sql, binds, { autoCommit: true });

    console.info("[InsertSet3g] Set3G record inserted successfully");
    return { success: true, message: "Set3G record inserted successfully" };
  } catch (err) {
    console.error("[InsertSet3g] Insert failed:", err);
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
        SELECT J.TRANSACTIONID, J.MSISDN, J.STATUS
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
