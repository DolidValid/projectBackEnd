import  getConnection  from "../config/db.js";

async function insertInfoFile({ firstName, lastName, email, phone }) {
  let connection;
  try {
    connection = await getConnection();

    const sql = `      
Insert into INFO_FILE
   (ID, FILE_NAME, SOURCE_FILE, RECORD_NUMBER, UPLOAD_DATE, 
    INSERT_DATE, EXECUTION_DATE, EXECUTION_DELAY, ETAT, IS_LOCKED, 
    OPERATION_FILE, NBR_ERROR_LINES, USER_BATCH, USER_AD)
 Values
   (:email, 'Set3GProfile_192025_13919', 'FILE', :phone, 
    TO_DATE('01/09/2025 13:09:01', 'DD/MM/YYYY HH24:MI:SS'), 
    TO_DATE('01/09/2025 13:09:01', 'DD/MM/YYYY HH24:MI:SS'), 
    TO_DATE('01/09/2025 13:09:14', 'DD/MM/YYYY HH24:MI:SS'), 
    5, 'C', 0, 'FILE', 0, :firstName, :lastName)`;

    await connection.execute(sql, { firstName, lastName, email, phone }, { autoCommit: true });

    return { success: true, message: "User inserted successfully" };
  } catch (err) {
    console.error("Insert user failed:", err);
    throw err;
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (err) {
        console.error("Error closing DB connection:", err);
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
  try {
    connection = await getConnection();

    const sql = `
      INSERT INTO SET_3G_PROFILE_BATCH
        (ID, MSISDN, ACTION, SIGN_CONTRACT_3G_DATE, TEMPLATE_NAME, 
         USER_LOGIN, FILE_ID, NOTIFICATION_MSISDN, NOTIFICATION_TEMPLATE)
      VALUES
        (:id, :msisdn, :action, :signContractDate, :templateName,
         :userLogin, :fileId, :notificationMsisdn, :notificationTemplate)
    `;

    const binds = {
      id,
      msisdn,
      action,
      signContractDate,
      templateName: templateName || promo, // if you want promo as default
      userLogin: userLogin || "crmesb",   // default if not passed
      fileId: `Set3GProfile_${fileId}_${Date.now()}`, // dynamic unique file id
      notificationMsisdn: notificationMsisdn || msisdn,
      notificationTemplate: notificationTemplate || promo,
      jobId,
    };

    await connection.execute(sql, binds, { autoCommit: true });

    return { success: true, message: "Set3G record inserted successfully" };
  } catch (err) {
    console.error("InsertSet3g failed:", err);
    throw err;
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (err) {
        console.error("Error closing DB connection:", err);
      }
    }
  }
}


export { insertInfoFile, InsertSet3g };
