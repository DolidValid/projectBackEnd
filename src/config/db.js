import oracledb from "oracledb";

oracledb.initOracleClient({ libDir: "C:\\Program Files\\instantclient-basic-windows.x64-21.8.0.0.0dbru\\instantclient_21_8" }); 

const dbConfig = {
  user: "ESB_BATCH",
  password: "esb_batch123",
  connectString: "salesbodbp-scan:1521/SRV_TIBCOPR_LB"
};

async function getConnection() {
  try {
    // Use the getConnection method from the imported oracledb object
    return await oracledb.getConnection(dbConfig);
  } catch (err) {
    console.error("Oracle DB connection failed:", err);
    throw err;
  }
}

export default getConnection;