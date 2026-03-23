import oracledb from "oracledb";

// ============================================================
// ESB_LOG Oracle Database Connection Configuration
// ============================================================
// This connection is used to query the TRANSACTION_STATE table
// in the ESB_LOG database to fetch batch activation results.
//
// ⚠️ IMPORTANT: Replace the credentials below with your actual
//    ESB_LOG database credentials before using this connection.
// ============================================================

const esbLogDbConfig = {
  user: "ESB_LOG",         // ← Replace with your ESB_LOG username
  password: "l0$!13f", // ← Replace with your ESB_LOG password
  connectString: "salesbodbp-scan:1521/SRV_TIBCOPR_LB" // ← Replace with your ESB_LOG connect string
};

async function getEsbLogConnection() {
  try {
    return await oracledb.getConnection(esbLogDbConfig);
  } catch (err) {
    console.error("[ESB_LOG] Oracle DB connection failed:", err);
    throw err;
  }
}

export default getEsbLogConnection;
