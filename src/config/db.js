import oracledb from "oracledb";

const dbConfig = {
  user: "your_db_username",
  password: "your_db_password",
  connectString: "your_oracle_server_ip:1521/ORCLPDB1"
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