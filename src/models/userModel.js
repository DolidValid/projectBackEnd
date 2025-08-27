import  getConnection  from "../config/db.js";

async function insertUser({ firstName, lastName, email, phone }) {
  let connection;
  try {
    connection = await getConnection();

    const sql = `
      INSERT INTO USERS (FIRST_NAME, LAST_NAME, EMAIL, PHONE)
      VALUES (:firstName, :lastName, :email, :phone)
    `;

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

export default insertUser ;
