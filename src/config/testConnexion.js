import getConnection from "./db.js"; // adjust path if needed

async function test() {
  try {
    const connection = await getConnection();
    console.log("✅ Connection established!");

    // Quick test query
    const result = await connection.execute("SELECT 1 AS TEST FROM dual");
    console.log("Query Result:", result.rows);

    // Always close
    await connection.close();
    console.log("🔒 Connection closed.");
  } catch (err) {
    console.error("❌ Test failed:", err);
  }
}

test();
