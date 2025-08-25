const { insertUser } = require("../models/userModel");

async function addUser(req, res) {
  try {
    const { firstName, lastName, email, phone } = req.body;

    if (!firstName || !lastName || !email || !phone) {
      return res.status(400).json({ error: "All fields are required" });
    }

    const result = await insertUser({ firstName, lastName, email, phone });

    res.json(result);
  } catch (err) {
    res.status(500).json({ error: "Failed to insert user" });
  }
}

module.exports = { addUser };
