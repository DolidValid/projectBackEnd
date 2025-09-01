import  insertInfoFile  from "../models/userModel.js";

async function addInfoFiles(req, res) {
  try {
    console.log(req.body);

    const { firstName, lastName, email, phone } = req.body;

    if (!firstName || !lastName || !email || !phone) {
      return res.status(400).json({ error: "All fields are required" });
    }

    const result = await insertInfoFile({ firstName, lastName, email, phone });

    res.json(result);
  } catch (err) {
    res.status(500).json({ error: "Failed to insert user" });
  }
}

export default addInfoFiles;
