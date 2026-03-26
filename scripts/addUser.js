import { createUser } from "../src/models/authModel.js";

const username = process.argv[2];
const password = process.argv[3];

if (!username || !password) {
  console.log("Usage: node scripts/addUser.js <username> <password>");
  process.exit(1);
}

try {
  await createUser(username, password);
  console.log(`User '${username}' created successfully!`);
} catch (error) {
  console.error("Error creating user:", error);
}
