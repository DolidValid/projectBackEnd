import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";
import bcrypt from "bcryptjs";

// 1. Get the directory of the current file (src/models)
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// 2. Go up one level (..) to 'src', then into 'config'
const USERS_FILE = path.join(__dirname, "..", "config", "users.json");

// ... the rest of your functions remain the same

const readUsers = async () => {
  try {
    const data = await fs.readFile(USERS_FILE, "utf8");
    return JSON.parse(data);
  } catch (err) {
    if (err.code === "ENOENT") {
      // If file doesn't exist, create it with empty array
      await fs.writeFile(USERS_FILE, "[]", "utf8");
      return [];
    }
    throw err;
  }
};

const writeUsers = async (users) => {
  await fs.writeFile(USERS_FILE, JSON.stringify(users, null, 2), "utf8");
};

export const findUserByUsername = async (username) => {
  const users = await readUsers();
  return users.find((u) => u.username === username);
};

export const createUser = async (username, password) => {
  const users = await readUsers();
  const hashedPassword = await bcrypt.hash(password, 10);
  
  users.push({
    username,
    password: hashedPassword,
    createdAt: new Date().toISOString(),
  });

  await writeUsers(users);
  return { success: true };
};
