import { logAudit } from './src/models/userModel.js';

async function test() {
  console.log("Writing test audit logs...");
  await logAudit("admin_test", "TEST_UPLOAD", "Testing unified path and local time");
  await logAudit("admin_test", "TEST_PAUSE", "Testing pause trace");
  console.log("Done.");
  process.exit(0);
}

test();
