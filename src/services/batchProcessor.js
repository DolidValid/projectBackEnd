import fs from 'fs/promises';
import path from 'path';
import cron from 'node-cron';

const TRACKING_FILE = path.join(process.cwd(), 'batch_info.txt');

// --- Processors ---

async function processCreateContract(fileId, filePath, dataArray) {
  console.log(`[BatchProcessor] 🚀 Executing processCreateContract for File ID: ${fileId} with ${dataArray.length} records.`);
  // TODO: Add loop here to process your records, call Oracle APIs etc.
}

async function processSetStatus(fileId, filePath, dataArray) {
  console.log(`[BatchProcessor] 🚀 Executing processSetStatus for File ID: ${fileId} with ${dataArray.length} records.`);
  // TODO: Add loop here to process your records, call Oracle APIs etc.
}

async function processActivation3g(fileId, filePath, dataArray) {
  console.log(`[BatchProcessor] 🚀 Executing processActivation3g for File ID: ${fileId} with ${dataArray.length} records.`);
  // TODO: Add loop here to process your records, call Oracle APIs etc.
}

async function processActivateServiceParametre(fileId, filePath, dataArray) {
  console.log(`[BatchProcessor] 🚀 Executing processActivateServiceParametre for File ID: ${fileId} with ${dataArray.length} records.`);
  // TODO: Add loop here to process your records, call Oracle APIs etc.
}

async function processUpdateRatePlan(fileId, filePath, dataArray) {
  console.log(`[BatchProcessor] 🚀 Executing processUpdateRatePlan for File ID: ${fileId} with ${dataArray.length} records.`);
  // TODO: Add loop here to process your records, call Oracle APIs etc.
}

// --- Background Job ---

async function checkPendingBatches() {
  console.log("[BatchProcessor] ⏰ Checking for pending batches...");

  try {
    // 1. Read tracking file
    let fileContent;
    try {
      fileContent = await fs.readFile(TRACKING_FILE, 'utf8');
    } catch (err) {
      if (err.code === 'ENOENT') {
        // tracking file doesn't exist yet, simply ignore
        return;
      }
      throw err;
    }

    const lines = fileContent.trim().split('\n');
    let hasChanges = false;
    const updatedLines = [];

    const now = new Date();

    for (const line of lines) {
      if (!line) continue;

      try {
        const batch = JSON.parse(line);

        // Parse DD/MM/YYYY HH24:MI:SS or ISO String
        // InfoFile.jsx currently sends: DD/MM/YYYY HH:mm:ss
        let executionDate;
        if (batch.executionDate.includes('/')) {
            const [datePart, timePart] = batch.executionDate.split(' ');
            const [day, month, year] = datePart.split('/');
            executionDate = new Date(`${year}-${month}-${day}T${timePart}`);
        } else {
            // ISO format fallback
            executionDate = new Date(batch.executionDate);
        }

        // 2. Check if we should process it
        if (batch.etat === 'PENDING' && now >= executionDate) {
          console.log(`[BatchProcessor] 👉 Found pending batch ready for execution: ${batch.fileId} (${batch.operationType})`);

          // 3. Locate the file payload
          const batchDirectory = path.join(process.cwd(), "batches", batch.operationType);
          const payloadFilePath = path.join(batchDirectory, `${batch.fileId}.txt`);
          
          let payloadContent = "[]";
          try {
              payloadContent = await fs.readFile(payloadFilePath, 'utf8');
          } catch (e) {
              console.error(`[BatchProcessor] ❌ Payload file missing for ${batch.fileId} at ${payloadFilePath}`);
          }
          
          const dataArray = JSON.parse(payloadContent);

          // 4. Route to specific processor
          try {
            switch (batch.operationType) {
              case "CREATE_CONTRACT":
                await processCreateContract(batch.fileId, payloadFilePath, dataArray);
                break;
              case "SET_STATUS":
                await processSetStatus(batch.fileId, payloadFilePath, dataArray);
                break;
              case "ACTIVATION_3G":
                await processActivation3g(batch.fileId, payloadFilePath, dataArray);
                break;
              case "ACTIVATE_SERVICE_PARAMETRE":
                await processActivateServiceParametre(batch.fileId, payloadFilePath, dataArray);
                break;
              case "UPDATE_RATE_PLAN":
                await processUpdateRatePlan(batch.fileId, payloadFilePath, dataArray);
                break;
              default:
                console.warn(`[BatchProcessor] ⚠️ Unknown operationType: ${batch.operationType}`);
            }

            // 5. Mark as processed!
            batch.etat = 'PROCESSED';
            hasChanges = true;

          } catch (processErr) {
            console.error(`[BatchProcessor] ❌ Error executing batch ${batch.fileId}:`, processErr);
            // Optionally set status to ERROR, but for now we leave it PENDING maybe to retry?
            // Next time it will try again. Or you can add batch.etat = 'ERROR'
          }
        }

        updatedLines.push(JSON.stringify(batch));
      } catch (parseErr) {
        console.error("[BatchProcessor] Error parsing line in batch_info.txt:", parseErr);
        // keep line exactly as is if corrupted
        updatedLines.push(line);
      }
    }

    // 6. Save updated tracking file safely overwriting the old one if needed
    if (hasChanges) {
      const newFileContent = updatedLines.join('\n') + '\n';
      await fs.writeFile(TRACKING_FILE, newFileContent, 'utf8');
      console.log(`[BatchProcessor] ✅ batch_info.txt updated successfully.`);
    }

  } catch (err) {
    console.error("[BatchProcessor] Core cron job error:", err);
  }
}

export function startBatchTimer() {
  // Run every 1 minute
  console.log("[BatchProcessor] Timer initialized. Will check for pending batches every minute.");
  cron.schedule('* * * * *', () => {
    checkPendingBatches();
  });
}
