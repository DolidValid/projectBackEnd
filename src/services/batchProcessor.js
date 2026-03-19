import fs from 'fs/promises';
import path from 'path';
import cron from 'node-cron';
import axios from 'axios';

const TRACKING_FILE = path.join(process.cwd(), 'batch_info.txt');

// ============================================================
// THROTTLE / TPS CONFIGURATION
// ============================================================
// Adjust these values to control the rate of outgoing SOAP calls.
// - TPS: Target transactions-per-second (e.g. 5 = max 5 calls/sec)
// - DELAY_BETWEEN_CALLS_MS: Computed from TPS (1000/TPS ms between calls)
// - BURST_SIZE: How many calls to send before pausing (set to 1 for strict throttle)
// - TIMEOUT_MS: Per-request timeout so a slow call doesn't block the queue
// ============================================================
const THROTTLE_CONFIG = {
  TPS: parseInt(process.env.BATCH_TPS) || 5,              // 5 calls per second by default
  BURST_SIZE: parseInt(process.env.BATCH_BURST) || 1,      // 1 = strictly sequential
  TIMEOUT_MS: parseInt(process.env.BATCH_TIMEOUT) || 30000, // 30s per request
};
THROTTLE_CONFIG.DELAY_BETWEEN_CALLS_MS = Math.ceil(1000 / THROTTLE_CONFIG.TPS);

console.log(`[BatchProcessor] Throttle config: ${THROTTLE_CONFIG.TPS} TPS, ${THROTTLE_CONFIG.DELAY_BETWEEN_CALLS_MS}ms delay, burst=${THROTTLE_CONFIG.BURST_SIZE}, timeout=${THROTTLE_CONFIG.TIMEOUT_MS}ms`);

// --- Helpers ---

/** Sleep for given milliseconds */
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Extract TransactionId from a SOAP XML response body.
 * Works for both success and fault responses.
 * Looks for <ns:TransactionId>...</ns:TransactionId> or <TransactionId>...</TransactionId>
 */
function extractTransactionId(xmlString) {
  if (!xmlString || typeof xmlString !== 'string') return null;
  // Try namespaced version first: <ns:TransactionId>...
  let match = xmlString.match(/<[^>]*?TransactionId[^>]*?>([^<]+)<\/[^>]*?TransactionId>/i);
  if (match && match[1]) return match[1].trim();
  return null;
}

/**
 * Update the batch_info.txt tracking file for a specific fileId.
 * Merges new fields (like transactionIds) into the matching JSON line.
 */
async function updateBatchTracking(fileId, updates) {
  try {
    let fileContent;
    try {
      fileContent = await fs.readFile(TRACKING_FILE, 'utf8');
    } catch (err) {
      if (err.code === 'ENOENT') return;
      throw err;
    }

    const lines = fileContent.trim().split('\n');
    const updatedLines = [];
    let changed = false;

    for (const line of lines) {
      if (!line.trim()) continue;
      try {
        const batch = JSON.parse(line);
        if (batch.fileId === fileId) {
          // Merge all update fields into this batch line
          Object.assign(batch, updates);
          changed = true;
        }
        updatedLines.push(JSON.stringify(batch));
      } catch {
        updatedLines.push(line); // keep corrupted lines as-is
      }
    }

    if (changed) {
      await fs.writeFile(TRACKING_FILE, updatedLines.join('\n') + '\n', 'utf8');
    }
  } catch (err) {
    console.error(`[BatchProcessor] Error updating tracking for ${fileId}:`, err.message);
  }
}

// --- Processors ---

async function processCreateContract(fileId, filePath, dataArray) {
  console.log(`[BatchProcessor] 🚀 Executing processCreateContract for File ID: ${fileId} with ${dataArray.length} records.`);
  
  const generateSoapRequest = (record) => {
    // Helper to handle the "ID#VALUE|ID#VALUE" pattern seen in the image
    const parseList = (inputString) => {
      if (!inputString || typeof inputString !== 'string' || inputString.trim() === "") return [];
      return inputString.split('|').map(item => {
        const parts = item.split('#');
        return {
          id: parts[0] || "",
          value: parts[1] || ""
        };
      });
    };

    // Helper for case-insensitive key lookup to prevent empty payloads
    const getVal = (key) => {
      const foundKey = Object.keys(record).find(k => k.toLowerCase() === key.toLowerCase());
      return foundKey ? record[foundKey] : "";
    };

    // Post-processor: remove any empty XML tags like <Tag></Tag> or <Tag/> and blank lines
    const removeEmptyTags = (xml) => {
      // Repeatedly remove empty tags (handles nested empty parents)
      let cleaned = xml;
      let prev;
      do {
        prev = cleaned;
        // Remove <Tag></Tag> (with optional whitespace inside)
        cleaned = cleaned.replace(/<(\w+)([^>]*)>\s*<\/\1>/g, '');
        // Remove self-closing <Tag/>
        cleaned = cleaned.replace(/<(\w+)([^>]*)\s*\/>/g, (match, tag) => {
          // Keep intentional self-closing tags like <wsh:userLogin/> and <wsh:notification/>
          if (tag.includes(':')) return match;
          return '';
        });
      } while (cleaned !== prev);
      // Remove blank lines left behind
      cleaned = cleaned.replace(/^\s*[\r\n]/gm, '');
      return cleaned;
    };
  
    const checkItems = parseList(getVal('CHECK_LIST'));
    const comboItems = parseList(getVal('COMBO_LIST'));
    const textItems = parseList(getVal('TEXT_LIST'));
  
    const rawXml = `
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:wsh="http://www.ooredoo.dz/wsheader" xmlns:set="http://www.ooredoo.dz/ws/contract/setContractAndServices">
   <soapenv:Header>
      <wsse:Security soapenv:mustUnderstand="0" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
         <wsse:UsernameToken wsu:Id="UsernameToken-16739353" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
            <wsse:Username>soaesb</wsse:Username>
            <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">esbsoa2014</wsse:Password>
         </wsse:UsernameToken>
      </wsse:Security>
      <wsh:userLogin/>
      <wsh:notification/>
   </soapenv:Header>
   <soapenv:Body>
      <set:setContractAndServices>
         <SUBSCRIPTOR>
            <IdClient>${getVal('CS_ID') || "?"}</IdClient>
            <Contract>
               <IdSubscription>${getVal('CO_ID')}</IdSubscription>
               <Status>${getVal('CONTRACT_STATUS')}</Status>
               <StatusReason>${getVal('STATUS_REASON')}</StatusReason>
               <Action>${getVal('ACTION')}</Action>
               <ContractOwner>${getVal('MSISDN')}</ContractOwner>
               <BillDetail>${getVal('BILL_DETAIL')}</BillDetail>
               <Currency>${getVal('CURRENCY')}</Currency>
               <ContractContext>${getVal('TEMPLATE_NAME')}</ContractContext>
               <Resource>${getVal('SIM_NUM')}</Resource>
               <SubcriptionDate>${getVal('SUBSCRIPTION_DATE')}</SubcriptionDate>
               
               <Product>
                  <Idproduct>${getVal('TMCODE')}</Idproduct>
                  <Package>
                     <IdPackage>${getVal('SP_CODE')}</IdPackage>
                     <Service>
                        <IdService>${getVal('SN_CODE')}</IdService>
                        <Status>${getVal('STATUS')}</Status>
                        <ChargingCode>${getVal('CHARGING_CODE')}</ChargingCode>
                        <ChargingAmount>${getVal('CHARGING_AMOUNT')}</ChargingAmount>
                        <ChargingFrequency>${getVal('CHARGING_FREQUENCY')}</ChargingFrequency>
                        <ServiceContext>${getVal('SERVICE_CONTEXT')}</ServiceContext>
                        <ServiceExpiryDate>${getVal('EXPIRY_DATE')}</ServiceExpiryDate>
                        <Consumer>
                           <ServiceUser>${getVal('SN_CODE') ? getVal('MSISDN') : ''}</ServiceUser>
                        </Consumer>
                     </Service>
                  </Package>
               </Product>

               <ContractInfo>
                  ${checkItems.map(item => `
                  <Check>
                     <CheckId>${item.id}</CheckId>
                     <CheckValue>${item.value}</CheckValue>
                  </Check>`).join('')}
                  ${comboItems.map(item => `
                  <Combo>
                     <ComboId>${item.id}</ComboId>
                     <ComboValue>${item.value}</ComboValue>
                  </Combo>`).join('')}
                  ${textItems.map(item => `
                  <Text>
                     <TextId>${item.id}</TextId>
                     <TextValue>${item.value}</TextValue>
                  </Text>`).join('')}
               </ContractInfo>
            </Contract>

            <SetOfferId>
               <action>${getVal('ACTION_SETOFFERID')}</action>
               <offerId>${getVal('OFFERID')}</offerId>
               <offerProviderID>${getVal('OFFERPROVIDERID')}</offerProviderID>
               <offerType>${getVal('OFFERTYPE')}</offerType>
            </SetOfferId>
         </SUBSCRIPTOR>
      </set:setContractAndServices>
   </soapenv:Body>
</soapenv:Envelope>`.trim();

    // Clean up: remove all empty XML tags automatically
    return removeEmptyTags(rawXml);
  };

  const endpointUrl = 'http://127.0.0.1:0170/BusinessProcess/Interfaces/intfContract-service.serviceagent/ContractEndPoint';
  const logDir = path.join(process.cwd(), 'logs');
  const logFile = path.join(logDir, `${fileId}.log`);
  
  // Ensure log directory exists
  try { await fs.mkdir(logDir, { recursive: true }); } catch(e){}

  const logMessage = async (msg) => {
    const timestamp = new Date().toISOString();
    const formattedMsg = `[${timestamp}] ${msg}\n`;
    console.log(msg); // still log to console
    await fs.appendFile(logFile, formattedMsg, 'utf8');
  };

  await logMessage(`🚀 Starting processCreateContract for File ID: ${fileId} with ${dataArray.length} records.`);
  await logMessage(`⚙️ Throttle: ${THROTTLE_CONFIG.TPS} TPS (${THROTTLE_CONFIG.DELAY_BETWEEN_CALLS_MS}ms between calls)`);

  // Counters for execution summary
  let successCount = 0;
  let failCount = 0;
  const startTime = Date.now();

  for (let i = 0; i < dataArray.length; i++) {
    const record = dataArray[i];
    await logMessage(`--- Processing Line ${i+1} of ${dataArray.length} ---`);
    const soapPayload = generateSoapRequest(record);
    
    let transactionId = null;
    let lineStatus = 'FAILED';
    let errorMessage = null;

    try {
      await logMessage(`Sending SOAP request to ${endpointUrl}...`);
      await logMessage(`Payload snippet:\n${soapPayload.substring(0, 500)}...`);
      
      const response = await axios.post(endpointUrl, soapPayload, {
        headers: {
          'Content-Type': 'text/xml;charset=UTF-8',
          'SOAPAction': '"/BusinessProcess/Interfaces/intfContract-service.serviceagent/ContractEndPoint/SetContractAndServicesOperation"'
        },
        timeout: THROTTLE_CONFIG.TIMEOUT_MS
      });
      
      // Extract TransactionId from success response
      const responseBody = typeof response.data === 'string' ? response.data : JSON.stringify(response.data);
      transactionId = extractTransactionId(responseBody);
      lineStatus = 'SUCCESS';
      successCount++;

      await logMessage(`✅ Line ${i+1} SUCCESS. WS Response Status: ${response.status}`);
      await logMessage(`📋 TransactionId: ${transactionId || 'N/A'}`);
      await logMessage(`WS Response Body:\n${responseBody}`);
      
    } catch (err) {
      failCount++;
      errorMessage = err.message;
      await logMessage(`❌ Line ${i+1} FAILED. Error Message: ${err.message}`);
      if (err.response) {
        // Extract TransactionId even from error/fault responses
        const errBody = typeof err.response.data === 'string' ? err.response.data : JSON.stringify(err.response.data);
        transactionId = extractTransactionId(errBody);
        await logMessage(`WS Error Status: ${err.response.status}`);
        await logMessage(`📋 TransactionId (from fault): ${transactionId || 'N/A'}`);
        await logMessage(`WS Error Response Body:\n${errBody}`);
      }
    }

    // Update the record directly in dataArray with transactionId and status
    dataArray[i].transactionId = transactionId || null;
    dataArray[i].wsStatus = lineStatus;
    if (errorMessage) {
      dataArray[i].wsError = errorMessage;
    }

    // Write updated dataArray back to the batch payload file after EACH line
    // So if the server crashes, we don't lose progress — each line's transactionId is persisted
    try {
      await fs.writeFile(filePath, JSON.stringify(dataArray, null, 2), 'utf8');
      await logMessage(`💾 Batch file updated: line ${i+1} → transactionId=${transactionId || 'N/A'}, status=${lineStatus}`);
    } catch (writeErr) {
      await logMessage(`⚠️ Failed to write batch file after line ${i+1}: ${writeErr.message}`);
    }

    // Update batch_info.txt progress
    await updateBatchTracking(fileId, {
      progress: `${i + 1}/${dataArray.length}`,
      etat: 'IN_PROGRESS'
    });

    // Throttle: wait between calls to respect TPS limit
    if (i < dataArray.length - 1) {
      await sleep(THROTTLE_CONFIG.DELAY_BETWEEN_CALLS_MS);
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);
  const actualTps = (dataArray.length / parseFloat(elapsed)).toFixed(2);

  await logMessage(`📊 Execution summary: ${successCount} success, ${failCount} failed, ${elapsed}s elapsed, actual TPS: ${actualTps}`);
  await logMessage(`✅ Finished processCreateContract for File ID: ${fileId}`);

  // Final update: mark batch_info.txt as PROCESSED with summary
  await updateBatchTracking(fileId, {
    progress: `${dataArray.length}/${dataArray.length}`,
    etat: 'PROCESSED',
    executionSummary: {
      total: dataArray.length,
      success: successCount,
      failed: failCount,
      elapsedSeconds: parseFloat(elapsed),
      actualTps: parseFloat(actualTps)
    }
  });
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

            // 5. Mark as processed - the processor itself updates batch_info.txt
            //    with transactionIds and etat='PROCESSED' at the end.
            //    We still set hasChanges to ensure we save any other updates.
            hasChanges = true;

          } catch (processErr) {
            console.error(`[BatchProcessor] ❌ Error executing batch ${batch.fileId}:`, processErr);
            batch.etat = 'ERROR';
            batch.errorMessage = processErr.message;
            hasChanges = true;
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
