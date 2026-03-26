import fs from 'fs/promises';
import path from 'path';
import cron from 'node-cron';
import axios from 'axios';

const TRACKING_FILE = path.join(process.cwd(), 'batch_info.txt');

// ============================================================
// THROTTLE / TPS CONFIGURATION
// ============================================================
const THROTTLE_CONFIG = {
  TPS: parseInt(process.env.BATCH_TPS) || 5,              // 5 calls per second by default
  BURST_SIZE: parseInt(process.env.BATCH_BURST) || 1,      // 1 = strictly sequential
  TIMEOUT_MS: parseInt(process.env.BATCH_TIMEOUT) || 30000, // 30s per request
};
THROTTLE_CONFIG.DELAY_BETWEEN_CALLS_MS = Math.ceil(1000 / THROTTLE_CONFIG.TPS);

console.log(`[BatchProcessor] Throttle config: ${THROTTLE_CONFIG.TPS} TPS, ${THROTTLE_CONFIG.DELAY_BETWEEN_CALLS_MS}ms delay, burst=${THROTTLE_CONFIG.BURST_SIZE}, timeout=${THROTTLE_CONFIG.TIMEOUT_MS}ms`);

// ============================================================
// BATCH STATE MANAGEMENT (Pause / Resume / Cancel)
// ============================================================
// Possible states: 'running' | 'paused' | 'cancelled'
const batchStates = new Map();

/**
 * Pause a running batch. The processor loop will wait until resumed or cancelled.
 */
export function pauseBatch(fileId) {
  const current = batchStates.get(fileId);
  if (current === 'running') {
    batchStates.set(fileId, 'paused');
    console.log(`[BatchProcessor] ⏸️  Batch ${fileId} PAUSED`);
    return { success: true, state: 'paused' };
  }
  return { success: false, message: `Cannot pause batch in state: ${current || 'not running'}` };
}

/**
 * Resume a paused batch.
 */
export function resumeBatch(fileId) {
  const current = batchStates.get(fileId);
  if (current === 'paused') {
    batchStates.set(fileId, 'running');
    console.log(`[BatchProcessor] ▶️  Batch ${fileId} RESUMED`);
    return { success: true, state: 'running' };
  }
  return { success: false, message: `Cannot resume batch in state: ${current || 'not running'}` };
}

/**
 * Cancel a running or paused batch. The processor loop will exit.
 */
export function cancelBatch(fileId) {
  const current = batchStates.get(fileId);
  if (current === 'running' || current === 'paused') {
    batchStates.set(fileId, 'cancelled');
    console.log(`[BatchProcessor] 🛑 Batch ${fileId} CANCELLED`);
    return { success: true, state: 'cancelled' };
  }
  return { success: false, message: `Cannot cancel batch in state: ${current || 'not running'}` };
}

/**
 * Get all batch states (for the API to query).
 */
export function getBatchStates() {
  const result = {};
  for (const [key, value] of batchStates) {
    result[key] = value;
  }
  return result;
}

/**
 * Wait while a batch is paused. Resolves when resumed or cancelled.
 * Returns 'running' or 'cancelled'.
 */
async function waitWhilePaused(fileId) {
  while (batchStates.get(fileId) === 'paused') {
    await sleep(500); // Check every 500ms
  }
  return batchStates.get(fileId); // 'running' or 'cancelled'
}

// --- Helpers ---

/** Sleep for given milliseconds */
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Extract TransactionId from a SOAP XML response body.
 */
function extractTransactionId(xmlString) {
  if (!xmlString || typeof xmlString !== 'string') return null;
  let match = xmlString.match(/<[^>]*?TransactionId[^>]*?>([^<]+)<\/[^>]*?TransactionId>/i);
  if (match && match[1]) return match[1].trim();
  return null;
}

/**
 * Update the batch_info.txt tracking file for a specific fileId.
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
          Object.assign(batch, updates);
          changed = true;
        }
        updatedLines.push(JSON.stringify(batch));
      } catch {
        updatedLines.push(line);
      }
    }

    if (changed) {
      await fs.writeFile(TRACKING_FILE, updatedLines.join('\n') + '\n', 'utf8');
    }
  } catch (err) {
    console.error(`[BatchProcessor] Error updating tracking for ${fileId}:`, err.message);
  }
}

/**
 * Get the current status of a batch from the tracking file.
 */
async function getBatchStatus(fileId) {
  try {
    const fileContent = await fs.readFile(TRACKING_FILE, 'utf8');
    const lines = fileContent.trim().split('\n');
    for (const line of lines) {
      if (!line.trim()) continue;
      const batch = JSON.parse(line);
      if (batch.fileId === fileId) {
        return batch.etat;
      }
    }
  } catch (err) {
    console.error(`[BatchProcessor] Error getting status for ${fileId}:`, err.message);
  }
  return null;
}

/**
 * Get all active/pending batches from the tracking file.
 */
export async function getActiveBatches() {
  try {
    let fileContent;
    try {
      fileContent = await fs.readFile(TRACKING_FILE, 'utf8');
    } catch (err) {
      if (err.code === 'ENOENT') return [];
      throw err;
    }

    const lines = fileContent.trim().split('\n');
    const activeBatches = [];

    for (const line of lines) {
      if (!line.trim()) continue;
      try {
        const batch = JSON.parse(line);
        // Include PENDING, IN_PROGRESS, PAUSED batches
        if (['PENDING', 'IN_PROGRESS', 'PAUSED'].includes(batch.etat)) {
          // Attach runtime state if available
          const runtimeState = batchStates.get(batch.fileId);
          activeBatches.push({
            ...batch,
            runtimeState: runtimeState || null
          });
        }
      } catch {
        // skip malformed lines
      }
    }

    return activeBatches;
  } catch (err) {
    console.error('[BatchProcessor] Error getting active batches:', err.message);
    return [];
  }
}

// Helper for case-insensitive key lookup
const getVal = (record, key) => {
  if (!record) return "";
  const foundKey = Object.keys(record).find(k => k.toLowerCase() === key.toLowerCase());
  return foundKey ? record[foundKey] : "";
};

// Post-processor: remove any empty XML tags
const removeEmptyTags = (xml) => {
  let cleaned = xml;
  let prev;
  do {
    prev = cleaned;
    cleaned = cleaned.replace(/<(\w+)([^>]*)>\s*<\/\1>/g, '');
    cleaned = cleaned.replace(/<(\w+)([^>]*)\s*\/>/g, (match, tag) => {
      if (tag.includes(':')) return match;
      return '';
    });
  } while (cleaned !== prev);
  cleaned = cleaned.replace(/^\s*[\r\n]/gm, '');
  return cleaned;
};

// Helper to handle the "ID#VALUE|ID#VALUE" pattern
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

// --- Processors ---

async function processCreateContract(fileId, filePath, dataArray) {
  const generateSoapRequest = (record) => {
    const checkItems = parseList(getVal(record, 'CHECK_LIST'));
    const comboItems = parseList(getVal(record, 'COMBO_LIST'));
    const textItems = parseList(getVal(record, 'TEXT_LIST'));
  
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
            <IdClient>${getVal(record, 'CS_ID') || "?"}</IdClient>
            <Contract>
               <IdSubscription>${getVal(record, 'CO_ID')}</IdSubscription>
               <Status>${getVal(record, 'CONTRACT_STATUS')}</Status>
               <StatusReason>${getVal(record, 'STATUS_REASON')}</StatusReason>
               <Action>${getVal(record, 'ACTION')}</Action>
               <ContractOwner>${getVal(record, 'MSISDN')}</ContractOwner>
               <BillDetail>${getVal(record, 'BILL_DETAIL')}</BillDetail>
               <Currency>${getVal(record, 'CURRENCY')}</Currency>
               <ContractContext>${getVal(record, 'TEMPLATE_NAME')}</ContractContext>
               <Resource>${getVal(record, 'SIM_NUM')}</Resource>
               <SubcriptionDate>${getVal(record, 'SUBSCRIPTION_DATE')}</SubcriptionDate>
               
               <Product>
                  <Idproduct>${getVal(record, 'TMCODE')}</Idproduct>
                  <Package>
                     <IdPackage>${getVal(record, 'SP_CODE')}</IdPackage>
                     <Service>
                        <IdService>${getVal(record, 'SN_CODE')}</IdService>
                        <Status>${getVal(record, 'STATUS')}</Status>
                        <ChargingCode>${getVal(record, 'CHARGING_CODE')}</ChargingCode>
                        <ChargingAmount>${getVal(record, 'CHARGING_AMOUNT')}</ChargingAmount>
                        <ChargingFrequency>${getVal(record, 'CHARGING_FREQUENCY')}</ChargingFrequency>
                        <ServiceContext>${getVal(record, 'SERVICE_CONTEXT')}</ServiceContext>
                        <ServiceExpiryDate>${getVal(record, 'EXPIRY_DATE')}</ServiceExpiryDate>
                        <Consumer>
                           <ServiceUser>${getVal(record, 'SN_CODE') ? getVal(record, 'MSISDN') : ''}</ServiceUser>
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
               <action>${getVal(record, 'ACTION_SETOFFERID')}</action>
               <offerId>${getVal(record, 'OFFERID')}</offerId>
               <offerProviderID>${getVal(record, 'OFFERPROVIDERID')}</offerProviderID>
               <offerType>${getVal(record, 'OFFERTYPE')}</offerType>
            </SetOfferId>
         </SUBSCRIPTOR>
      </set:setContractAndServices>
   </soapenv:Body>
</soapenv:Envelope>`.trim();
    return removeEmptyTags(rawXml);
  };

  const endpointUrl = 'http://127.0.0.1:0170/BusinessProcess/Interfaces/intfContract-service.serviceagent/ContractEndPoint';
  const logDir = path.join(process.cwd(), 'logs');
  const logFile = path.join(logDir, `${fileId}.log`);
  try { await fs.mkdir(logDir, { recursive: true }); } catch(e){}

  const logMessage = async (msg) => {
    const timestamp = new Date().toISOString();
    const formattedMsg = `[${timestamp}] ${msg}\n`;
    console.log(msg);
    await fs.appendFile(logFile, formattedMsg, 'utf8');
  };

  // Register this batch as running
  batchStates.set(fileId, 'running');

  await logMessage(`🚀 Starting processCreateContract for File ID: ${fileId} with ${dataArray.length} records.`);
  
  let successCount = 0;
  let failCount = 0;
  const startTime = Date.now();

  for (let i = 0; i < dataArray.length; i++) {
    // --- CHECK FOR PAUSE / CANCEL ---
    const currentState = batchStates.get(fileId);
    
    if (currentState === 'cancelled') {
      await logMessage(`🛑 Batch ${fileId} was CANCELLED at line ${i + 1}/${dataArray.length}`);
      await updateBatchTracking(fileId, {
        progress: `${i}/${dataArray.length}`,
        etat: 'CANCELLED',
        executionSummary: { total: dataArray.length, processed: i, success: successCount, failed: failCount, cancelledAt: i }
      });
      batchStates.delete(fileId);
      return;
    }

    if (currentState === 'paused') {
      await logMessage(`⏸️  Batch ${fileId} PAUSED at line ${i + 1}`);
      await updateBatchTracking(fileId, { etat: 'PAUSED', progress: `${i}/${dataArray.length}` });
      const resumedState = await waitWhilePaused(fileId);
      if (resumedState === 'cancelled') {
        await logMessage(`🛑 Batch ${fileId} was CANCELLED while paused at line ${i + 1}`);
        await updateBatchTracking(fileId, {
          progress: `${i}/${dataArray.length}`,
          etat: 'CANCELLED',
          executionSummary: { total: dataArray.length, processed: i, success: successCount, failed: failCount, cancelledAt: i }
        });
        batchStates.delete(fileId);
        return;
      }
      await logMessage(`▶️  Batch ${fileId} RESUMED at line ${i + 1}`);
      await updateBatchTracking(fileId, { etat: 'IN_PROGRESS' });
    }
    // --- END PAUSE / CANCEL CHECK ---

    const record = dataArray[i];
    await logMessage(`--- Processing Line ${i+1} of ${dataArray.length} ---`);
    const soapPayload = generateSoapRequest(record);
    
    let transactionId = null;
    let lineStatus = 'FAILED';
    let errorMessage = null;

    try {
      const response = await axios.post(endpointUrl, soapPayload, {
        headers: {
          'Content-Type': 'text/xml;charset=UTF-8',
          'SOAPAction': '"/BusinessProcess/Interfaces/intfContract-service.serviceagent/ContractEndPoint/SetContractAndServicesOperation"'
        },
        timeout: THROTTLE_CONFIG.TIMEOUT_MS
      });
      const responseBody = typeof response.data === 'string' ? response.data : JSON.stringify(response.data);
      transactionId = extractTransactionId(responseBody);
      lineStatus = 'SUCCESS';
      successCount++;
      await logMessage(`✅ Line ${i+1} SUCCESS. TxId: ${transactionId || 'N/A'}`);
    } catch (err) {
      failCount++;
      errorMessage = err.message;
      await logMessage(`❌ Line ${i+1} FAILED: ${err.message}`);
      if (err.response) {
        const errBody = typeof err.response.data === 'string' ? err.response.data : JSON.stringify(err.response.data);
        transactionId = extractTransactionId(errBody);
      }
    }

    dataArray[i].transactionId = transactionId || null;
    dataArray[i].wsStatus = lineStatus;
    if (errorMessage) dataArray[i].wsError = errorMessage;

    await fs.writeFile(filePath, JSON.stringify(dataArray, null, 2), 'utf8');
    await updateBatchTracking(fileId, {
      progress: `${i + 1}/${dataArray.length}`,
      etat: 'IN_PROGRESS'
    });

    if (i < dataArray.length - 1) await sleep(THROTTLE_CONFIG.DELAY_BETWEEN_CALLS_MS);
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);
  await logMessage(`📊 Finished ${fileId}: ${successCount} ok, ${failCount} fail in ${elapsed}s`);
  await updateBatchTracking(fileId, {
    progress: `${dataArray.length}/${dataArray.length}`,
    etat: 'PROCESSED',
    executionSummary: { total: dataArray.length, success: successCount, failed: failCount, elapsedSeconds: parseFloat(elapsed) }
  });
  batchStates.delete(fileId);
}

async function processSetStatus(fileId, filePath, dataArray) {
  console.log(`[BatchProcessor] 🚀 Executing processSetStatus for File ID: ${fileId} with ${dataArray.length} records.`);
  
  const generateSoapRequest = (record) => {
    const rawXml = `
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:wsh="http://www.ooredoo.dz/wsheader" xmlns:set="http://www.ooredoo.dz/ws/contract/setContractStatus">
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
      <set:setContractStatusRequest>
         <msisdn>${getVal(record, 'MSISDN')}</msisdn>
         <coId>${getVal(record, 'CO_ID')}</coId>
         <status>${getVal(record, 'STATUS')}</status>
         <reason>${getVal(record, 'REASON')}</reason>
         <validFrom>${getVal(record, 'VALID_FROM')}</validFrom>
         <templateName>${getVal(record, 'TEMPLATE_NAME')}</templateName>
      </set:setContractStatusRequest>
   </soapenv:Body>
</soapenv:Envelope>`.trim();
    return removeEmptyTags(rawXml);
  };

  const endpointUrl = 'http://127.0.0.1:0170/BusinessProcess/Interfaces/intfContract-service.serviceagent/ContractEndPoint';
  const logDir = path.join(process.cwd(), 'logs');
  const logFile = path.join(logDir, `${fileId}.log`);
  try { await fs.mkdir(logDir, { recursive: true }); } catch(e){}

  const logMessage = async (msg) => {
    const timestamp = new Date().toISOString();
    const formattedMsg = `[${timestamp}] ${msg}\n`;
    console.log(msg);
    await fs.appendFile(logFile, formattedMsg, 'utf8');
  };

  // Register this batch as running
  batchStates.set(fileId, 'running');

  await logMessage(`🚀 Starting processSetStatus for File ID: ${fileId} with ${dataArray.length} records.`);
  
  let successCount = 0;
  let failCount = 0;
  const startTime = Date.now();

  for (let i = 0; i < dataArray.length; i++) {
    // --- CHECK FOR PAUSE / CANCEL ---
    const currentState = batchStates.get(fileId);
    
    if (currentState === 'cancelled') {
      await logMessage(`🛑 Batch ${fileId} was CANCELLED at line ${i + 1}/${dataArray.length}`);
      await updateBatchTracking(fileId, {
        progress: `${i}/${dataArray.length}`,
        etat: 'CANCELLED',
        executionSummary: { total: dataArray.length, processed: i, success: successCount, failed: failCount, cancelledAt: i }
      });
      batchStates.delete(fileId);
      return;
    }

    if (currentState === 'paused') {
      await logMessage(`⏸️  Batch ${fileId} PAUSED at line ${i + 1}`);
      await updateBatchTracking(fileId, { etat: 'PAUSED', progress: `${i}/${dataArray.length}` });
      const resumedState = await waitWhilePaused(fileId);
      if (resumedState === 'cancelled') {
        await logMessage(`🛑 Batch ${fileId} was CANCELLED while paused at line ${i + 1}`);
        await updateBatchTracking(fileId, {
          progress: `${i}/${dataArray.length}`,
          etat: 'CANCELLED',
          executionSummary: { total: dataArray.length, processed: i, success: successCount, failed: failCount, cancelledAt: i }
        });
        batchStates.delete(fileId);
        return;
      }
      await logMessage(`▶️  Batch ${fileId} RESUMED at line ${i + 1}`);
      await updateBatchTracking(fileId, { etat: 'IN_PROGRESS' });
    }
    // --- END PAUSE / CANCEL CHECK ---

    const record = dataArray[i];
    await logMessage(`--- Processing Line ${i+1} of ${dataArray.length} ---`);
    const soapPayload = generateSoapRequest(record);
    
    let transactionId = null;
    let lineStatus = 'FAILED';
    let errorMessage = null;

    try {
      const response = await axios.post(endpointUrl, soapPayload, {
        headers: {
          'Content-Type': 'text/xml;charset=UTF-8',
          'SOAPAction': '"/BusinessProcess/Interfaces/intfContract-service.serviceagent//SetContractStatusOperation"'
        },
        timeout: THROTTLE_CONFIG.TIMEOUT_MS
      });
      const responseBody = typeof response.data === 'string' ? response.data : JSON.stringify(response.data);
      transactionId = extractTransactionId(responseBody);
      lineStatus = 'SUCCESS';
      successCount++;
      await logMessage(`✅ Line ${i+1} SUCCESS. TxId: ${transactionId || 'N/A'}`);
    } catch (err) {
      failCount++;
      errorMessage = err.message;
      await logMessage(`❌ Line ${i+1} FAILED: ${err.message}`);
      if (err.response) {
        const errBody = typeof err.response.data === 'string' ? err.response.data : JSON.stringify(err.response.data);
        transactionId = extractTransactionId(errBody);
      }
    }

    dataArray[i].transactionId = transactionId || null;
    dataArray[i].wsStatus = lineStatus;
    if (errorMessage) dataArray[i].wsError = errorMessage;

    await fs.writeFile(filePath, JSON.stringify(dataArray, null, 2), 'utf8');
    await updateBatchTracking(fileId, {
      progress: `${i + 1}/${dataArray.length}`,
      etat: 'IN_PROGRESS'
    });

    if (i < dataArray.length - 1) await sleep(THROTTLE_CONFIG.DELAY_BETWEEN_CALLS_MS);
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);
  await logMessage(`📊 Finished ${fileId}: ${successCount} ok, ${failCount} fail in ${elapsed}s`);
  await updateBatchTracking(fileId, {
    progress: `${dataArray.length}/${dataArray.length}`,
    etat: 'PROCESSED',
    executionSummary: { total: dataArray.length, success: successCount, failed: failCount, elapsedSeconds: parseFloat(elapsed) }
  });
  batchStates.delete(fileId);
}

async function processActivation3g(fileId, filePath, dataArray) {
  console.log(`[BatchProcessor] 🚀 Executing processActivation3g for File ID: ${fileId} with ${dataArray.length} records.`);
}

async function processActivateServiceParametre(fileId, filePath, dataArray) {
  console.log(`[BatchProcessor] 🚀 Executing processActivateServiceParametre for File ID: ${fileId} with ${dataArray.length} records.`);
}

async function processUpdateRatePlan(fileId, filePath, dataArray) {
  console.log(`[BatchProcessor] 🚀 Executing processUpdateRatePlan for File ID: ${fileId} with ${dataArray.length} records.`);
}

// --- Background Job ---

async function checkPendingBatches() {
  console.log("[BatchProcessor] ⏰ Checking for pending batches...");
  try {
    let fileContent;
    try {
      fileContent = await fs.readFile(TRACKING_FILE, 'utf8');
    } catch (err) {
      if (err.code === 'ENOENT') return;
      throw err;
    }

    const lines = fileContent.trim().split('\n');
    const now = new Date();

    for (const line of lines) {
      if (!line) continue;
      try {
        const batch = JSON.parse(line);
        let executionDate;
        if (batch.executionDate && batch.executionDate.includes('/')) {
            const [datePart, timePart] = batch.executionDate.split(' ');
            const [day, month, year] = datePart.split('/');
            executionDate = new Date(`${year}-${month}-${day}T${timePart}`);
        } else if (batch.executionDate) {
            executionDate = new Date(batch.executionDate);
        } else {
            executionDate = now;
        }

        if (batch.etat === 'PENDING' && now >= executionDate) {
          console.log(`[BatchProcessor] 👉 Processing: ${batch.fileId} (${batch.operationType})`);
          await updateBatchTracking(batch.fileId, { etat: 'IN_PROGRESS' });

          try {
            const batchDirectory = path.join(process.cwd(), "batches", batch.operationType);
            const payloadFilePath = path.join(batchDirectory, `${batch.fileId}.txt`);
            const payloadContent = await fs.readFile(payloadFilePath, 'utf8');
            const dataArray = JSON.parse(payloadContent);

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
                throw new Error(`Unknown operationType: ${batch.operationType}`);
            }
          } catch (processErr) {
            console.error(`[BatchProcessor] ❌ Error ${batch.fileId}:`, processErr);
            await updateBatchTracking(batch.fileId, { etat: 'ERROR', errorMessage: processErr.message });
          }
        }
      } catch (parseErr) {
        console.error("[BatchProcessor] Error parsing line:", parseErr);
      }
    }
  } catch (err) {
    console.error("[BatchProcessor] Core cron job error:", err);
  }
}

/**
 * Cancel a PENDING batch (before it even starts processing).
 * This updates the tracking file status directly.
 */
export async function cancelPendingBatch(fileId) {
  try {
    let fileContent;
    try {
      fileContent = await fs.readFile(TRACKING_FILE, 'utf8');
    } catch (err) {
      if (err.code === 'ENOENT') return { success: false, message: 'No tracking file found' };
      throw err;
    }

    const lines = fileContent.trim().split('\n');
    let found = false;

    for (const line of lines) {
      if (!line.trim()) continue;
      try {
        const batch = JSON.parse(line);
        if (batch.fileId === fileId && batch.etat === 'PENDING') {
          found = true;
          break;
        }
      } catch {}
    }

    if (found) {
      await updateBatchTracking(fileId, { etat: 'CANCELLED' });
      return { success: true, message: 'Pending batch cancelled' };
    }

    return { success: false, message: 'Batch not found or not in PENDING state' };
  } catch (err) {
    return { success: false, message: err.message };
  }
}

export function startBatchTimer() {
  console.log("[BatchProcessor] Timer initialized.");
  cron.schedule('* * * * *', () => {
    checkPendingBatches();
  });
}
