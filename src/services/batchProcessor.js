import fs from 'fs/promises';
import path from 'path';
import cron from 'node-cron';
import axios from 'axios';

const TRACKING_FILE = path.join(process.cwd(), 'batch_info.txt');

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

  for (let i = 0; i < dataArray.length; i++) {
    const record = dataArray[i];
    await logMessage(`--- Processing Line ${i+1} of ${dataArray.length} ---`);
    const soapPayload = generateSoapRequest(record);
    
    try {
      await logMessage(`Sending SOAP request to ${endpointUrl}...`);
      await logMessage(`Payload snippet:\n${soapPayload.substring(0, 500)}...`);
      
      const response = await axios.post(endpointUrl, soapPayload, {
        headers: {
          'Content-Type': 'text/xml;charset=UTF-8',
          'SOAPAction': '"/BusinessProcess/Interfaces/intfContract-service.serviceagent/ContractEndPoint/SetContractAndServicesOperation"'
        }
      });
      
      await logMessage(`✅ Line ${i+1} SUCCESS. WS Response Status: ${response.status}`);
      await logMessage(`WS Response Body:\n${JSON.stringify(response.data, null, 2)}`);
      
    } catch (err) {
      await logMessage(`❌ Line ${i+1} FAILED. Error Message: ${err.message}`);
      if (err.response) {
        await logMessage(`WS Error Status: ${err.response.status}`);
        await logMessage(`WS Error Response Body:\n${JSON.stringify(err.response.data, null, 2)}`);
      }
    }
  }

  await logMessage(`✅ Finished processCreateContract for File ID: ${fileId}`);
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
