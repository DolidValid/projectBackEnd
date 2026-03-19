import fs from 'fs/promises';
import path from 'path';
import getEsbLogConnection from '../config/dbEsbLog.js';

const TRACKING_FILE = path.join(process.cwd(), 'batch_info.txt');

/**
 * Service: resultBatch
 * 
 * 1. Receives a fileId
 * 2. Reads batch_info.txt to find the matching batch entry
 * 3. Reads the batch payload file to extract transactionIds
 * 4. Queries the ESB_LOG Oracle DB (TRANSACTION_STATE table) 
 *    using those transactionIds
 * 5. Returns the combined results
 */
async function getResultBatch(fileId) {
  console.info(`[resultBatch] Starting for fileId: ${fileId}`);

  // ───────────────────────────────────────────────
  // STEP 1: Find the batch entry in batch_info.txt
  // ───────────────────────────────────────────────
  let batchEntry = null;

  try {
    const fileContent = await fs.readFile(TRACKING_FILE, 'utf8');
    const lines = fileContent.trim().split('\n');

    for (const line of lines) {
      if (!line.trim()) continue;
      try {
        const batch = JSON.parse(line);
        if (batch.fileId === fileId) {
          batchEntry = batch;
          break;
        }
      } catch {
        // Skip corrupted lines
      }
    }
  } catch (err) {
    if (err.code === 'ENOENT') {
      throw new Error('batch_info.txt not found. No batches have been uploaded yet.');
    }
    throw err;
  }

  if (!batchEntry) {
    throw new Error(`No batch found with fileId: ${fileId}`);
  }

  console.info(`[resultBatch] Found batch entry:`, batchEntry);

  // ───────────────────────────────────────────────
  // STEP 2: Read the batch payload file to get transactionIds
  // ───────────────────────────────────────────────
  const operationType = batchEntry.operationType || 'CREATE_CONTRACT';
  const batchDirectory = path.join(process.cwd(), 'batches', operationType);
  const payloadFilePath = path.join(batchDirectory, `${fileId}.txt`);

  let dataArray = [];
  try {
    const payloadContent = await fs.readFile(payloadFilePath, 'utf8');
    dataArray = JSON.parse(payloadContent);
  } catch (err) {
    throw new Error(`Could not read batch payload file at ${payloadFilePath}: ${err.message}`);
  }

  // Extract transaction IDs from the batch records
  const transactionIds = dataArray
    .map(record => record.transactionId)
    .filter(id => id && id.trim() !== '');

  if (transactionIds.length === 0) {
    return {
      batchInfo: batchEntry,
      message: 'No transaction IDs found in the batch file. The batch may not have been processed yet.',
      transactionResults: []
    };
  }

  console.info(`[resultBatch] Found ${transactionIds.length} transaction IDs to query`);

  // ───────────────────────────────────────────────
  // STEP 3: Query ESB_LOG Oracle DB for results
  // ───────────────────────────────────────────────
  let connection;
  try {
    connection = await getEsbLogConnection();
    console.log('[resultBatch] ESB_LOG DB connection established');

    // Build dynamic IN clause with bind variables
    // Oracle bind variables: :t0, :t1, :t2, ...
    const bindNames = transactionIds.map((_, i) => `:t${i}`);
    const binds = {};
    transactionIds.forEach((id, i) => {
      binds[`t${i}`] = id;
    });

    const sql = `
      SELECT 
        TRANSACTION_ID,
        MAIN_INFO,
        CREATION_DATE,
        STATUS_DATE,
        STATUS,
        PROCESS_ADDITION_MSG,
        MSG_CODE,
        TRACE_IN_STEP
      FROM TRANSACTION_STATE 
      WHERE TRANSACTION_ID IN (${bindNames.join(', ')})
    `;

    console.debug('[resultBatch] Executing SQL with', transactionIds.length, 'bind variables');

    const result = await connection.execute(sql, binds, { 
      outFormat: 4002 // oracledb.OUT_FORMAT_OBJECT
    });

    console.info(`[resultBatch] Query returned ${result.rows.length} rows`);

    // Merge batch record data with Oracle results for a complete picture
    const mergedResults = dataArray.map(record => {
      const oracleResult = result.rows.find(
        row => row.TRANSACTION_ID === record.transactionId
      );
      return {
        // From the batch file
        msisdn: record.msisdn || record.MSISDN || '',
        fileId: record.fileId || '',
        transactionId: record.transactionId || '',
        wsStatus: record.wsStatus || '',
        // From Oracle ESB_LOG (if found)
        TRANSACTION_ID: oracleResult?.TRANSACTION_ID || record.transactionId || '',
        MAIN_INFO: oracleResult?.MAIN_INFO || '',
        CREATION_DATE: oracleResult?.CREATION_DATE || '',
        STATUS_DATE: oracleResult?.STATUS_DATE || '',
        STATUS: oracleResult?.STATUS || '',
        PROCESS_ADDITION_MSG: oracleResult?.PROCESS_ADDITION_MSG || '',
        MSG_CODE: oracleResult?.MSG_CODE || '',
        TRACE_IN_STEP: oracleResult?.TRACE_IN_STEP || ''
      };
    });

    return {
      batchInfo: batchEntry,
      totalRecords: dataArray.length,
      transactionIdsFound: transactionIds.length,
      oracleResultsCount: result.rows.length,
      transactionResults: mergedResults
    };

  } catch (err) {
    console.error('[resultBatch] Oracle query failed:', err);
    throw err;
  } finally {
    if (connection) {
      try {
        await connection.close();
        console.log('[resultBatch] ESB_LOG DB connection closed');
      } catch (err) {
        console.error('[resultBatch] Error closing ESB_LOG connection:', err);
      }
    }
  }
}

export { getResultBatch };
