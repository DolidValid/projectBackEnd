import fs from 'fs/promises';
import path from 'path';
import getEsbLogConnection from '../config/dbEsbLog.js';

const TRACKING_FILE = path.join(process.cwd(), 'batch_info.txt');

/**
 * Service: getResultBatch
 * 
 * This service handles two modes of operation:
 * 1. BATCH MODE: If a fileId is provided and exists in local tracking, it reads the 
 *    batch payload file to merge local data (MSISDN) with DB status.
 * 2. GLOBAL MODE: If no batch is found locally, it performs a direct query on the 
 *    ESB_LOG TRANSACTION_STATE table by MSISDN, TransactionID, or FileID.
 */
async function getResultBatch({ fileId, searchMsisdn, searchTransactionId, page = 1, limit = 50 }) {
  console.info(`[getResultBatch] Mode Detection for: fileId=${fileId}, MSISDN=${searchMsisdn}, TxId=${searchTransactionId}`);

  let batchEntry = null;

  // ─────────────────────────────────────────────────────────────────────────
  // STEP 1: Check if we should operate in BATCH MODE
  // ─────────────────────────────────────────────────────────────────────────
  if (fileId && fileId !== 'all') {
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
        } catch (e) {}
      }
    } catch (err) {
      console.warn(`[getResultBatch] Note: batch_info.txt not found or inaccessible. Falling back to global search.`);
    }
  }

  // If we found a batch entry, proceed with BATCH MODE (Payload + DB merge)
  if (batchEntry) {
    console.info(`[getResultBatch] -> BATCH MODE ACTIVE for ${fileId}`);
    return await getBatchModeResults(batchEntry, { searchMsisdn, searchTransactionId, page, limit });
  }

  // Otherwise, proceed with GLOBAL MODE (Direct DB query)
  console.info(`[getResultBatch] -> GLOBAL MODE ACTIVE`);
  return await getGlobalModeResults({ fileId, searchMsisdn, searchTransactionId, page, limit });
}

/** 
 * INTERNAL: Batch Mode logic (Reads payload file and queries specific Transaction IDs)
 */
async function getBatchModeResults(batchEntry, { searchMsisdn, searchTransactionId, page, limit }) {
  const fileId = batchEntry.fileId;
  const operationType = batchEntry.operationType || 'CREATE_CONTRACT';
  const batchDirectory = path.join(process.cwd(), 'batches', operationType);
  const payloadFilePath = path.join(batchDirectory, `${fileId}.txt`);

  let dataArray = [];
  try {
    const payloadContent = await fs.readFile(payloadFilePath, 'utf8');
    dataArray = JSON.parse(payloadContent);
  } catch (err) {
    // Fallback: if payload file is missing but entry exists, we can still try global mode for that ID
    console.warn(`[getResultBatch] Payload file missing at ${payloadFilePath}. Falling back to global mode.`);
    return await getGlobalModeResults({ fileId, searchMsisdn, searchTransactionId, page, limit });
  }

  // Apply filters to payload
  let filteredData = dataArray;
  if (searchMsisdn) {
    filteredData = filteredData.filter(r => (r.msisdn || r.MSISDN || '').toString().includes(searchMsisdn));
  }
  if (searchTransactionId) {
    filteredData = filteredData.filter(r => (r.transactionId || '').includes(searchTransactionId));
  }

  const totalRecords = filteredData.length;
  const offset = (page - 1) * limit;
  const pageData = filteredData.slice(offset, offset + limit);

  const transactionIds = pageData
    .map(r => r.transactionId)
    .filter(id => id && id.trim() !== '');

  let dbResults = [];
  if (transactionIds.length > 0) {
    let connection;
    try {
      connection = await getEsbLogConnection();
      const bindNames = transactionIds.map((_, i) => `:t${i}`);
      const binds = {};
      transactionIds.forEach((id, i) => binds[`t${i}`] = id);

      const sql = `
        SELECT TRANSACTION_ID, MAIN_INPUT, CREATION_DATE, STATUS_DATE, STATUS, 
               ADDITIONAL_INPUT, MSGCODE, STEP, TRACE_INFO
        FROM TRANSACTION_STATE 
        WHERE TRANSACTION_ID IN (${bindNames.join(', ')})
      `;

      const result = await connection.execute(sql, binds, { outFormat: 4002 });
      dbResults = result.rows;
    } catch (err) {
      console.error(`[getResultBatch] DB Query failed in Batch Mode:`, err);
      // We continue with empty DB results so user can at least see local payload data
    } finally {
      if (connection) await connection.close();
    }
  }

  // Merge Local + DB
  const merged = pageData.map(record => {
    const dbRow = dbResults.find(row => row.TRANSACTION_ID === record.transactionId);
    return {
      msisdn: record.msisdn || record.MSISDN || '',
      transactionId: record.transactionId || '',
      TRANSACTION_ID: record.transactionId || '',
      wsStatus: record.wsStatus || '',
      // Map DB fields
      STATUS: dbRow?.STATUS || record.wsStatus || 'PENDING',
      MAIN_INFO: dbRow?.MAIN_INPUT || record.wsError || '',
      PROCESS_ADDITION_MSG: dbRow?.ADDITIONAL_INPUT || '',
      TRACE_IN_STEP: dbRow?.TRACE_INFO || dbRow?.STEP || '',
      CREATION_DATE: dbRow?.CREATION_DATE || '',
      STATUS_DATE: dbRow?.STATUS_DATE || ''
    };
  });

  return {
    batchInfo: batchEntry,
    transactionResults: merged,
    pagination: {
      page,
      limit,
      totalRecords,
      totalPages: Math.ceil(totalRecords / limit)
    }
  };
}

/**
 * INTERNAL: Global Mode logic (Direct DB query with filters)
 */
async function getGlobalModeResults({ fileId, searchMsisdn, searchTransactionId, page, limit }) {
  let connection;
  try {
    connection = await getEsbLogConnection();
    let whereClauses = [];
    let binds = {};

    if (searchTransactionId) {
      whereClauses.push("TRANSACTION_ID = :searchTransactionId");
      binds.searchTransactionId = searchTransactionId;
    }

    if (searchMsisdn) {
      const clean = searchMsisdn.replace(/^213/, '').replace(/^0/, '');
      const variants = [searchMsisdn, clean, '0' + clean, '213' + clean];
      const uniqueVariants = [...new Set(variants)];
      const msisdnBinds = uniqueVariants.map((_, i) => `:m${i}`).join(', ');
      whereClauses.push(`(ADDITIONAL_INPUT IN (${msisdnBinds}) OR MAIN_INPUT IN (${msisdnBinds}))`);
      uniqueVariants.forEach((v, i) => binds[`m${i}`] = v);
    }

    if (fileId && fileId !== 'all') {
      whereClauses.push("(MAIN_INPUT = :fileId OR ADDITIONAL_INPUT = :fileId)");
      binds.fileId = fileId;
    }

    let whereSql = whereClauses.length > 0 ? "WHERE " + whereClauses.join(" AND ") : "WHERE CREATION_DATE > SYSDATE - 10";

    const countSql = `SELECT COUNT(*) FROM TRANSACTION_STATE ${whereSql}`;
    const countResult = await connection.execute(countSql, binds);
    const totalRecords = countResult.rows[0][0];

    const offset = (page - 1) * limit;
    const dataSql = `
      SELECT TRANSACTION_ID, MAIN_INPUT, CREATION_DATE, STATUS_DATE, STATUS, 
             ADDITIONAL_INPUT, MSGCODE, STEP, TRACE_INFO
      FROM TRANSACTION_STATE 
      ${whereSql}
      ORDER BY CREATION_DATE DESC
      OFFSET :offset ROWS FETCH NEXT :maxRows ROWS ONLY
    `;
    
    const pageBinds = { ...binds, offset, maxRows: limit };
    const result = await connection.execute(dataSql, pageBinds, { outFormat: 4002 });

    const mapped = result.rows.map(row => ({
      transactionId: row.TRANSACTION_ID,
      TRANSACTION_ID: row.TRANSACTION_ID,
      STATUS: row.STATUS,
      MAIN_INFO: row.MAIN_INPUT,
      PROCESS_ADDITION_MSG: row.ADDITIONAL_INPUT,
      TRACE_IN_STEP: row.TRACE_INFO || row.STEP,
      CREATION_DATE: row.CREATION_DATE,
      STATUS_DATE: row.STATUS_DATE
    }));

    return {
      transactionResults: mapped,
      pagination: {
        page,
        limit,
        totalRecords,
        totalPages: Math.ceil(totalRecords / limit)
      }
    };
  } finally {
    if (connection) await connection.close();
  }
}

export { getResultBatch };
