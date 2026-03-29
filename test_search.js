import { getResultBatch } from './src/services/resultBatchService.js';

async function test() {
  try {
    const fileId = "SetContractAndServices_23032026_103712"; // Exist in batch_info
    const result = await getResultBatch({ fileId });
    console.log("BATCH MODE RESULT:", JSON.stringify(result, null, 2));

    const globalResult = await getResultBatch({ fileId: 'all' });
    console.log("GLOBAL MODE RESULT count:", globalResult.transactionResults.length);
  } catch (err) {
    console.error("TEST FAILED:", err);
  }
}

test();
