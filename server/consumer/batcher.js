import { logActivity } from '../lib/activity.js';

// Batch buffer with Inactivity Timeout:
// 1. If buffer reaches batchSize (20): Flushes immediately!
// 2. If no new event arrives for flushIntervalMs (default 30s): Flushes whatever is in buffer (e.g. 15 items) without waiting for 20!
export function createLocationBatcher({
  repository,
  batchSize = 20,
  flushIntervalMs = 30000, // 30 seconds inactivity timeout
  activity = logActivity,
}) {
  let buffer = [];
  let timer = null;
  let isFlushing = false;

  async function flush(triggerReason = 'target_reached') {
    if (buffer.length === 0 || isFlushing) return;
    isFlushing = true;

    if (timer) {
      clearTimeout(timer);
      timer = null;
    }

    // Pull batchSize items (or all items if timeout or shutdown)
    const itemsToFlush = buffer.splice(0, batchSize);

    if (triggerReason === 'inactivity_timeout') {
      console.log(
        `\n⏰ [BATCH TIMEOUT] 30s tak koi naya event nahi aaya! Flushing ${itemsToFlush.length} items to MongoDB (no waiting for ${batchSize})...\n`
      );
    } else {
      console.log(
        `\n🚀 [BATCH FLUSH] Target reached (${itemsToFlush.length} items)! Updating MongoDB in ONE batch now...\n`
      );
    }

    try {
      activity('mongo.batch.flushing', {
        batchCount: itemsToFlush.length,
        remainingInBuffer: buffer.length,
        triggerReason,
      });

      await repository.saveBatch(itemsToFlush);
    } catch (error) {
      console.error(`❌ [BATCH ERROR] Failed to save batch to MongoDB: ${error.message}`);
      buffer = [...itemsToFlush, ...buffer].slice(0, 1000);
      activity('mongo.batch.error', { count: itemsToFlush.length, reason: error.message }, 'error');
    } finally {
      isFlushing = false;
      // If items still remain in buffer:
      if (buffer.length >= batchSize) {
        void flush('target_reached');
      } else if (buffer.length > 0 && flushIntervalMs) {
        timer = setTimeout(() => void flush('inactivity_timeout'), flushIntervalMs);
      }
    }
  }

  function add(location) {
    buffer.push(location);

    console.log(
      `📦 [BATCH BUFFER] (${buffer.length}/${batchSize}) collected | Order: ${location.orderId} | Rider: ${location.riderId} | Lat: ${location.latitude.toFixed(4)}, Lng: ${location.longitude.toFixed(4)}`
    );

    activity('mongo.batch.buffering', {
      orderId: location.orderId,
      currentCount: buffer.length,
      targetBatchSize: batchSize,
    });

    // Case 1: Target reached (20 events) -> Flush immediately!
    if (buffer.length >= batchSize) {
      if (timer) {
        clearTimeout(timer);
        timer = null;
      }
      void flush('target_reached');
      return;
    }

    // Case 2: Under 20 events -> Start/Reset 30-second inactivity timer
    // Agar agle 30 seconds tak koi event nahi aaya, toh remaining events flush ho jayenge!
    if (flushIntervalMs) {
      clearTimeout(timer);
      timer = setTimeout(() => void flush('inactivity_timeout'), flushIntervalMs);
    }
  }

  async function stop() {
    if (timer) {
      clearTimeout(timer);
      timer = null;
    }
    // On worker shutdown, flush any remaining items so nothing is lost
    while (buffer.length > 0) {
      console.log(`⚠️ [WORKER SHUTDOWN] Flushing remaining ${buffer.length} items to MongoDB...`);
      await flush('shutdown').catch(() => {});
    }
  }

  return {
    add,
    flush,
    stop,
    getPendingCount: () => buffer.length,
  };
}
