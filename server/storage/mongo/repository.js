import { logActivity } from '../../lib/activity.js';

export function createLocationRepository(model, activity = logActivity) {
  return {
    async saveIfNewer(location) {
      const filter = {
        _id: location.orderId,
        $or: [{ timestamp: { $lt: location.timestamp } }, { timestamp: { $exists: false } }],
      };
      const update = {
        $set: { ...location, persistedAt: new Date() },
        $push: {
          route: {
            latitude: location.latitude,
            longitude: location.longitude,
            timestamp: location.timestamp,
          },
        },
      };
      let result;
      try {
        result = await model.updateOne(filter, update, { upsert: true, runValidators: true });
      } catch (error) {
        if (error.code !== 11000) throw error;
        // Another delivery/worker may already own this order. Recheck the timestamp
        // without inserting; this also handles concurrent first writes safely.
        result = await model.updateOne(filter, update, { runValidators: true });
      }
      const saved = result.upsertedCount > 0 || result.modifiedCount > 0;
      activity(saved ? 'mongo.location.saved' : 'mongo.location.ignored', {
        ...location,
        collection: model.collection.collectionName,
      });
      return saved;
    },

    async saveBatch(locations) {
      if (!locations || locations.length === 0) return { modifiedCount: 0, upsertedCount: 0 };

      // Group locations by orderId to execute optimized bulk write per order
      const byOrder = new Map();
      for (const loc of locations) {
        if (!byOrder.has(loc.orderId)) {
          byOrder.set(loc.orderId, []);
        }
        byOrder.get(loc.orderId).push(loc);
      }

      const bulkOps = [];
      for (const [orderId, points] of byOrder.entries()) {
        // De-duplicate points by timestamp and sort chronologically
        const uniqueMap = new Map();
        for (const p of points) {
          uniqueMap.set(p.timestamp, p);
        }
        const sortedPoints = Array.from(uniqueMap.values()).sort((a, b) => a.timestamp - b.timestamp);
        const latest = sortedPoints[sortedPoints.length - 1];

        bulkOps.push({
          updateOne: {
            filter: {
              _id: orderId,
              $or: [{ timestamp: { $lte: latest.timestamp } }, { timestamp: { $exists: false } }],
            },
            update: {
              $set: {
                orderId: latest.orderId,
                riderId: latest.riderId,
                latitude: latest.latitude,
                longitude: latest.longitude,
                timestamp: latest.timestamp,
                persistedAt: new Date(),
              },
              $push: {
                route: {
                  $each: sortedPoints.map((p) => ({
                    latitude: p.latitude,
                    longitude: p.longitude,
                    timestamp: p.timestamp,
                  })),
                },
              },
            },
            upsert: true,
          },
        });
      }

      let result;
      try {
        result = await model.bulkWrite(bulkOps, { ordered: false });
      } catch (error) {
        if (error.code === 11000 || error.writeErrors?.some((e) => e.code === 11000)) {
          // Retry duplicate keys safely without upsert
          const retryOps = bulkOps.map((op) => ({
            updateOne: {
              ...op.updateOne,
              upsert: false,
            },
          }));
          result = await model.bulkWrite(retryOps, { ordered: false });
        } else {
          throw error;
        }
      }

      console.log(
        `✅ [MONGO BATCH SAVED] Successfully saved batch of ${locations.length} coordinates across ${byOrder.size} order(s) to MongoDB! (Upserted: ${result.upsertedCount}, Modified: ${result.modifiedCount})\n`
      );

      activity('mongo.batch.saved', {
        batchCount: locations.length,
        ordersCount: byOrder.size,
        upserted: result.upsertedCount,
        modified: result.modifiedCount,
        collection: model.collection.collectionName,
      });

      return result;
    },

    latest(orderId) {
      return model.findById(orderId).lean();
    },
  };
}
