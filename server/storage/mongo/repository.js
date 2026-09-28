import { logActivity } from '../../lib/activity.js';

export function createLocationRepository(model, activity = logActivity) {
  return {
    async saveIfNewer(location) {
      const filter = {
        _id: location.orderId,
        $or: [{ timestamp: { $lt: location.timestamp } }, { timestamp: { $exists: false } }],
      };
      const update = { $set: { ...location, persistedAt: new Date() } };
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
    latest(orderId) {
      return model.findById(orderId).lean();
    },
  };
}
