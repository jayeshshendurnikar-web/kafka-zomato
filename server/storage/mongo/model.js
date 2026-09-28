import mongoose from 'mongoose';

const locationSchema = new mongoose.Schema(
  {
    _id: { type: String, required: true }, // orderId is the unique document identity.
    orderId: { type: String, required: true },
    riderId: { type: String, required: true, index: true },
    latitude: { type: Number, required: true, min: -90, max: 90 },
    longitude: { type: Number, required: true, min: -180, max: 180 },
    timestamp: { type: Number, required: true },
    persistedAt: { type: Date, required: true },
  },
  { versionKey: false, bufferCommands: false },
);

export function getLocationModel(connection = mongoose.connection, collection = 'rider_locations') {
  const name = `RiderLocation_${collection}`;
  return connection.models[name] || connection.model(name, locationSchema, collection);
}
