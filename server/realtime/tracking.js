import { Server } from 'socket.io';
import { locationChannel } from '../storage/redis/store.js';
import { validateId } from '../api/validation.js';
import { logActivity } from '../lib/activity.js';

// Each process subscribes only to orders with local customers. No global fan-out or double broadcast.
export function attachTrackingServer(httpServer, { subscriber, authorize, logger = console }) {
  const io = new Server(httpServer, {
    cors: {
      origin: process.env.CLIENT_ORIGIN || '*',
      methods: ['GET', 'POST'],
      credentials: true,
    },
    transports: ['websocket'],
    maxHttpBufferSize: 4096,
  });

  const subscriptions = new Map();
  const queues = new Map();
  const roomFor = (orderId) => `order:${orderId}`;

  function serialize(orderId, task) {
    const previous = queues.get(orderId) || Promise.resolve();
    const result = previous.catch(() => {}).then(task);
    queues.set(orderId, result);
    const clear = () => {
      if (queues.get(orderId) === result) queues.delete(orderId);
    };
    result.then(clear, clear);
    return result;
  }

  async function join(socket, orderId) {
    return serialize(orderId, async () => {
      if (!socket.connected) return;
      if (!subscriber.isReady) throw new Error('Tracking temporarily unavailable');
      let members = subscriptions.get(orderId);
      if (!members) {
        await subscriber.subscribe(locationChannel(orderId), (raw) => {
          try {
            const location = JSON.parse(raw);
            if (location.orderId === orderId) {
              io.local.to(roomFor(orderId)).emit('location:update', location);
              logActivity('websocket.location.sent', {
                orderId,
                riderId: location.riderId,
                timestamp: location.timestamp,
                customers: io.sockets.adapter.rooms.get(roomFor(orderId))?.size || 0,
              });
            }
          } catch (error) {
            logger.warn(`Invalid location broadcast: ${error.message}`);
          }
        });
        members = new Set();
        subscriptions.set(orderId, members);
        logActivity('redis.channel.subscribed', { orderId, channel: locationChannel(orderId) });
      }
      if (!socket.connected) {
        if (members.size === 0) {
          await subscriber.unsubscribe(locationChannel(orderId));
          subscriptions.delete(orderId);
        }
        return;
      }
      members.add(socket.id);
      await socket.join(roomFor(orderId));
      logActivity('websocket.order.subscribed', { orderId, socketId: socket.id });
      // The disconnect cleanup is queued after this task, including disconnects during subscribe.
    });
  }

  function leave(socket, orderId) {
    return serialize(orderId, async () => {
      const members = subscriptions.get(orderId);
      if (!members) return;
      members.delete(socket.id);
      if (members.size === 0) {
        await subscriber.unsubscribe(locationChannel(orderId));
        subscriptions.delete(orderId);
        logActivity('redis.channel.unsubscribed', { orderId });
      }
    });
  }

  io.use((socket, next) => {
    try {
      const orderId = validateId(socket.handshake.auth?.orderId);
      const claims = authorize(socket.handshake.auth?.token, { role: 'customer', orderId });
      socket.data.orderId = orderId;
      socket.data.expiresAt = claims.exp * 1000;
      next();
    } catch (error) {
      next(new Error(error.message));
    }
  });

  io.on('connection', (socket) => {
    const { orderId, expiresAt } = socket.data;
    logActivity('websocket.connected', { orderId, socketId: socket.id });
    const expiry = setTimeout(
      () => socket.disconnect(true),
      Math.min(expiresAt - Date.now(), 2147483647),
    );
    socket.on('tracking:subscribe', async (ack) => {
      if (typeof ack !== 'function') return;
      try {
        await join(socket, orderId);
        if (socket.connected) ack({ ok: true });
      } catch (error) {
        logger.error(`Tracking subscribe: ${error.message}`);
        ack({ ok: false, error: 'Tracking temporarily unavailable' });
      }
    });
    socket.on('disconnect', (reason) => {
      logActivity('websocket.disconnected', { orderId, socketId: socket.id, reason });
      clearTimeout(expiry);
      leave(socket, orderId).catch((error) => logger.error(`Tracking cleanup: ${error.message}`));
    });
  });

  // Pub/sub cannot replay gaps: close the transport to trigger automatic connect → subscribe → fetch.
  const reconnectCustomers = () => {
    logActivity('redis.subscriber.reconnecting', { customers: io.sockets.sockets.size }, 'warn');
    for (const socket of io.sockets.sockets.values()) socket.conn.close();
  };
  subscriber.on('reconnecting', reconnectCustomers);
  const cleanUnused = () => {
    for (const [orderId, members] of subscriptions) {
      if (members.size === 0)
        leave({ id: '' }, orderId).catch((error) => logger.error(error.message));
    }
  };
  subscriber.on('ready', cleanUnused);

  return {
    io,
    async close() {
      subscriber.off('reconnecting', reconnectCustomers);
      subscriber.off('ready', cleanUnused);
      await new Promise((resolve) => io.close(resolve));
      await Promise.allSettled([...queues.values()]);
    },
  };
}
