import { Config } from '../';
import { Message } from './';
import * as net from 'net';

const HEADER = Buffer.from([6, 3]);
const MAX_QUEUE = 10_000; // drop oldest beyond this — logging must not grow unbounded
const INITIAL_RETRY_MS = 500;
const MAX_RETRY_MS = 30_000;

/**
 * TCP transport: one persistent connection, lazy connect on first send,
 * a bounded write queue while (re)connecting, reconnect with backoff.
 * Values containing a newline are encoded as complex (binary) pairs
 * per the ld_format spec: KEY '\n' be-uint32(len) VALUE.
 */
class TcpTransport {
  private config: Config;
  private socket: net.Socket | null = null;
  private connecting = false;
  private retryTimer: NodeJS.Timeout | null = null;
  private retryDelay = INITIAL_RETRY_MS;
  private queue: Buffer[] = [];
  private lastError: Error | null = null;

  constructor(config: Config) {
    this.config = config;
  }

  send(message: Message): void {
    if (this.queue.length >= MAX_QUEUE) {
      this.queue.shift();
    }
    this.queue.push(this.encode(message));
    if (this.socket) {
      this.flush();
    } else {
      this.connect();
    }
  }

  private encode(message: Message): Buffer {
    const parts: Buffer[] = [HEADER];
    for (const key in message) {
      const value = `${message[key]}`;
      const safeKey = key.replace(/[=\n]/g, '_');
      if (value.includes('\n')) {
        const valueBuf = Buffer.from(value, 'utf8');
        const len = Buffer.alloc(4);
        len.writeUInt32BE(valueBuf.length);
        parts.push(Buffer.from(`${safeKey}\n`, 'utf8'), len, valueBuf);
      } else {
        parts.push(Buffer.from(`${safeKey}=${value}\n`, 'utf8'));
      }
    }
    parts.push(Buffer.from('\n', 'utf8'));
    return Buffer.concat(parts);
  }

  private flush() {
    while (this.socket && this.queue.length > 0) {
      this.socket.write(new Uint8Array(this.queue.shift() as Buffer));
    }
  }

  private connect() {
    if (this.connecting || this.socket || this.retryTimer) return;
    this.connecting = true;

    const socket = net.createConnection({
      host: this.config.host,
      port: this.config.port,
    });
    socket.setKeepAlive(true, 120_000);

    socket.on('connect', () => {
      this.connecting = false;
      this.retryDelay = INITIAL_RETRY_MS;
      this.socket = socket;
      this.flush();
    });

    // 'close' always follows 'error', so reconnect logic lives in one place.
    socket.on('error', (err) => {
      this.lastError = err;
    });
    socket.on('close', () => {
      if (this.socket === socket) this.socket = null;
      this.connecting = false;
      socket.destroy();
      if (this.queue.length > 0) this.scheduleReconnect();
    });
  }

  private scheduleReconnect() {
    if (this.retryTimer) return;
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      this.connect();
    }, this.retryDelay);
    // Timers must not keep the host process alive.
    this.retryTimer.unref?.();
    this.retryDelay = Math.min(this.retryDelay * 2, MAX_RETRY_MS);
  }
}
export default TcpTransport;
