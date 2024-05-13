import { Config } from '../';
import { Message } from './';
import { v4 } from 'uuid';
import * as dgram from 'node:dgram';

class UdpTransport {
  private client: dgram.Socket;
  private config: Config;
  constructor(config: Config) {    
    this.config = config;
  }

  #writeInt(value: string | number | boolean) {
    const size = Buffer.byteLength(`${value}`);
    return Buffer.alloc(4).write(`${size}`);
  }

  #writeComplexPair(
    key: string,
    value: string | number | boolean,
    buffer: number[],
  ) {
    const newBuffer = [] as number[];
    newBuffer
      .concat(Array.from(Buffer.from(key, 'utf8')))
      .concat(Array.from(Buffer.from('\n', 'utf8')))
      .concat(this.#writeInt(value))
      .concat(Array.from(Buffer.from(`${value}`, 'utf8')));
    return buffer.concat(newBuffer);
  }
  #writeSimplePair(key: string, value: string | number | boolean) {
    return Array.from(Buffer.from(key + '=' + value + '\n', 'utf8'));
  }

  // TODO Сделадь мультистрочные
  #writePair(key: string, value: string | number | boolean): number[] {
    // if (value && typeof value === "string" && value?.indexOf('\n')) {
    //     return this.#writeComplexPair(key, value, buffer)
    // }else {
    // console.log('simple pairs')
    return this.#writeSimplePair(key, value);
    // }
  }

  #toBytes(value: string | number, len: number) {
    const buf = Buffer.alloc(len);
    buf.write(`${value}`);
    return Array.from(buf);
  }

  #makeUUIDMessage() {
    const hash = v4();
    return this.#toBytes(hash, 16);
  }

  #chunk(buf: number[], maxBytes: number) {
    const result = [];
    for (let i = 0; i < buf.length; i += maxBytes) {
      const chunk = buf.slice(i, i + maxBytes);
      result.push(chunk);
    }
    return result;
  }

  #prepareUdpMessages(message: Message): number[][] {
    let messages = [] as number[];
    const byteMessage = [] as number[][];
    for (const key in message) {
      const pair = this.#writePair(key, message[key]);
      messages = messages.concat(pair);
    }
    messages = messages.concat(Array.from(Buffer.from('\n', 'utf8')));
    let logdocBytes = [6, 3];
    const arrayOfChunks = this.#chunk(messages, 2028);
    const uuidMessage = this.#makeUUIDMessage();
    const lengthChunks = arrayOfChunks.length;

    logdocBytes = logdocBytes.concat(lengthChunks);

    arrayOfChunks.forEach((value, index) => {
      let body = logdocBytes.concat(index + 1);
      body = body.concat(uuidMessage);
      byteMessage[index] = body.concat(value);
    });
    return byteMessage;
  }

  send(message: Message) {
    const readyMessages = this.#prepareUdpMessages(message);
    readyMessages.forEach((chunk) => {
      const client = dgram.createSocket('udp4');
      client.send(
        new Uint8Array(chunk),
        this.config.port,
        this.config.host,
        (err) => {
          if (err) throw err;
          client.close();
        },
      );
    });
  }
}
export default UdpTransport;
