// Bundle entry: onClientRequest variant (header / ip / jwt / query policies).
import { httpRequest } from 'http-request';
import { createResponse } from 'create-response';
import { crypto } from 'crypto';
import { logger } from 'log';
import cfg from './policies.js';
import { createHandlers } from './handlers.js';

const h = createHandlers(cfg, { httpRequest, createResponse, subtle: crypto.subtle, logger, now: Date.now });

export const onClientRequest = h.onClientRequest;
export const onClientResponse = h.onClientResponse;
