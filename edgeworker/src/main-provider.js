// Bundle entry: responseProvider variant (adds body: policies; forwards to origin).
import { httpRequest } from 'http-request';
import { createResponse } from 'create-response';
import { crypto } from 'crypto';
import { logger } from 'log';
import cfg from './policies.js';
import { createHandlers } from './handlers.js';

const h = createHandlers(cfg, { httpRequest, createResponse, subtle: crypto.subtle, logger, now: Date.now });

export const responseProvider = h.responseProvider;
