// Inyección de fallos controlada ("modo caos").
// Se configura por variables de entorno y se puede cambiar en caliente con POST /__chaos
// durante la demo, para forzar los reintentos de Prefect sin reiniciar nada.

const env = process.env;

const state = {
  enabled: env.CHAOS_ENABLED !== 'false',
  failureRate: Number(env.CHAOS_FAILURE_RATE ?? 0.25),
  modes: (env.CHAOS_MODES ?? '503,429,timeout,malformed').split(',').map((m) => m.trim()),
  timeoutDelayMs: Number(env.CHAOS_TIMEOUT_DELAY_MS ?? 20000),
  latencyMs: [Number(env.MOCK_LATENCY_MIN_MS ?? 120), Number(env.MOCK_LATENCY_MAX_MS ?? 600)],
  // Recursos que SIEMPRE fallan: sirven para demostrar qué pasa cuando se agotan los reintentos.
  poison: {
    routes: (env.CHAOS_POISON_ROUTES ?? 'BOG-LET').split(',').filter(Boolean),
    locations: (env.CHAOS_POISON_LOCATIONS ?? 'LET').split(',').filter(Boolean),
  },
};

const stats = { requests: 0, injected: { 503: 0, 429: 0, timeout: 0, malformed: 0, poison: 0 } };

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export function getChaos() {
  return { ...state, stats };
}

export function updateChaos(patch = {}) {
  for (const key of ['enabled', 'failureRate', 'modes', 'timeoutDelayMs', 'latencyMs']) {
    if (patch[key] !== undefined) state[key] = patch[key];
  }
  if (patch.poison) state.poison = { ...state.poison, ...patch.poison };
  return getChaos();
}

export function isPoisoned(kind, key) {
  return state.enabled && state.poison[kind]?.includes(key);
}

export function poisonResponse(res, key) {
  stats.injected.poison++;
  console.log(`[chaos] ☠ recurso envenenado ${key} → 500 permanente`);
  return res.status(500).json({ error: 'UPSTREAM_PROVIDER_DOWN', message: `El proveedor no responde para ${key}` });
}

// Middleware: latencia realista + fallo aleatorio según la tasa configurada.
export function chaosMiddleware(scope) {
  return async (req, res, next) => {
    stats.requests++;
    const [min, max] = state.latencyMs;
    await sleep(min + Math.random() * (max - min));

    if (!state.enabled || Math.random() >= state.failureRate) return next();

    const mode = state.modes[Math.floor(Math.random() * state.modes.length)];
    stats.injected[mode] = (stats.injected[mode] ?? 0) + 1;
    console.log(`[chaos] ⚡ ${scope} ${req.originalUrl} → fallo inyectado: ${mode}`);

    switch (mode) {
      case '503':
        return res.status(503).json({ error: 'SERVICE_UNAVAILABLE', message: 'Proveedor temporalmente no disponible' });
      case '429':
        res.set('Retry-After', '2');
        return res.status(429).json({ error: 'RATE_LIMITED', message: 'Demasiadas solicitudes' });
      case 'timeout':
        // El cliente corta antes (timeout de 8 s); respondemos tarde para simular una red colgada.
        await sleep(state.timeoutDelayMs);
        return next();
      case 'malformed':
        // JSON truncado: simula una respuesta corrupta / página cambiada.
        res.set('Content-Type', 'application/json');
        return res.status(200).send('{"itineraries": [{"itineraryId": "x", "fare": {"amou');
      default:
        return next();
    }
  };
}
