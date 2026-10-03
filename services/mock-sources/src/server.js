import express from 'express';
import { chaosMiddleware, getChaos, updateChaos, isPoisoned, poisonResponse } from './chaos.js';
import { searchFlights } from './flights.js';
import { searchCars } from './cars.js';

const app = express();
app.use(express.json());

app.get('/health', (_req, res) => res.json({ status: 'ok', service: 'mock-sources' }));

// ---- Panel de caos (solo red interna / demo) -------------------------------
app.get('/__chaos', (_req, res) => res.json(getChaos()));
app.post('/__chaos', (req, res) => {
  const updated = updateChaos(req.body);
  console.log('[chaos] configuración actualizada', JSON.stringify(updated));
  res.json(updated);
});

// ---- Feed de aerolíneas colombianas ----------------------------------------
app.get('/airlines/v1/itineraries', chaosMiddleware('airlines'), (req, res) => {
  const { origin, destination, date } = req.query;
  if (!origin || !destination || !date) {
    return res.status(400).json({ error: 'BAD_REQUEST', message: 'origin, destination y date son obligatorios' });
  }
  if (isPoisoned('routes', `${origin}-${destination}`)) return poisonResponse(res, `${origin}-${destination}`);
  const result = searchFlights({ origin, destination, date });
  if (!result) return res.status(404).json({ error: 'ROUTE_NOT_SERVED' });
  res.json(result);
});

// ---- Proveedor de alquiler de autos ----------------------------------------
app.get('/rentals/v1/availability', chaosMiddleware('rentals'), (req, res) => {
  const { location, pickupDate } = req.query;
  if (!location || !pickupDate) {
    return res.status(400).json({ error: 'BAD_REQUEST', message: 'location y pickupDate son obligatorios' });
  }
  if (isPoisoned('locations', location)) return poisonResponse(res, location);
  const result = searchCars({ location, pickupDate });
  if (!result) return res.status(404).json({ error: 'LOCATION_NOT_SERVED' });
  res.json(result);
});

const port = Number(process.env.PORT ?? 4010);
app.listen(port, () => {
  console.log(`[mock-sources] escuchando en :${port} · caos=${JSON.stringify(getChaos())}`);
});
