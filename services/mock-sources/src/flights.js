import { seededRandom, pick, between, intBetween, roundTo } from './random.js';

export const AIRPORTS = {
  BOG: { city: 'Bogotá', lat: 4.7016, lng: -74.1469 },
  MDE: { city: 'Medellín', lat: 6.1645, lng: -75.4231 },
  CTG: { city: 'Cartagena', lat: 10.4424, lng: -75.513 },
  ADZ: { city: 'San Andrés', lat: 12.5836, lng: -81.7112 },
  SMR: { city: 'Santa Marta', lat: 11.1196, lng: -74.2306 },
  CLO: { city: 'Cali', lat: 3.5432, lng: -76.3816 },
  BAQ: { city: 'Barranquilla', lat: 10.8896, lng: -74.7808 },
  PEI: { city: 'Pereira', lat: 4.8127, lng: -75.7395 },
  LET: { city: 'Leticia', lat: -4.1935, lng: -69.9432 },
};

const AIRLINES = [
  { code: 'AV', name: 'Avianca', priceFactor: 1.15, brands: ['XS', 'S', 'M'] },
  { code: 'LA', name: 'LATAM Airlines Colombia', priceFactor: 1.05, brands: ['Basic', 'Light', 'Plus'] },
  { code: 'P5', name: 'Wingo', priceFactor: 0.78, brands: ['Básica', 'Clásica'] },
  { code: '9R', name: 'SATENA', priceFactor: 0.95, brands: ['Promo', 'Flexi'] },
  { code: 'VE', name: 'Clic', priceFactor: 0.9, brands: ['Light', 'Full'] },
];

function distanceKm(a, b) {
  const R = 6371;
  const toRad = (d) => (d * Math.PI) / 180;
  const dLat = toRad(b.lat - a.lat);
  const dLng = toRad(b.lng - a.lng);
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(toRad(a.lat)) * Math.cos(toRad(b.lat)) * Math.sin(dLng / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(h));
}

// Hora local de Colombia (UTC-5, sin horario de verano) en ISO-8601 con offset.
function colombiaIso(date, minutesFromMidnight) {
  const hh = String(Math.floor(minutesFromMidnight / 60) % 24).padStart(2, '0');
  const mm = String(minutesFromMidnight % 60).padStart(2, '0');
  const base = new Date(`${date}T${hh}:${mm}:00-05:00`);
  if (minutesFromMidnight >= 24 * 60) base.setUTCDate(base.getUTCDate() + 1);
  return base.toISOString().replace('.000Z', 'Z');
}

export function searchFlights({ origin, destination, date }) {
  const from = AIRPORTS[origin];
  const to = AIRPORTS[destination];
  if (!from || !to || origin === destination) return null;

  const rnd = seededRandom(`flights:${origin}:${destination}:${date}`);
  const km = distanceKm(from, to);
  const baseDuration = Math.round(35 + km / 11.5);
  const daysAhead = Math.max(0, Math.round((new Date(date) - new Date()) / 86400000));
  const advanceFactor = daysAhead < 7 ? 1.45 : daysAhead < 21 ? 1.15 : 1.0;
  const weekday = new Date(`${date}T12:00:00-05:00`).getUTCDay();
  const weekendFactor = weekday === 5 || weekday === 0 ? 1.18 : 1.0;

  const count = intBetween(rnd, 4, 9);
  const itineraries = [];
  for (let i = 0; i < count; i++) {
    const airline = pick(rnd, AIRLINES);
    const depMinutes = roundTo(intBetween(rnd, 5 * 60, 21 * 60), 5);
    const stops = km > 700 && rnd() < 0.35 ? 1 : 0;
    const duration = baseDuration + (stops ? intBetween(rnd, 60, 140) : intBetween(rnd, -5, 15));
    const base = 150000 + km * 420;
    const amount = roundTo(base * airline.priceFactor * advanceFactor * weekendFactor * between(rnd, 0.82, 1.3), 100);
    const flightNumber = `${airline.code}${intBetween(rnd, 100, 9899)}`;

    itineraries.push({
      itineraryId: `${flightNumber}-${origin}${destination}-${date}`,
      carrier: { code: airline.code, name: airline.name },
      flightNumber,
      departure: { airport: origin, city: from.city, time: colombiaIso(date, depMinutes) },
      arrival: { airport: destination, city: to.city, time: colombiaIso(date, depMinutes + duration) },
      durationMinutes: duration,
      stops,
      fare: { amount, currency: 'COP', cabin: 'ECONOMY', brand: pick(rnd, airline.brands) },
      seatsLeft: intBetween(rnd, 0, 38),
    });
  }

  itineraries.sort((a, b) => a.departure.time.localeCompare(b.departure.time));
  return { search: { origin, destination, date }, currency: 'COP', distanceKm: Math.round(km), itineraries };
}
