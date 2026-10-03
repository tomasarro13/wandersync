import { seededRandom, pick, between, intBetween, roundTo } from './random.js';
import { AIRPORTS } from './flights.js';

const SUPPLIERS = [
  { name: 'Localiza', factor: 0.95 },
  { name: 'Hertz', factor: 1.12 },
  { name: 'Avis', factor: 1.08 },
  { name: 'Budget', factor: 0.92 },
  { name: 'Europcar', factor: 1.0 },
];

const FLEET = [
  { category: 'Económico', models: ['Kia Picanto', 'Chevrolet Spark GT', 'Renault Kwid'], seats: 4, bags: 1, base: 95000 },
  { category: 'Compacto', models: ['Renault Logan', 'Mazda 2', 'Chevrolet Onix'], seats: 5, bags: 2, base: 125000 },
  { category: 'Sedán', models: ['Mazda 3', 'Toyota Corolla', 'Kia K3'], seats: 5, bags: 3, base: 165000 },
  { category: 'SUV', models: ['Renault Duster', 'Mazda CX-30', 'Toyota Fortuner'], seats: 5, bags: 4, base: 230000 },
  { category: 'Van', models: ['Hyundai H1', 'Kia Carnival'], seats: 8, bags: 5, base: 320000 },
];

// San Andrés tiene inventario pequeño y caro (isla); el resto, normal.
const LOCATION_FACTOR = { ADZ: 1.35, CTG: 1.15, SMR: 1.1 };

export function searchCars({ location, pickupDate }) {
  const airport = AIRPORTS[location];
  if (!airport) return null;

  const rnd = seededRandom(`cars:${location}:${pickupDate}`);
  const vehicles = [];
  for (const supplier of SUPPLIERS) {
    const offered = FLEET.filter(() => rnd() < 0.7);
    for (const group of offered) {
      const model = pick(rnd, group.models);
      const amount = roundTo(group.base * supplier.factor * (LOCATION_FACTOR[location] ?? 1) * between(rnd, 0.88, 1.2), 500);
      vehicles.push({
        vehicleId: `${supplier.name.toUpperCase()}-${location}-${model.replace(/\s+/g, '').toUpperCase()}-${pickupDate}`,
        supplier: supplier.name,
        model,
        category: group.category,
        transmission: rnd() < 0.55 ? 'Automática' : 'Mecánica',
        seats: group.seats,
        bags: group.bags,
        ac: rnd() < 0.95,
        dailyRate: { amount, currency: 'COP' },
        available: intBetween(rnd, 0, 6),
      });
    }
  }
  return { location, city: airport.city, pickupDate, vehicles };
}
