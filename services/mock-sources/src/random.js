// PRNG determinista: la misma búsqueda (ruta + fecha) devuelve siempre el mismo inventario,
// así la ingesta es reproducible y los upserts no generan duplicados.

function hashString(str) {
  let h = 1779033703 ^ str.length;
  for (let i = 0; i < str.length; i++) {
    h = Math.imul(h ^ str.charCodeAt(i), 3432918353);
    h = (h << 13) | (h >>> 19);
  }
  return (h >>> 0);
}

export function seededRandom(seed) {
  let a = hashString(String(seed));
  return function mulberry32() {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export const pick = (rnd, list) => list[Math.floor(rnd() * list.length)];
export const between = (rnd, min, max) => min + rnd() * (max - min);
export const intBetween = (rnd, min, max) => Math.floor(between(rnd, min, max + 1));
export const roundTo = (value, step) => Math.round(value / step) * step;
