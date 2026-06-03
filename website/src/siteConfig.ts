// Photon Grove homepage link, resolved at build time.
//
// Defaults to the production homepage so production builds (and any build with
// no overrides) point at https://photon-grove.com. Dev builds set
// VITE_PHOTON_GROVE_URL via website/.env.development, which Vite loads
// automatically in development mode.
export const photonGroveUrl =
  import.meta.env.VITE_PHOTON_GROVE_URL ?? 'https://photon-grove.com'
