// Photon Grove homepage link, resolved at build time.
//
// Defaults to the production homepage so production builds (and any build with
// no overrides) point at https://photon-grove.com. Dev builds set
// VITE_PHOTON_GROVE_URL via website/.env.development, which Vite loads
// automatically in development mode.
// Treat an empty or whitespace-only override as unset so a blank env var can't
// produce a broken href="" — fall back to the production homepage instead.
const override = import.meta.env.VITE_PHOTON_GROVE_URL?.trim()

export const photonGroveUrl = override || 'https://photon-grove.com'
