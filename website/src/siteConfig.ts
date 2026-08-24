// Use the development homepage override when set; blank values fall back to production.
// Vite loads VITE_PHOTON_GROVE_URL from website/.env.development in development.
const override = import.meta.env.VITE_PHOTON_GROVE_URL?.trim()

export const photonGroveUrl = override || 'https://photon-grove.com'
