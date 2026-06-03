# evt website

Documentation and architecture-diagram site for `evt`, built with Vite + React.

```sh
moon run website:dev     # local dev server
moon run website:build   # production build into website/dist
```

## Environment-aware homepage link

The footer credits Photon Grove and links to its homepage. The URL is resolved
at build time from a single config value in [`src/siteConfig.ts`](src/siteConfig.ts):

- **Production** (default): `https://photon-grove.com`
- **Dev**: `https://dev.photon-grove.com`

The dev URL comes from `VITE_PHOTON_GROVE_URL` in [`.env.development`](.env.development),
which Vite loads automatically in development mode (`vite` / `vite build --mode
development`). Production builds (`vite build`) omit it and fall back to the
production default, so no extra configuration is needed for the deployed site.
