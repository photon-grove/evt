# evt website

The docs and architecture-diagram site for evt, built with Vite and React. It renders every
Markdown file under `docs/` except `docs/internal/`, and `src/docs.ts` decides the order and
grouping.

```sh
moon run website:dev     # local dev server
moon run website:build   # production build into website/dist
```

## The homepage link

The footer links to the Photon Grove homepage, and the URL is resolved at build time from one
value in [`src/siteConfig.ts`](src/siteConfig.ts). Production builds use
`https://photon-grove.com` by default. Development builds use `https://dev.photon-grove.com`,
which comes from `VITE_PHOTON_GROVE_URL` in [`.env.development`](.env.development). Vite loads
that file in development mode (`vite`, or `vite build --mode development`). A production
`vite build` skips it and falls back to the default, so the deployed site needs no extra
configuration.
