# Eventlog documentation

Use Node.js 24 or newer and npm. From this directory:

```bash
npm ci --ignore-scripts
npm run build
npm run serve
```

The shared Docs System plugin supplies documentation styling. Only `docs/` is site content;
repository designs, planning records and proof receipts remain internal.

The credential-free `Documentation validation` workflow builds on pull requests and main pushes.
Successful main pushes upload `b10x-project-site`, including hidden files and the exact source
declaration. Atlas generates the single site caller; the pinned Website publisher deploys the
artifact to `/eventlog/`. Keep deployment credentials out of this repository.

The passive bundle separately feeds `/docs/eventlog/`. A failed build or publication leaves the
previous successful site available. Roll back through a reviewed source revert and a fresh build.
