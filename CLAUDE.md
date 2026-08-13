# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Lark (Feishu international) bot that turns a CSV of shipments into commercial invoices as Lark Spreadsheets. A user drops a `.csv` into a Lark chat; the bot parses it, generates one styled invoice sheet (tab) per row, archives the raw CSV to MongoDB, and replies with a card of buttons linking to the created spreadsheets.

Comments, logs, and commit messages are in Vietnamese — match that when editing.

## Commands

No build, lint, or test tooling exists (`npm test` is the placeholder stub). Development is manual:

```bash
npm install
npx nodemon index.js     # local server on :3000 (nodemon is a devDependency, no script wired up)
node index.js            # plain run
```

Requires a `.env` (gitignored) with `LARK_APP_ID`, `LARK_APP_SECRET`, `MONGODB_URI`.

Deployment is Vercel ([vercel.json](vercel.json)): all routes funnel to `index.js`, which exports the Express app and only calls `app.listen` when `NODE_ENV !== 'production'`.

Testing a change realistically means exposing `/webhook/event` (e.g. a tunnel) and sending a real CSV through a Lark chat — the Lark Sheets calls have no local mock.

## Architecture

Everything lives in [index.js](index.js); [countryCodes.js](countryCodes.js) and [translations.js](translations.js) are pure lookup helpers.

**Request flow** — `POST /webhook/event`:
1. Answers `url_verification` challenges, then dedupes on `header.event_id` via an in-memory `processedEvents` Set with a 10-minute TTL. This is per-process, so on Vercel it does *not* reliably prevent duplicate processing across cold starts.
2. On `im.message.receive_v1` with a `.csv` file: downloads the file via the IM resources endpoint, strips BOM, parses with PapaParse (`header: true`, `dynamicTyping`).
3. Maps each CSV row to `{ waybillNumber, fields: [{ val, range }] }` — the `range` strings are absolute cell addresses in the invoice template.
4. Chunks rows by `CHUNK_SIZE` (100) and creates one spreadsheet per chunk, **sequentially** (a parallel version was tried and reverted — see git history).
5. Saves raw CSV + parsed rows to MongoDB (`csv_storage` collection) and replies with an interactive card of link buttons.

**Two API layers, deliberately mixed.** `@larksuiteoapi/node-sdk` is used only for auth (`tenantAccessToken.internal`) and chat replies (`im.message.reply`). All Sheets work is raw `fetch` against `open.larksuite.com` Sheets v2/v3 endpoints with a manually passed `Bearer ${tenantToken}`. Don't assume an SDK method exists for sheet operations.

**Template-clone strategy** in `createSpreadsheetForBatch`:
- Write `INVOICE_TEMPLATE` (a 19×6 array literal) into the default sheet, apply merges one-by-one, apply styles in a single `styles_batch_update`, then stamp `public/logo.png` via `values_image`.
- Clone that sheet once per invoice row using `copySheet`, then delete the template sheet at the end.
- Fill data with `values_batch_update`, chunked at 150 ranges per call.

**Cell ranges are hardcoded and coupled.** `INVOICE_TEMPLATE`, the `mergeRanges` list, `stylePayload`, and the per-row `fields[].range` values (F10, B12–B15, B17, C17, E17, F17, E18, F18) all describe the same layout. Adding or removing a template row shifts every one of them — change them together.

**Unit price is tiered by quantity**, via `getUnitPrice` / `UNIT_PRICE_TIERS` (qty 1–3 → $50, 4 → $45, 5 → $35, 6 → $30, 7 → $25, 8–9 → $20). Qty ≥ 10 (or an unparseable qty) returns `null`, which leaves the price and amount cells blank for manual review. This relies on `INVOICE_TEMPLATE` keeping D17/F17/E18/F18 empty — `fields` are filtered with `.filter(f => f.val)`, so any placeholder left in the template would show through instead of a blank.

**Rate-limit defenses are load-bearing, not incidental.** `COPY_BATCH_SIZE = 5`, three retries with 1.5s backoff on a non-zero Lark `code`, 500ms sleep between successful copy batches, and the 150-range fill chunks were all tuned empirically against Lark throttling (the commit log is largely this tuning). Raising them, or reintroducing parallelism, is how sheets silently go missing — the code even counts `missingSheetsCount` for rows whose cloned tab never appeared. Balance this against the Vercel function timeout: sequential batches with sleeps are the reason `CHUNK_SIZE` is what it is.

**Sheet naming.** `sanitizeSheetName(waybillNumber, globalOffset + index)` strips Lark-illegal characters, truncates to 40 chars, and appends `-${index + 2}` (offset by 2 because the template occupies the first position). `globalOffset` accumulates across chunks so numbering stays continuous across multiple spreadsheets.

**CSV column matching is fuzzy.** `extractAttribute(row, keyword)` finds the first header whose lowercase name *contains* the keyword. Column headers can drift without breaking, but a new column containing an existing keyword will shadow it.

**Product naming.** `cleanProductKey` strips a leading `"2 pairs of "` / `"a piece of "` prefix; `translateProductName` does an exact (normalized, case-insensitive) lookup in `exactTranslationMap` and returns `''` on a miss — there is no fuzzy fallback, so an unlisted product renders English-only. `getProductUnit` returns `Piece` if the description mentions "piece", otherwise `Pair`. Quantity is scraped from the leading number of the item description.

Errors inside the handler are caught and posted back into the chat with the full stack trace, and the webhook always returns 200.

## Other agent configs

A Codex config exists at `~/.codex/config.toml`. Reply `/import` to scan and list what's importable (MCP servers, slash commands, subagents, skills, instructions), then `/import --yes=<digest>` to apply the user-level items.
