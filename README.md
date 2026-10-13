# Gist Kit Action

A multi-functional GitHub Action for managing
[GitHub Gists](https://gist.github.com) from a workflow. Supports creating,
updating, downloading, deleting individual files, and deleting an entire Gist —
all from a single action.

## Features

- **Create** a Gist from one or more local files.
- **Update** an existing Gist by upserting files.
- **Download** files from a Gist (anonymously, no token required).
- **Delete** individual files from a Gist.
- **Delete** an entire Gist.
- Token falls back to `$GITHUB_TOKEN` / `$GH_TOKEN` if not provided.
- Filenames use the same `path` semantics as
  [`actions/upload-artifact`](https://github.com/actions/upload-artifact)
  (newline-separated list, glob support).

## Inputs

| Name | Required | Default | Description |
|---|---|---|---|
| `token` | No | _(empty)_ | Token with gist write permission. If empty, `$GITHUB_TOKEN` or `$GH_TOKEN` is used. Ignored for the `download` operation. |
| `gist-id` | No | _(empty)_ | Gist identifier. Required for `update`, `download`, `delete-files`, `delete-gist`. Ignored for `create`. |
| `operation` | No | `download` | One of `create`, `update`, `download`, `delete-files`, `delete-gist`. Unknown values fail the action. |
| `create-public-gist` | No | `false` | Whether a newly-created Gist should be public. Only the exact string `true` (case-sensitive) is treated as `true`. Ignored for non-create operations. |
| `filenames` | No | _(empty)_ | Newline-separated list of file names. Required for `create`, `update`, `delete-files`; optional for `download`; ignored for `delete-gist`. Supports glob patterns for `create`/`update`. |
| `log-level` | No | `info` | One of `debug`, `info`, `warn`, `error`. Controls which messages `@actions/core` logs. |

## Outputs

| Name | Description |
|---|---|
| `gist-id` | The resulting Gist id. For `create`, this is the newly-assigned id; for all other operations, it echoes the input `gist-id`. |

## Usage

### Create a Gist

```yaml
- name: Create Gist
  id: create
  uses: pooneyy/gist-kit-action@v1
  with:
    operation: create
    filenames: |
      build/output.txt
      dist/bundle.js
    create-public-gist: 'true'
# gist-id is now available as ${{ steps.create.outputs.gist-id }}
```

### Update an existing Gist

```yaml
- name: Update Gist
  uses: pooneyy/gist-kit-action@v1
  with:
    operation: update
    gist-id: ${{ steps.create.outputs.gist-id }}
    filenames: |
      build/output.txt
```

### Download files

```yaml
- name: Download Gist files
  uses: pooneyy/gist-kit-action@v1
  with:
    operation: download
    gist-id: abc123def
    # Empty `filenames` → download every file.
    # Otherwise each entry is matched against Gist file names (after
    # stripping any leading directory components).
    filenames: |
      ./out/readme.md
```

### Delete files

```yaml
- name: Delete files from Gist
  uses: pooneyy/gist-kit-action@v1
  with:
    operation: delete-files
    gist-id: ${{ steps.create.outputs.gist-id }}
    filenames: |
      build/output.txt
      dist/bundle.js
```

### Delete a Gist

```yaml
- name: Delete Gist
  uses: pooneyy/gist-kit-action@v1
  with:
    operation: delete-gist
    gist-id: ${{ steps.create.outputs.gist-id }}
```

## Behavior details

- **`create-public-gist` is case-sensitive.** Only the exact string `true` is
  treated as `true`. Any other value (including `True`, `1`, `yes`, or empty)
  maps to `false` (private Gist).
- **`filenames` matching for `download`** strips the leading directory
  components from each entry before matching against Gist file names. The
  original entry is used as the destination path on disk, so `/tmp/2.txt` writes
  to `/tmp/2.txt` after matching the Gist file `2.txt`.
- **Retries** are applied only to the second leg of `download` (the `raw_url`
  fetch), up to 3 attempts with a 1 s delay between attempts. Other API calls
  fail fast on the first non-2xx response.
- **Glob expansion** is performed for `create` and `update` only, using
  [`@actions/glob`](https://github.com/actions/toolkit/tree/main/packages/glob).
  For `delete-files` the input is treated as a literal list of Gist file names;
  for `download` it is a list of desired destination paths.

## Development

Install dependencies, then run the test suite:

```bash
npm install
npm test
```

Bundle the TypeScript sources into `dist/index.js` (the file GitHub Actions
actually executes) with:

```bash
npm run bundle
```

## License

[MIT](LICENSE)
