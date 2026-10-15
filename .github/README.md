# Gist Kit Action

English | [中文](README_zh.md)

A multi-functional GitHub Action for managing
[GitHub Gists](https://gist.github.com) from a workflow. Supports creating,
updating, downloading, deleting individual files, and deleting an entire Gist.

## Features

- **Create** a Gist from one or more local files.
- **Update** an existing Gist by upserting files.
- **Download** files from a Gist.
  - Supports anonymous requests;
  - You can optionally pass a token, which you can use for authentication of
    metadata requests when you encounter REST API rate limits. This token
    doesn't require any permissions, but it must be a
    [personal access token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token),
    [apply for one here](https://github.com/settings/tokens). You cannot use the
    built-in
    [`${{ github.token }}`](https://docs.github.com/en/actions/tutorials/authenticate-with-github_token)
    from actions, because the token doesn't have permission to access Gist.
- **Delete** individual files from a Gist.
- **Delete** an entire Gist.
- If no token is explicitly provided, it attempts to obtain the token from the environment variables `$GITHUB_TOKEN` / `$GH_TOKEN`.
- Filenames use the same `path` semantics as
  [`actions/upload-artifact`](https://github.com/actions/upload-artifact),
  newline-separated list, glob support.

<!-- prettier-ignore-start -->

## Inputs

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `token` | No | _(empty)_ | Token with gist write permission. If empty, `$GITHUB_TOKEN` or `$GH_TOKEN` is used. Required for all write operations; optional for `download` (Only used for metadata retrieval, downloads are always anonymous. The personal access token does not require any permissions.). |
| `gist-id` | No | _(empty)_ | Gist identifier. Required for `update`, `download`, `delete-files`, `delete-gist`. Ignored for `create`. |
| `operation` | No | `download` | One of `create`, `update`, `download`, `delete-files`, `delete-gist`. Unknown values fail the action. |
| `create-public-gist` | No | `false` | Whether a newly-created Gist should be public. Only the exact string `true` (case-sensitive) is treated as `true`. Ignored for non-create operations. |
| `filenames` | No | _(empty)_ | Newline-separated list of file names. Required for `create`, `update`, `delete-files`; optional for `download`; ignored for `delete-gist`. |

## Outputs

| Name | Description |
| --- | --- |
| `gist-id` | The resulting Gist id. For `create`, this is the newly-assigned id; for all other operations, it echoes the input `gist-id`. |

<!-- prettier-ignore-end -->

## Usage

### Create a Gist

```yaml
- name: Create Gist
  id: create
  uses: pooneyy/gist-kit-action@v1
  with:
    token: ${{ secrets.GIST_WRITE_PERMISSION_TOKEN }}
    operation: create
    filenames: |
      build/output.txt
      dist/bundle.js
      /path/to/config/*.json
    create-public-gist: true
# gist-id is now available as ${{ steps.create.outputs.gist-id }}
```

### Update an existing Gist

```yaml
- name: Update Gist
  uses: pooneyy/gist-kit-action@v1
  with:
    token: ${{ secrets.GIST_WRITE_PERMISSION_TOKEN }}
    operation: update
    gist-id: ${{ steps.create.outputs.gist-id }}
    filenames: |
      build/output.txt
      /path/to/config/*.json
```

### Download files

```yaml
- name: Download Gist files
  uses: pooneyy/gist-kit-action@v1
  with:
    operation: download
    gist-id: abc123def
    # Empty `filenames` will download all files; otherwise, each
    # entry will match a Gist filename (after removing any leading
    # directory part) and download to the specified location when
    # accompanied by /path/to.
    filenames: |
      /path/to/download/readme.md
      /path/to/config/*.json
```

### Delete files

```yaml
- name: Delete files from Gist
  uses: pooneyy/gist-kit-action@v1
  with:
    token: ${{ secrets.GIST_WRITE_PERMISSION_TOKEN }}
    operation: delete-files
    gist-id: ${{ steps.create.outputs.gist-id }}
    filenames: |
      build/output.txt
      dist/bundle.js
      *.json
```

### Delete a Gist

```yaml
- name: Delete Gist
  uses: pooneyy/gist-kit-action@v1
  with:
    token: ${{ secrets.GIST_WRITE_PERMISSION_TOKEN }}
    operation: delete-gist
    gist-id: ${{ steps.create.outputs.gist-id }}
```

## Behavior details

- **`create-public-gist` is case-sensitive.** Only the exact string `true` is
  treated as `true`. Any other value (including `True`, `1`, `yes`, or empty)
  maps to `false` (private Gist).
- **`filenames` matching for `download`** strips the leading directory
  components from each entry before matching against Gist file names. The
  original entry is used as the destination path on disk, so `/tmp/hello.txt`
  writes to `/tmp/hello.txt` after matching the Gist file `hello.txt`.
- **`download` authentication.** The metadata call (`GET /gists/{id}`) uses the
  supplied `token` when available and falls back to an anonymous request
  otherwise.

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
