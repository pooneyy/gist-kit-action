# Gist Kit Action

[English](README.md) | 中文

一个多功能的 GitHub Action，用于在 workflow 中管理
[GitHub Gists](https://gist.github.com)。支持创建、更新、下载、删除单个文件以及删除整个 Gist。

## 功能特性

- **创建** Gist：基于一个或多个本地文件。
- **更新** 已存在的 Gist：通过 upsert 文件的方式。
- **下载**
  Gist 中的文件（默认匿名请求；token 是可选的，仅用于元数据请求的鉴权）。
- **删除** Gist 中的单个文件。
- **删除** 整个 Gist。
- 如果未提供 token，则回退使用 `$GITHUB_TOKEN` / `$GH_TOKEN`。
- 文件名使用与
  [`actions/upload-artifact`](https://github.com/actions/upload-artifact) 相同的
  `path` 语义，换行分隔列表，支持 glob。

<!-- prettier-ignore-start -->

## Inputs（输入参数）

| 名称 | 是否必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `token` | 否 | _(空)_ | 拥有 gist 写权限的 token。若为空，则使用 `$GITHUB_TOKEN` 或 `$GH_TOKEN`。所有写操作均必填；`download` 可选（仅用于获取元数据，下载始终为匿名请求，个人访问令牌无需任何权限）。 |
| `gist-id` | 否 | _(空)_ | Gist 标识符。`update`、`download`、`delete-files`、`delete-gist` 操作必填。`create` 操作忽略此参数。 |
| `operation` | 否 | `download` | 之一：`create`、`update`、`download`、`delete-files`、`delete-gist`。传入未知值将导致 action 失败。 |
| `create-public-gist` | 否 | `false` | 新创建的 Gist 是否公开。仅当字符串完全等于 `true`（区分大小写）时视为 `true`。非 `create` 操作忽略此参数。 |
| `filenames` | 否 | _(空)_ | 换行分隔的文件名列表。`create`、`update`、`delete-files` 操作必填；`download` 操作可选；`delete-gist` 操作忽略此参数。 |

## Outputs（输出参数）

| 名称 | 说明 |
| --- | --- |
| `gist-id` | 生成的 Gist id。`create` 操作为新分配的 id；其他所有操作会原样回显输入的 `gist-id`。 |

<!-- prettier-ignore-end -->

## 使用方法

### 创建 Gist

```yaml
- name: 创建 Gist
  id: create
  uses: pooneyy/gist-kit-action@v1
  with:
    token: ${{ secrets.GIST_WRITE_PERMISSION_TOKEN }}
    operation: create
    filenames: |
      build/output.txt
      dist/bundle.js
      /path/to/config/*.json
    create-public-gist: 'true'
# gist-id 现在可以通过 ${{ steps.create.outputs.gist-id }} 获取
```

### 更新已存在的 Gist

```yaml
- name: 更新 Gist
  uses: pooneyy/gist-kit-action@v1
  with:
    token: ${{ secrets.GIST_WRITE_PERMISSION_TOKEN }}
    operation: update
    gist-id: ${{ steps.create.outputs.gist-id }}
    filenames: |
      build/output.txt
      /path/to/config/*.json
```

### 下载文件

```yaml
- name: 下载 Gist 文件
  uses: pooneyy/gist-kit-action@v1
  with:
    operation: operation: download
    gist-id: abc123def
    # `filenames` 为空时会下载所有文件；否则，每条记录会先去除路径部分，
    # 然后与 Gist 文件名匹配；附带 /path/to 的条目会下载到指定位置。
    filenames: |
      /path/to/download/readme.md
      /path/to/config/*.json
```

### 删除文件

```yaml
- name: 从 Gist 删除文件
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

### 删除 Gist

```yaml
- name: 删除 Gist
  uses: pooneyy/gist-kit-action@v1
  with:
    token: ${{ secrets.GIST_WRITE_PERMISSION_TOKEN }}
    operation: delete-gist
    gist-id: ${{ steps.create.outputs.gist-id }}
```

## 行为细节

- **`create-public-gist` 区分大小写。** 仅当字符串完全等于 `true` 时视为
  `true`。任何其他值（包括 `True`、`1`、`yes` 或空）都视为
  `false`（私有 Gist）。
- **`download` 操作的 `filenames` 匹配**
  会先去除每条记录的前导目录部分，然后再与 Gist 文件名匹配。原始记录会用作磁盘上的目标路径，因此
  `/tmp/hello.txt` 在匹配到 Gist 文件 `hello.txt` 后会写入到 `/tmp/hello.txt`。
- **`download` 鉴权。** 元数据调用（`GET /gists/{id}`）在提供 `token`
  时会使用该 token，否则回退为匿名请求。

## 开发

安装依赖后运行测试套件：

```bash
npm install
npm test
```

使用以下命令将 TypeScript 源码打包为 `dist/index.js` (GitHub
Actions实际执行的文件):

```bash
npm run bundle
```

## 许可证

[MIT](LICENSE)
