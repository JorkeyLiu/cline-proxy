# AGENTS.md

本文件是本仓库长期有效的认知与行为契约。优先级高于任务中的临时推断；与源码权威冲突时以源码权威为准，并按最小自然边界修正行为。

## 系统定位

Go 实现的统一反向代理。对外同时兼容 OpenAI Chat、OpenAI Responses、Anthropic Messages；对内按 model 在 Cline 多账号池与 Opencode Zen 免费模型网关之间分流；附带内联 Web 管理台。

运行时角色划分：协议入口只做边界与路由，不拥有上游状态；Cline 账号池拥有账号选择、冷却、恢复语义；Zen 网关拥有免费模型同步、压缩、代理池出口语义；管理台只是状态投影与操作入口，不拥有业务权威。

## 请求脊柱

模型推理 API 的主脊柱，按序解释行为归因：

入口日志 → API key 边界 → 协议转换（含 `override.md` 系统提示词覆盖） → 模型路由 → 上游执行 → 状态统计与日志投影。

现实兼容边界（改动前必须先识别，若要统一需显式评估/决定）：Chat 与 Anthropic 当前可读取工作目录 `override.md` 覆盖 system prompt；Responses 当前不应用该覆盖。`/admin/*` 与 health 是边界例外，不经过模型 API 的完整脊柱；管理台当前未挂 API key 鉴权，任何鉴权变化仍是用户产品决策。

调试时沿脊柱定位根因：鉴权问题止于 API key 边界；Chat / Anthropic 提示词异常先查工作目录 `override.md` 是否存在，Responses 不走该覆盖，需按其协议转换链排查；模型走向错误属于路由层；上下游字段差异属于协议转换层；计数、冷却、压缩状态不一致属于瞬态状态或投影层，不得反向修改权威配置来“修复投影”。

## 权威与投影

权威只存在于以下位置：

- 账号 / Key 持久化：经 `kit.ResolveDataPath` 定位的 `.cline-accounts.json`。
- Zen 配置持久化：经 `kit.ResolveDataPath` 定位的 `.zen-config.json`。
- 系统提示词覆盖开关：工作目录直接读取的 `override.md`，不存在即使用客户端自带，不做额外查找或回退决策。
- 模块依赖：`go.mod` / `go.sum`。
- 构建 / 部署 / 发布：`Dockerfile`、`docker-compose.yml`、`.github/workflows/build.yml`。发布版本号递增逻辑由该 workflow 管理。

以下只是投影或瞬态状态，不得当作权威配置读取或提交：

- 请求日志、统计聚合、运行日志。
- `AccessToken` / `ExpiresAt`、代理运行时配置、熔断 / 冷却标记、压缩缓存。

新增运行时持久化文件必须走 `internal/kit` 的 `ResolveDataPath` 约定，不得自行拼写数据目录路径。

## 变更传播边界

`internal/kit` 是底层约定，改动即全局传播，保持最小化并显式评估兼容范围。

协议入口的三种 API 必须同步保持；当前 `/v1` 与无前缀兼容入口都属于既有 API，修改路由或协议行为时必须同步维护，不得无意删除或使其分叉。

管理台的 `opencode` / `zen` 别名必须同步；管理台 HTML 是 Go 内联字符串，没有独立前端构建，不得按前后端分离方式新增构建步骤或产物。

`README.md` 是产品使用说明，不得视作精确源码地图，其结构段落若与源码不一致，以源码为准。

## 安全红线

- 代理和新增实现不得将凭据泄露到新增日志、调试输出、普通响应、对话或版本控制：涉及账号凭据、`refresh` / `access token`、API key、代理密码、数据目录内容。
- 现有 `RefreshToken` 是经 `kit.ResolveDataPath` 定位的 `.cline-accounts.json` 权威持久化的一部分，而 `AccessToken` / `ExpiresAt` 是瞬态且不落盘；不得因安全整改误删既有恢复/导出产品语义，任何改变要有明确任务范围与兼容评估。
- 对外展示代理 URL 时必须脱敏。
- 不得将日志 / 统计投影当作权威配置回写。
- 不得削弱现有 API key 行为；管理台鉴权语义属于产品决策，任何改变需用户明确决定。若确认管理后台当前无鉴权，仅作为风险边界报告，不得擅自补鉴权或改变可访问性。
- 敏感状态文件受 `.gitignore` 保护，不得绕过、削弱或提交其中列出的文件。

## 验证门禁

当前仓库无测试文件，无 lint / format / typecheck 门禁。以下为代码任务的最低完成标准，针对最终工作树执行并保留原始退出码：

- `gofmt` 对改动 Go 文件无差异。
- `go vet ./...` 通过。
- `CGO_ENABLED=0 go build -ldflags="-s -w" .` 通过。
- 新增或修改可测试行为时，补聚焦测试并运行 `go test ./...`。

协议转换、流式响应、管理台交互等行为，仅靠静态检查或编译通过不足以证明；需要实际请求或运行时 UI 观察作为证据。

宿主可能没有 Go，可使用与仓库 CI / Docker 对齐的 `golang:1.26-alpine` 容器执行验证。已存在的工具链漂移：`go.mod` 为 `1.25.0`，CI / Docker 使用 `1.26`；不得自行统一版本，涉及兼容范围的改动需显式判断。

## Git 与发布

未经用户明确要求，不得 commit、amend、push、force-push、创建 PR、打 tag、发布；不得丢弃用户已有改动。发布逻辑由 workflow 拥有，代理实现侧不得另立版本规则。

## 范围与风格

- 优先根因与最小自然边界；保留已有 API、路由、JSON 兼容性，除非任务明确要求破坏性改动。
- Go 代码遵循 `gofmt`；错误必须携带上下文；不得吞掉影响行为的错误；并发共享状态保持已有互斥纪律。
- 日志不得记录完整请求内容或敏感 Header；新增高容量日志必须有明确边界。

## 稳定来源

- [README.md](README.md)：产品使用说明，不视作源码地图。
- [go.mod](go.mod)：模块依赖权威。
- [Dockerfile](Dockerfile)：构建权威。
- [docker-compose.yml](docker-compose.yml)：部署权威。
- [.github/workflows/build.yml](.github/workflows/build.yml)：构建与发布权威。
- [.gitignore](.gitignore)：敏感状态保护边界。
- [internal/kit/data.go](internal/kit/data.go)：数据路径解析权威。
