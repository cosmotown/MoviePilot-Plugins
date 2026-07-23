# P115StrgmSub - 115网盘订阅追更插件

当前版本：**1.9.8**

MoviePilot v2 插件。根据 MoviePilot 订阅、媒体库正式缺失状态和本地 STRM 实际文件，从 115 分享与 AYCLUB 结构化结果中选择目标资源。115 API 与选择性转存统一交给独立的 p115-openclaw 服务执行；插件不读取 115 Cookie、不安装 `p115client`，也不接管 MoviePilot 后续整理和订阅完成状态。

## 工作流

```text
MoviePilot 订阅
  → P115StrgmSub 检查官方缺失状态
  → 本地 STRM 实时对账
  → AYCLUB / PanSou 等搜索源
  ├─ 115 分享：插件选择文件，p115-openclaw 选择性转存到 /mp整理 对应分类
  └─ ED2K：按 contract v2 原样提交 OpenClaw/p115 后端
  → MoviePilot 继续负责后续整理和订阅刷新
  → STRM / 媒体库扫描
  → MoviePilot 正式缺失检查确认入库
```

## 1.9.8 修复

- **修复最后一轮误读缓存**：MoviePilot 自动服务改用无参数专用入口，在入口内显式标记 `scheduled_cron`，不再依赖调度层传递业务 `kwargs`。
- **保持 Cron 语义**：继续由 `_is_last_run_today()` 按实际 Cron 计算当天最后一轮；`0 5,13,21 * * *` 的 05:00、13:00 仍只读缓存，21:00 对已播缺集执行 `scheduled_evening_refresh` 真实搜索。
- **边界不扩大**：手动同步、`onlyonce` 和生命周期定向同步不冒充最后一轮；电影退避、已完成订阅过滤、电视剧播出门禁及 AYCLUB Bridge 协议均未修改。
- **增强入口日志**：记录触发原因、是否定向、是否最后一轮、刷新授权和 Cron，便于直接核对调度链路。

## 1.9.7 修复

- **堵住生命周期强刷无限重试**：普通搜索、`lifecycle_force_refresh` 和 `scheduled_evening_refresh` 共享同一电影每日真实搜索额度；自动任务每个 TMDB media key 每日最多发送一次 Telegram 查询，只有明确的人工强刷入口可绕过。
- **失败退避与次数保护**：AYCLUB 网络错误、HTTP 504 或仍在等待迟到回复时保留生命周期强刷标记，但写入 6 小时 `retry_after`；同日后续同步只读缓存，并额外保留每日失败次数上限。
- **无资源终态冷却**：真实查询成功返回无资源后清除生命周期强刷，至少冷却 24 小时；尚未出现流媒体发布信号的电影按 14 天或下一已知上线日期取更晚时间。
- **迟到回复消费**：插件请求携带 `request_id`，识别桥接返回的原始请求 ID 与迟到回复缓存；迟到的空结果可在后续 cache-only 查询中直接消费并清除强刷标记。
- **可观测日志**：门禁和结果日志增加 `force_refresh_pending`、`last_real_search_at`、`retry_after`、`daily_search_count`、`no_result_cooldown_until` 和跳过原因。

配套 AYCLUB Bridge 1.5.1 负责识别明确无资源终态、关联请求回复并缓存迟到结果。本次 1.9.8 不修改桥接器或其协议字段。

## 1.9.6 修复

- **定时刷新与 Cron 对齐**：配置 Cron 的每日最后一轮固定执行 AYCLUB 真实搜索，记录原因为 `scheduled_evening_refresh`，并向桥接传递 `cache_only: false`；白天轮次继续严格只读缓存，手动同步和普通生命周期事件不会因此获得强刷权限。

## 1.9.5 主要变更

- **隔离 115 依赖**：移除 MoviePilot 共享环境中的 `p115client` 依赖，不再读取插件内 115 Cookie，避免影响 P115Disk、P115StrmHelper 等插件。
- **统一远端执行**：115 分享检查、文件枚举和选择性转存均通过 p115-openclaw 的认证接口执行。
- **严格限定转存目录**：只接受 `/mp整理` 下的已知分类目录；服务未配置、分类失败或目标越界时直接停止，不再回退到任意目录。
- **MoviePilot 职责边界**：整理事件只被动更新插件自身的在途状态，不触发即时搜索或刷新；后续整理、订阅刷新和最终完成仍由 MoviePilot 负责。
- **本地 STRM 实时对账**：搜索前按本地 STRM 文件重建实际缺集，降低 MoviePilot 历史或媒体库扫描滞后造成的重复投递。
- **ED2K contract v2**：只有后端明确回显经过校验的请求 ID、季集范围和活动任务状态时，才登记在途并阻止重复搜索。
- **ED2K 季集严格匹配**：从标题和真实 ED2K 文件名提取季集号，避免把 `2160p` 等分辨率误判为集号范围；电视剧资源必须与当前季缺集明确相交。
- **完整季包校验**：只有分享中的真实逐集文件构成完整连续季时，才允许按整季范围补缺或覆盖洗版。
- **旧状态安全迁移**：清理 1.8.8 以来可能由宽松 HTTP 响应、错误集号映射或旧 contract 生成的不可信 ED2K 在途与去重记录。
- **保留既有能力**：继续支持 AYCLUB 的电影/电视剧 ED2K、隐藏链接、同消息 115+ED2K、短哈希隐私日志、生命周期去重、普通来源兜底和 PT 窗口重启恢复。

## 版本组件

```text
plugins.v2/p115strgmsub/        P115StrgmSub 1.9.8
companion/tg-ayclub-bridge/     AYCLUB Bridge 1.5.1（独立配套项目，不在本仓库）
```

桥接源码不应复制进 MoviePilot 插件目录。它应覆盖桥接项目原有 `bridge.py`，并继续使用本机私有 Session 与环境变量。

## 隐私与仓库边界

不得提交：

- Telegram Session、账号信息或写死的机器人用户名；
- 115 Cookie、API Key、Access Token、密码；
- `.env`、数据库、日志、缓存和备份；
- 用户个人域名、内网 IP、媒体库 ID 或实际服务地址；
- 原始 ED2K 测试链接。

## 安装与持久化

MoviePilot 容器内插件目录通常为：

```text
/app/app/plugins/p115strgmsub
```

镜像重建可能清除未挂载的自定义插件。GitHub 用于源码备份和恢复；生产环境建议使用 bind mount 持久化插件目录。详见 `docs/PERSISTENT_DEPLOYMENT.md`。

`package.v2.json` 保持 `release: false`，表示自维护测试版本，不代表官方插件市场发布。
