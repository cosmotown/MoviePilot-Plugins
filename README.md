# P115StrgmSub - 115网盘订阅追更插件

当前版本：**1.9.6**

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
plugins.v2/p115strgmsub/        P115StrgmSub 1.9.6
companion/tg-ayclub-bridge/     AYCLUB Bridge 1.4.3（独立配套项目，不在本次同步范围）
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
