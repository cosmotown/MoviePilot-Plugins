# P115StrgmSub - 115网盘订阅追更插件

当前版本：**1.8.8**

MoviePilot v2 插件。根据 MoviePilot 订阅与媒体库正式缺失状态，从 115 分享与 AYCLUB 结构化结果中选择目标资源。115 分享仍由插件直接转存；ED2K 原样提交给已配置的 OpenClaw/p115 后端执行，插件不轮询下载、不移动下载文件。

## 工作流

```text
MoviePilot 订阅
  → P115StrgmSub 检查官方缺失状态
  → AYCLUB / PanSou 等搜索源
  ├─ 115 分享：插件选择文件并转存到待整理目录
  └─ ED2K：原样提交 OpenClaw/p115 后端
  → 后端或 115 流程进入 MoviePilot 整理
  → STRM / 媒体库扫描
  → MoviePilot 正式缺失检查确认入库
```

## 1.8.8 主要能力

- **AYCLUB ED2K 支持**：识别电影仅 ED2K、电视剧单集 ED2K、隐藏 TextUrl 和同消息 115+ED2K。
- **电影仅 ED2K**：提交成功后登记为在途，避免立即被后续来源重复下载；同消息内仍可继续处理 115。
- **电视剧单集 ED2K**：只覆盖解析出的目标集，其他缺集仍可继续走 115 或普通来源。
- **职责边界明确**：插件只向既有认证端点提交原始 ED2K，不轮询、不移动文件；后端负责下载和后续移动。
- **隐私日志**：ED2K 只记录 SHA-256 短引用，不输出原始链接、认证 Token 或后端回显文本。
- **持久去重与生命周期**：同订阅、同生命周期、同资源 24 小时内避免重复提交；ED2K 在途状态最长保留 24 小时。
- **115 原流程不变**：115 分享校验、文件选择、目录分类、转存及 MoviePilot 生命周期联动保持原逻辑。
- **AYCLUB 失效分享兜底**：已观察到发布的剧集即使 115 分享失效，也允许 PanSou 等普通来源继续兜底。
- **分类服务安全回退**：可选分类服务不可用或无法给出目标目录时，回退到配置的安全待整理根目录。
- **PT 窗口重启恢复**：临时 PT 开放窗口保存精确结束时间，MoviePilot 重启后按原截止时间恢复。
- **MoviePilot 官方缺失为准**：转存或提交只记为在途，完成判断仍由 MoviePilot/媒体库正式状态确认。

## 版本组件

```text
plugins.v2/p115strgmsub/        P115StrgmSub 1.8.8
companion/tg-ayclub-bridge/     AYCLUB Bridge 1.4.3（独立配套项目）
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
