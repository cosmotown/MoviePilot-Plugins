# P115StrgmSub - 115网盘订阅追更插件

当前版本：**1.8.3-r4**

MoviePilot 插件。根据 MoviePilot 订阅与媒体库状态，从 115 分享资源中选择需要的电影或剧集文件并转存，随后交由 MoviePilot 与 115 网盘 STRM 助手完成整理和入库。

## 工作流

```text
MoviePilot 订阅
  → P115StrgmSub 检查官方缺失状态
  → AYCLUB 本机桥接 / PanSou 等搜索源
  → 精确选择目标文件并转存到待整理目录
  → MoviePilot 整理入库
  → STRM 助手生成 STRM
  → MoviePilot 事件回传并完成订阅
```

## 1.8.3-r4 主要能力

- **MoviePilot 生命周期联动**：监听订阅新增、修改、重置、删除、完成，以及整理成功和失败事件。
- **定向同步**：单个订阅重置或更新时只处理对应 `subscribe_id`，不再触发其他订阅的全量扫描。
- **官方缺失状态为准**：搜索前使用 MoviePilot 官方订阅链获取真正缺失内容；转存成功只记录为等待整理，不直接伪造完成状态。
- **电影合集精确匹配**：按 TMDB、标题和年份排除合集中的续集或其他影片。
- **同片多版本择优**：同一电影只提交一个具体文件 ID，按照订阅过滤、片名、分辨率、片源、HDR、音轨、编码和文件大小综合排序。
- **电视剧集数过滤**：只处理 MoviePilot 当前缺失且已播出的目标季集，支持整季包与单集资源。
- **查询节流**：默认在 MoviePilot 时区的 `06:30、14:30、22:30` 执行；白天优先只读桥接缓存，晚间按发布状态与退避策略进行真实查询。
- **多搜索源回退**：支持 AYCLUB 本机 Telegram 桥接与 PanSou 等来源，并可配置优先级。
- **安全日志**：115 分享链接使用脱敏引用，桥接返回的分享地址执行来源校验。

## 隐私与仓库边界

本仓库只保存插件源码，不保存任何运行时凭证或会话文件。请勿提交：

- Telegram 机器人用户名、账号信息或 Session；
- 115 Cookie、API Key、Access Token、密码；
- `.env`、数据库、日志和备份目录；
- 用户个人域名、内网 IP 或实际服务地址。

AYCLUB 集成在公开说明中仅描述为“本机 Telegram 桥接服务”。桥接端的机器人目标、账号和 Session 必须留在本机私有配置中。

## 目录

```text
plugins.v2/p115strgmsub/
package.v2.json
README.md
```

## 安装与升级

插件源码目录为：

```text
/app/app/plugins/p115strgmsub
```

普通容器重启不会删除容器内文件，但更新镜像并重建容器可能清空未挂载的自定义插件。GitHub 用于源码备份和版本恢复；要让容器重建后仍自动保留插件，应把插件目录绑定挂载到 NAS 主机目录。参见 [`docs/PERSISTENT_DEPLOYMENT.md`](docs/PERSISTENT_DEPLOYMENT.md)。

## 兼容要求

- MoviePilot v2，且具备本版本使用的订阅与整理事件接口；
- 115 网盘 STRM 助手负责后续整理、STRM 生成与媒体库联动；
- AYCLUB 搜索启用时，建议本机桥接服务版本不低于 **1.4.2**，以支持 `force_refresh`、`cache_only`、缓存失效和结构化元数据。

## 版本说明

完整变更记录见 `package.v2.json`。当前仓库保持 `release: false`，用于自维护与测试，不代表官方插件市场发布。
