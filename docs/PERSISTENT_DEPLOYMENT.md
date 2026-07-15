# MoviePilot 容器更新后的持久化部署

GitHub 保存源码和版本记录，但不会阻止 Docker 在更新镜像并重建容器时删除容器内部的自定义插件目录。

当前插件运行目录（r5 同样适用）：

```text
/app/app/plugins/p115strgmsub
```

建议把源码固定保存在 NAS 主机目录，再通过 Docker bind mount 映射进 MoviePilot：

```text
/volume4/moviepilot-custom-plugins/p115strgmsub
  -> /app/app/plugins/p115strgmsub
```

## 首次迁移

在修改 Compose 前，先把当前已验证版本复制到主机目录：

```bash
mkdir -p /volume4/moviepilot-custom-plugins
docker cp moviepilot-v2:/app/app/plugins/p115strgmsub \
  /volume4/moviepilot-custom-plugins/p115strgmsub
```

确认版本：

```bash
grep -n 'plugin_version' \
  /volume4/moviepilot-custom-plugins/p115strgmsub/__init__.py | head
```

## Compose 增加挂载

在 `moviepilot-v2` 的 `volumes:` 下增加：

```yaml
- '/volume4/moviepilot-custom-plugins/p115strgmsub:/app/app/plugins/p115strgmsub'
```

随后按原 Compose 项目方式重建 MoviePilot 容器，并检查日志中加载的版本。

## 更新插件

以后更新代码时，替换主机目录中的 `p115strgmsub`，再重启 MoviePilot。GitHub tag 用于确定可回滚版本。

## 隐私

不要把以下内容复制进公开仓库：

- Telegram Session 或账号文件；
- 115 Cookie、API Token、密码；
- MoviePilot 数据库和配置文件；
- 本机 `.env`、日志、内网地址或个人域名。
